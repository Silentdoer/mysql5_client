library mysql_client.connection;

import "dart:async";
import "dart:io";

import "protocol.dart";

class ConnectionError extends Error {
  final String message;

  ConnectionError(this.message);

  String toString() => "ConnectionError: $message";
}

class QueryError extends Error {
  final String message;

  QueryError(this.message);

  String toString() => "QueryError: $message";
}

class PreparedStatementError extends Error {
  final String message;

  PreparedStatementError(this.message);

  String toString() => "PreparedStatementError: $message";
}

class Connection {
  Socket _socket;

  Protocol _protocol;

  ProtocolResult _lastProtocolResult;

  bool get isClosed => _protocol == null;

  Future connect(host, int port, String userName, String password,
      [String database]) async {
    if (!isClosed) {
      throw new StateError("Connection already connected");
    }

    var socket = await Socket.connect(host, port);
    socket.setOption(SocketOption.TCP_NODELAY, true);

    var protocol = new Protocol(socket);

    try {
      var response =
          await protocol.connectionProtocol.readInitialHandshakeResponse();

      if (response is! InitialHandshakePacket) {
        throw new ConnectionError(response.errorMessage);
      }

      protocol.connectionProtocol.writeHandshakeResponsePacket(userName,
          password, database, response.authPluginData, response.authPluginName);

      response = await protocol.readCommandResponse();

      if (response is ErrorPacket) {
        throw new ConnectionError(response.errorMessage);
      }

      _socket = socket;
      _protocol = protocol;
    } finally {
      protocol.connectionProtocol.freeReusables();
    }
  }

  Future<QueryResult> executeQuery(String query) async {
    if (isClosed) {
      throw new StateError("Connection closed");
    }

    await _lastProtocolResult?.free();

    _lastProtocolResult = null;

    try {
      _protocol.queryCommandTextProtocol.writeCommandQueryPacket(query);

      var response =
          await _protocol.queryCommandTextProtocol.readCommandQueryResponse();

      if (response is OkPacket) {
        return new QueryResult.ok(
            response.affectedRows, response.lastInsertId, this);
      }

      if (response is! ResultSetColumnCountPacket) {
        throw new QueryError(response.errorMessage);
      }

      List<ColumnDefinition> columns = new List(response.columnCount);
      var columnIterator = new _QueryColumnIterator(columns.length, this);
      var hasColumn = true;
      var i = 0;
      while (hasColumn) {
        hasColumn = await columnIterator.rawNext();
        if (hasColumn) {
          columns[i++] =
              new ColumnDefinition(columnIterator.name, columnIterator.type);
        }
      }

      _lastProtocolResult = new QueryResult.resultSet(columns, this);

      return _lastProtocolResult;
    } finally {
      _protocol.queryCommandTextProtocol.freeReusables();
    }
  }

  Future<PreparedStatement> prepareQuery(String query) async {
    if (isClosed) {
      throw new StateError("Connection closed");
    }

    await _lastProtocolResult?.free();

    _lastProtocolResult = null;

    try {
      _protocol.preparedStatementProtocol
          .writeCommandStatementPreparePacket(query);

      var response = await _protocol.preparedStatementProtocol
          .readCommandStatementPrepareResponse();

      if (response is! CommandStatementPrepareOkResponsePacket) {
        throw new PreparedStatementError(response.errorMessage);
      }

      List<ColumnDefinition> parameters = new List(response.numParams);
      var parameterIterator = new _QueryColumnIterator(parameters.length, this);
      var hasParameter = true;
      var i = 0;
      while (hasParameter) {
        hasParameter = await parameterIterator.rawNext();
        if (hasParameter) {
          parameters[i++] = new ColumnDefinition(
              parameterIterator.name, parameterIterator.type);
        }
      }

      List<ColumnDefinition> columns = new List(response.numColumns);
      var columnIterator = new _QueryColumnIterator(columns.length, this);
      var hasColumn = true;
      var l = 0;
      while (hasColumn) {
        hasColumn = await columnIterator.rawNext();
        if (hasColumn) {
          columns[l++] =
              new ColumnDefinition(columnIterator.name, columnIterator.type);
        }
      }

      _lastProtocolResult = new PreparedStatement(
          response.statementId, parameters, columns, this);

      return _lastProtocolResult;
    } finally {
      _protocol.preparedStatementProtocol.freeReusables();
    }
  }

  Future close() async {
    if (isClosed) {
      throw new StateError("Connection closed");
    }

    await _lastProtocolResult?.free();

    var socket = _socket;

    _socket = null;
    _protocol = null;

    await socket.close();
    socket.destroy();
  }
}

abstract class ProtocolResult {
  Future free();

  Future close();
}

abstract class ProtocolIterator {
  bool get isClosed;

  Future<bool> next();

  // TODO qui si potrebbe utilizzare il FutureWrapper
  rawNext();

  Future close();
}

class ColumnDefinition {
  final String name;
  final int type;

  ColumnDefinition(this.name, this.type);
}

class QueryResult implements ProtocolResult, ProtocolIterator {
  final Connection _connection;

  final int affectedRows;

  final int lastInsertId;

  final List<ColumnDefinition> columns;

  _QueryRowIterator _rowIterator;

  QueryResult.resultSet(this.columns, this._connection)
      : this.affectedRows = null,
        this.lastInsertId = null {
    this._rowIterator = new _QueryRowIterator(this);
  }

  QueryResult.ok(this.affectedRows, this.lastInsertId, this._connection)
      : this.columns = null,
        this._rowIterator = null;

  int get columnCount => columns.length;

  bool get isClosed => _rowIterator == null || _rowIterator.isClosed;

  Future<bool> next() => _rowIterator.next();

  rawNext() => _rowIterator.rawNext();

  String getStringValue(int index) => _rowIterator.getStringValue(index);

  num getNumValue(int index) => _rowIterator.getNumValue(index);

  bool getBoolValue(int index) => _rowIterator.getBoolValue(index);

  @override
  Future free() async {
    await close();
  }

  @override
  Future close() async {
    if (_rowIterator != null && !_rowIterator.isClosed) {
      await _rowIterator.close();
    }
  }
}

class PreparedStatement implements ProtocolResult {
  final Connection _connection;

  final int _statementId;

  final List<ColumnDefinition> parameters;
  final List<ColumnDefinition> columns;

  final List<int> _parameterTypes;
  final List _parameterValues;
  List<int> _columnTypes;

  bool _isClosed;
  bool _isNewParamsBoundFlag;

  PreparedStatement(this._statementId, List<ColumnDefinition> parameters,
      List<ColumnDefinition> columns, this._connection)
      : this.parameters = parameters,
        this.columns = columns,
        this._parameterTypes = new List(parameters.length),
        this._parameterValues = new List(parameters.length) {
    _isClosed = false;
    _isNewParamsBoundFlag = true;
    _columnTypes = new List.generate(
        columns.length, (index) => columns[index].type,
        growable: false);
  }

  int get parameterCount => parameters.length;

  int get columnCount => columns.length;

  bool get isClosed => _isClosed;

  void setParameter(int index, value, [int sqlType]) {
    if (index >= parameterCount) {
      throw new IndexError(index, _parameterValues);
    }

    sqlType ??= _connection._protocol.preparedStatementProtocol
        .getSqlTypeFromValue(value);

    if (sqlType != null && _parameterTypes[index] != sqlType) {
      _parameterTypes[index] = sqlType;
      _isNewParamsBoundFlag = true;
    }

    _parameterValues[index] = value;
  }

  Future<PreparedQueryResult> executeQuery() async {
    // TODO check dello stato

    // TODO free del _lastProtocolResult

    // await _lastProtocolResult?.free();

    // _lastProtocolResult = null;

    try {
      _connection._protocol.preparedStatementProtocol
          .writeCommandStatementExecutePacket(_statementId, _parameterValues,
              _isNewParamsBoundFlag, _parameterTypes);

      var response = await _connection._protocol.preparedStatementProtocol
          .readCommandStatementExecuteResponse();

      if (response is ErrorPacket) {
        throw new QueryError(response.errorMessage);
      }

      _isNewParamsBoundFlag = false;

      if (response is OkPacket) {
        return new PreparedQueryResult.ok(
            response.affectedRows, response.lastInsertId);
      } else {
        var columnIterator = new _QueryColumnIterator(columnCount, _connection);
        var hasColumn = true;
        while (hasColumn) {
          hasColumn = await columnIterator._skip();
        }

        // TODO memorizzazione del _lastProtocolResult

        return new PreparedQueryResult.resultSet(this);
      }
    } finally {
      _connection._protocol.preparedStatementProtocol.freeReusables();
    }
  }

  @override
  Future free() async {}

  @override
  Future close() async {
    // TODO implementare PreparedStatement.close

    try {
      _connection._protocol.preparedStatementProtocol
          .writeCommandStatementClosePacket(_statementId);
    } finally {
      _connection._protocol.preparedStatementProtocol.freeReusables();
    }
  }
}

class PreparedQueryResult implements ProtocolResult {
  final PreparedStatement _statement;

  final int affectedRows;

  final int lastInsertId;

  _PreparedQueryRowIterator _rowIterator;

  PreparedQueryResult.resultSet(PreparedStatement statement)
      : this._statement = statement,
        this.affectedRows = null,
        this.lastInsertId = null {
    this._rowIterator = new _PreparedQueryRowIterator(this);
  }

  PreparedQueryResult.ok(this.affectedRows, this.lastInsertId)
      : this._statement = null,
        this._rowIterator = null;

  int get columnCount => _statement?.columnCount;

  bool get isClosed => _rowIterator == null || _rowIterator.isClosed;

  Future<bool> next() => _rowIterator.next();

  rawNext() => _rowIterator.rawNext();

  String getStringValue(int index) => _rowIterator.getStringValue(index);

  num getNumValue(int index) => _rowIterator.getNumValue(index);

  bool getBoolValue(int index) => _rowIterator.getBoolValue(index);

  @override
  Future free() async {
    await close();
  }

  @override
  Future close() async {
    if (_rowIterator != null && !_rowIterator.isClosed) {
      await _rowIterator.close();
    }
  }
}

class _QueryColumnIterator implements ProtocolIterator {
  final Connection _connection;

  final int columnCount;

  bool _isClosed;

  _QueryColumnIterator(this.columnCount, this._connection) {
    _isClosed = false;
  }

  bool get isClosed => _isClosed;

  Future close() async {
    if (!isClosed) {
      var hasNext = true;
      while (hasNext) {
        hasNext = _skip();
        hasNext = hasNext is Future ? await hasNext : hasNext;
      }

      _isClosed = true;
    }
  }

  Future<bool> next() {
    var value = rawNext();
    return value is Future ? value : new Future.value(value);
  }

  rawNext() {
    if (isClosed) {
      throw new StateError("Column iterator closed");
    }

    if (columnCount > 0) {
      var response = _connection._protocol.queryCommandTextProtocol
          .readResultSetColumnDefinitionResponse();

      return response is Future
          ? response.then((response) => _checkNext(response))
          : _checkNext(response);
    } else {
      return false;
    }
  }

  String get catalog => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.catalog;
  String get schema => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.schema;
  String get table =>
      _connection._protocol.queryCommandTextProtocol.reusableColumnPacket.table;
  String get orgTable => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.orgTable;
  String get name =>
      _connection._protocol.queryCommandTextProtocol.reusableColumnPacket.name;
  String get orgName => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.orgName;
  int get fieldsLength => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.fieldsLength;
  int get characterSet => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.characterSet;
  int get columnLength => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.columnLength;
  int get type =>
      _connection._protocol.queryCommandTextProtocol.reusableColumnPacket.type;
  int get flags =>
      _connection._protocol.queryCommandTextProtocol.reusableColumnPacket.flags;
  int get decimals => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.decimals;

  _skip() {
    var response = _connection._protocol.queryCommandTextProtocol
        .skipResultSetColumnDefinitionResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  bool _checkNext(Packet response) {
    if (response is ResultSetColumnDefinitionPacket) {
      return true;
    } else {
      _isClosed = true;
      _connection._protocol.queryCommandTextProtocol.freeReusables();
      return false;
    }
  }
}

class _QueryRowIterator implements ProtocolIterator {
  final QueryResult _result;

  bool _isClosed;

  _QueryRowIterator(this._result) {
    _isClosed = false;
  }

  bool get isClosed => _isClosed;

  Future close() async {
    if (!isClosed) {
      var hasNext = true;
      while (hasNext) {
        hasNext = _skip();
        hasNext = hasNext is Future ? await hasNext : hasNext;
      }

      _isClosed = true;
    }
  }

  Future<bool> next() {
    var value = rawNext();
    return value is Future ? value : new Future.value(value);
  }

  rawNext() {
    if (isClosed) {
      throw new StateError("Column iterator closed");
    }

    var response = _result._connection._protocol.queryCommandTextProtocol
        .readResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  String getStringValue(int index) => _result._connection._protocol
      .queryCommandTextProtocol.reusableRowPacket.getUTF8String(index);

  num getNumValue(int index) {
    var formatted = _result._connection._protocol.queryCommandTextProtocol
        .reusableRowPacket.getString(index);
    return formatted != null ? num.parse(formatted) : null;
  }

  bool getBoolValue(int index) {
    var formatted = getNumValue(index);
    return formatted != null ? formatted != 0 : null;
  }

  _skip() {
    var response = _result._connection._protocol.queryCommandTextProtocol
        .skipResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  bool _checkNext(Packet response) {
    if (response is ResultSetRowPacket) {
      return true;
    } else {
      _isClosed = true;
      _result._connection._protocol.queryCommandTextProtocol.freeReusables();
      return false;
    }
  }
}

class _PreparedQueryRowIterator implements ProtocolIterator {
  final PreparedQueryResult _result;

  bool _isClosed;

  _PreparedQueryRowIterator(this._result) {
    _isClosed = false;
  }

  bool get isClosed => _isClosed;

  Future close() async {
    if (!isClosed) {
      var hasNext = true;
      while (hasNext) {
        hasNext = _skip();
        hasNext = hasNext is Future ? await hasNext : hasNext;
      }

      _isClosed = true;
    }
  }

  Future<bool> next() {
    var value = rawNext();
    return value is Future ? value : new Future.value(value);
  }

  rawNext() {
    if (isClosed) {
      throw new StateError("Column iterator closed");
    }

    var response = _result
        ._statement._connection._protocol.preparedStatementProtocol
        .readResultSetRowResponse(_result._statement._columnTypes);

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  String getStringValue(int index) => _result._statement._connection._protocol
      .preparedStatementProtocol.reusableRowPacket.getUTF8String(index);

  num getNumValue(int index) {
    var column = _result._statement.columns[index];
    switch (column.type) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
        return _result._statement._connection._protocol
            .preparedStatementProtocol.reusableRowPacket.getInteger(index);
      case MYSQL_TYPE_DOUBLE:
        return _result._statement._connection._protocol
            .preparedStatementProtocol.reusableRowPacket.getDouble(index);
      default:
        throw new UnsupportedError("Sql type not supported ${column.type}");
    }
  }

  bool getBoolValue(int index) {
    var formatted = getNumValue(index);
    return formatted != null ? formatted != 0 : null;
  }

  _skip() {
    var response = _result._statement._connection._protocol
        .preparedStatementProtocol.skipResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  bool _checkNext(Packet response) {
    if (response is PreparedResultSetRowPacket) {
      return true;
    } else {
      _isClosed = true;
      _result._statement._connection._protocol.preparedStatementProtocol
          .freeReusables();
      return false;
    }
  }
}
