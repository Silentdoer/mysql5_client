library mysql_client.connection;

import "dart:async";
import "dart:io";

import "package:mysql_client/src/protocol.dart";

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

class PrepareStatementError extends Error {
  final String message;

  PrepareStatementError(this.message);

  String toString() => "PrepareStatementError: $message";
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
        return new QueryResult.ok(response.affectedRows, response.lastInsertId);
      }

      if (response is! ResultSetColumnCountPacket) {
        throw new QueryError(response.errorMessage);
      }

      List<ColumnDefinition> columns = new List(response.columnCount);
      var columnIterator = new QueryColumnIterator(columns.length, _protocol);
      var hasColumn = true;
      var i = 0;
      while (hasColumn) {
        hasColumn = await columnIterator.rawNext();
        if (hasColumn) {
          columns[i++] =
              new ColumnDefinition(columnIterator.name, columnIterator.type);
        }
      }

      _lastProtocolResult = new QueryResult.resultSet(columns, _protocol);

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
        throw new PrepareStatementError(response.errorMessage);
      }

      List<ColumnDefinition> parameters = new List(response.numParams);
      var parameterIterator =
          new QueryColumnIterator(parameters.length, _protocol);
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
      var columnIterator = new QueryColumnIterator(columns.length, _protocol);
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
          response.statementId, parameters, columns, _protocol);

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
  bool isClosed;

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

class QueryResult implements ProtocolResult {
  final Protocol _protocol;

  final int affectedRows;

  final int lastInsertId;

  final List<ColumnDefinition> columns;

  QueryRowIterator _rowIterator;

  QueryResult.resultSet(this.columns, Protocol protocol)
      : this.affectedRows = null,
        this.lastInsertId = null,
        this._protocol = protocol {
    this._rowIterator = new QueryRowIterator(this);
  }

  QueryResult.ok(this.affectedRows, this.lastInsertId)
      : this.columns = null,
        this._rowIterator = null,
        this._protocol = null;

  int get columnCount => columns.length;

  bool get isClosed => _rowIterator == null || _rowIterator.isClosed;

  Future<QueryRowIterator> rowIterator() async {
    if (isClosed) {
      throw new StateError("Query result closed");
    }

    return _rowIterator;
  }

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

class QueryColumnIterator extends ProtocolIterator {
  final int columnCount;

  final Protocol _protocol;

  bool _isClosed;

  QueryColumnIterator(this.columnCount, this._protocol) {
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
      var response = _protocol.queryCommandTextProtocol
          .readResultSetColumnDefinitionResponse();

      return response is Future
          ? response.then((response) => _checkNext(response))
          : _checkNext(response);
    } else {
      return false;
    }
  }

  String get catalog =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.catalog;
  String get schema =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.schema;
  String get table =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.table;
  String get orgTable =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.orgTable;
  String get name =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.name;
  String get orgName =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.orgName;
  int get fieldsLength =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.fieldsLength;
  int get characterSet =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.characterSet;
  int get columnLength =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.columnLength;
  int get type => _protocol.queryCommandTextProtocol.reusableColumnPacket.type;
  int get flags =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.flags;
  int get decimals =>
      _protocol.queryCommandTextProtocol.reusableColumnPacket.decimals;

  _skip() {
    var response = _protocol.queryCommandTextProtocol
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
      _protocol.queryCommandTextProtocol.freeReusables();
      return false;
    }
  }
}

class QueryRowIterator extends ProtocolIterator {
  final QueryResult _result;

  bool _isClosed;

  QueryRowIterator(this._result) {
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

    var response =
        _result._protocol.queryCommandTextProtocol.readResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  String getStringValue(int index) => _result._protocol.queryCommandTextProtocol
      .reusableRowPacket.getUTF8String(index);

  num getNumValue(int index) {
    var formatted = _result._protocol.queryCommandTextProtocol.reusableRowPacket
        .getString(index);
    return formatted != null ? num.parse(formatted) : null;
  }

  bool getBoolValue(int index) {
    var formatted = getNumValue(index);
    return formatted != null ? formatted != 0 : null;
  }

  _skip() {
    var response =
        _result._protocol.queryCommandTextProtocol.skipResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  bool _checkNext(Packet response) {
    if (response is ResultSetRowPacket) {
      return true;
    } else {
      _isClosed = true;
      _result._protocol.queryCommandTextProtocol.freeReusables();
      return false;
    }
  }
}

class PreparedStatement implements ProtocolResult {
  final Protocol _protocol;

  final int _statementId;

  final List<ColumnDefinition> parameters;
  final List<ColumnDefinition> columns;

  final List<int> _parameterTypes;
  final List _parameterValues;
  List<int> _columnTypes;

  bool _isClosed;
  bool _isNewParamsBoundFlag;

  PreparedStatement(this._statementId, List<ColumnDefinition> parameters,
      List<ColumnDefinition> columns, Protocol protocol)
      : this.parameters = parameters,
        this.columns = columns,
        this._parameterTypes = new List(parameters.length),
        this._parameterValues = new List(parameters.length),
        this._protocol = protocol {
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

    // TODO non mi piace molto questo metodo del protocol
    sqlType ??= _protocol.preparedStatementProtocol.getSqlTypeFromValue(value);

    if (sqlType != null && _parameterTypes[index] != sqlType) {
      _parameterTypes[index] = sqlType;
      _isNewParamsBoundFlag = true;
    }

    _parameterValues[index] = value;
  }

  Future<PreparedQueryResult> executeQuery() async {
    // TODO check dello stato

    try {
      _protocol.preparedStatementProtocol.writeCommandStatementExecutePacket(
          _statementId,
          _parameterValues,
          _isNewParamsBoundFlag,
          _parameterTypes);

      var response = await _protocol.preparedStatementProtocol
          .readCommandStatementExecuteResponse();

      if (response is ErrorPacket) {
        throw new QueryError(response.errorMessage);
      }

      _isNewParamsBoundFlag = false;

      if (response is OkPacket) {
        return new PreparedQueryResult.ok(
            response.affectedRows, response.lastInsertId);
      } else {
        var columnIterator = new QueryColumnIterator(columnCount, _protocol);
        var hasColumn = true;
        while (hasColumn) {
          hasColumn = await columnIterator._skip();
        }

        return new PreparedQueryResult.resultSet(this);
      }
    } finally {
      _protocol.preparedStatementProtocol.freeReusables();
    }
  }

  @override
  Future free() async {}

  @override
  Future close() async {
    // TODO implementare PreparedStatement.close

    try {
      _protocol.preparedStatementProtocol
          .writeCommandStatementClosePacket(_statementId);
    } finally {
      _protocol.preparedStatementProtocol.freeReusables();
    }
  }
}

class PreparedQueryResult implements ProtocolResult {
  final PreparedStatement _statement;

  final int affectedRows;

  final int lastInsertId;

  PreparedQueryRowIterator _rowIterator;

  PreparedQueryResult.resultSet(PreparedStatement statement)
      : this._statement = statement,
        this.affectedRows = null,
        this.lastInsertId = null {
    this._rowIterator = new PreparedQueryRowIterator(this);
  }

  PreparedQueryResult.ok(this.affectedRows, this.lastInsertId)
      : this._statement = null,
        this._rowIterator = null;

  int get columnCount => _statement?.columnCount;

  bool get isClosed => _rowIterator == null || _rowIterator.isClosed;

  Future<PreparedQueryRowIterator> rowIterator() async {
    if (isClosed) {
      throw new StateError("Query result closed");
    }

    return _rowIterator;
  }

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

class PreparedQueryRowIterator extends ProtocolIterator {
  final PreparedQueryResult _result;

  bool _isClosed;

  PreparedQueryRowIterator(this._result) {
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

    var response = _result._statement._protocol.preparedStatementProtocol
        .readResultSetRowResponse(_result._statement._columnTypes);

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  String getStringValue(int index) => _result._statement._protocol
      .preparedStatementProtocol.reusableRowPacket.getUTF8String(index);

  num getNumValue(int index) {
    var column = _result._statement.columns[index];
    switch (column.type) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
        return _result._statement._protocol.preparedStatementProtocol
            .reusableRowPacket.getInteger(index);
      case MYSQL_TYPE_DOUBLE:
        return _result._statement._protocol.preparedStatementProtocol
            .reusableRowPacket.getDouble(index);
      default:
        throw new UnsupportedError("Sql type not supported ${column.type}");
    }
  }

  bool getBoolValue(int index) {
    var formatted = getNumValue(index);
    return formatted != null ? formatted != 0 : null;
  }

  _skip() {
    var response = _result._statement._protocol.preparedStatementProtocol
        .skipResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  bool _checkNext(Packet response) {
    if (response is PreparedResultSetRowPacket) {
      return true;
    } else {
      _isClosed = true;
      _result._statement._protocol.preparedStatementProtocol.freeReusables();
      return false;
    }
  }
}
