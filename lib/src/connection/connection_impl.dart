library mysql_client.connection.impl;

import 'dart:async';
import 'dart:io';

import '../protocol.dart';
import '../connection.dart';

class ConnectionFactoryImpl implements ConnectionFactory {
  Future<Connection> connect(host, int port, String userName, String password,
      [String database]) async {
    var connection = new ConnectionImpl();
    await connection.connect(host, port, userName, password, database);
    return connection;
  }
}

class ConnectionImpl implements Connection {
  RawSocket _socket;

  Protocol _protocol;

  CommandResult _lastProtocolResult;

  bool get isClosed => _protocol == null;

  Future connect(host, int port, String userName, String password,
      [String database]) async {
    if (!isClosed) {
      throw new StateError("Connection already connected");
    }

    var socket = await RawSocket.connect(host, port);
    socket.setOption(SocketOption.TCP_NODELAY, true);
    socket.writeEventsEnabled = false;

    var protocol = new Protocol(socket);

    try {
      var response =
          await protocol.connectionProtocol.readInitialHandshakeResponse();

      // TODO verifica sequenceId

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
      protocol.connectionProtocol.free();
    }
  }

  Future<QueryResult> test(String query) async {
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
        return new CommandQueryResultImpl.ok(
            response.affectedRows, response.lastInsertId, this);
      }

      if (response is! ResultSetColumnCountPacket) {
        throw new QueryError(response.errorMessage);
      }

      List<ColumnDefinition> columns = new List(response.columnCount);
      var columnIterator = new QueryColumnIteratorImpl(columns.length, this);
      var hasColumn = true;
      var i = 0;
      while (hasColumn) {
        hasColumn = await columnIterator.rawNext();
        if (hasColumn) {
          columns[i++] =
              new ColumnDefinition(columnIterator.name, columnIterator.type);
        }
      }

      _lastProtocolResult = new CommandQueryResultImpl.resultSet(columns, this);

      return _lastProtocolResult;
    } finally {
      _protocol.queryCommandTextProtocol.free();
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
        return new CommandQueryResultImpl.ok(
            response.affectedRows, response.lastInsertId, this);
      }

      if (response is! ResultSetColumnCountPacket) {
        throw new QueryError(response.errorMessage);
      }

      List<ColumnDefinition> columns = new List(response.columnCount);
      var columnIterator = new QueryColumnIteratorImpl(columns.length, this);
      var hasColumn = true;
      var i = 0;
      while (hasColumn) {
        hasColumn = await columnIterator.rawNext();
        if (hasColumn) {
          columns[i++] =
              new ColumnDefinition(columnIterator.name, columnIterator.type);
        }
      }

      _lastProtocolResult = new CommandQueryResultImpl.resultSet(columns, this);

      return _lastProtocolResult;
    } finally {
      _protocol.queryCommandTextProtocol.free();
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
      var parameterIterator = new QueryColumnIteratorImpl(parameters.length, this);
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
      var columnIterator = new QueryColumnIteratorImpl(columns.length, this);
      var hasColumn = true;
      var l = 0;
      while (hasColumn) {
        hasColumn = await columnIterator.rawNext();
        if (hasColumn) {
          columns[l++] =
              new ColumnDefinition(columnIterator.name, columnIterator.type);
        }
      }

      _lastProtocolResult = new PreparedStatementImpl(
          response.statementId, parameters, columns, this);

      // TODO raccogliere gli statement aperti

      return _lastProtocolResult;
    } finally {
      _protocol.preparedStatementProtocol.free();
    }
  }

  Future close() async {
    if (isClosed) {
      throw new StateError("Connection closed");
    }

    await _lastProtocolResult?.close();

    // TODO chiudo tutti gli eventuali statement ancora aperti (senza inviare la richiesta di chiusura del protocollo)

    var socket = _socket;

    _socket = null;
    _protocol = null;

    await socket.close();
  }
}

abstract class BaseQueryResultImpl implements QueryResult {
  final int affectedRows;

  final int lastInsertId;

  RowIterator _rowIterator;

  BaseQueryResultImpl.resultSet()
      : this.affectedRows = null,
        this.lastInsertId = null {
    this._rowIterator = _createRowIterator();
  }

  BaseQueryResultImpl.ok(this.affectedRows, this.lastInsertId)
      : this._rowIterator = null;

  RowIterator _createRowIterator();

  List<ColumnDefinition> get columns;

  int get columnCount => columns?.length;

  bool get isClosed => _rowIterator == null || _rowIterator.isClosed;

  Future<bool> next() => _rowIterator.next();

  rawNext() => _rowIterator.rawNext();

  String getStringValue(int index) => _rowIterator.getStringValue(index);

  num getNumValue(int index) => _rowIterator.getNumValue(index);

  bool getBoolValue(int index) => _rowIterator.getBoolValue(index);

  Future<List<List>> getNextRows() {
    // TODO implementare getNextRows
    throw new UnimplementedError();
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

class CommandQueryResultImpl extends BaseQueryResultImpl {
  final ConnectionImpl _connection;

  final List<ColumnDefinition> columns;

  CommandQueryResultImpl.resultSet(this.columns, this._connection)
      : super.resultSet();

  CommandQueryResultImpl.ok(
      int affectedRows, int lastInsertId, this._connection)
      : this.columns = null,
        super.ok(affectedRows, lastInsertId);

  RowIterator _createRowIterator() => new CommandQueryRowIteratorImpl(this);

  Protocol get _protocol => _connection._protocol;
}

class PreparedStatementImpl implements PreparedStatement {
  final ConnectionImpl _connection;

  final int _statementId;

  final List<ColumnDefinition> parameters;
  final List<ColumnDefinition> columns;

  final List<int> _parameterTypes;
  final List _parameterValues;
  List<int> _columnTypes;

  bool _isClosed;
  bool _isNewParamsBoundFlag;

  PreparedStatementImpl(this._statementId, List<ColumnDefinition> parameters,
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
    if (_isClosed) {
      throw new StateError("Prepared statement closed");
    }

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

  Future<QueryResult> executeQuery() async {
    if (_isClosed) {
      throw new StateError("Prepared statement closed");
    }

    await _connection._lastProtocolResult?.free();

    _connection._lastProtocolResult = null;

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
        return new PreparedQueryResultImpl.ok(
            response.affectedRows, response.lastInsertId);
      } else {
        var columnIterator = new QueryColumnIteratorImpl(columnCount, _connection);
        var hasColumn = true;
        while (hasColumn) {
          hasColumn = await columnIterator._skip();
        }

        _connection._lastProtocolResult =
            new PreparedQueryResultImpl.resultSet(this);

        // TODO raccolgo l'ultimo result abbinato a questo statement

        return _connection._lastProtocolResult;
      }
    } finally {
      _connection._protocol.preparedStatementProtocol.free();
    }
  }

  @override
  Future free() async {
    // TODO non posso chiudere lo statement ma posso liberare qualcosa?
  }

  @override
  Future close() async {
    if (!_isClosed) {
      _isClosed = true;

      // TODO chiudo l'eventuale ultimo queryresult attaccato

      // TODO avviso informo il connection della chiusura dello statement

      try {
        _connection._protocol.preparedStatementProtocol
            .writeCommandStatementClosePacket(_statementId);
      } finally {
        _connection._protocol.preparedStatementProtocol.free();
      }
    }
  }
}

class PreparedQueryResultImpl extends BaseQueryResultImpl {
  final PreparedStatement _statement;

  PreparedQueryResultImpl.resultSet(PreparedStatement statement)
      : this._statement = statement,
        super.resultSet();

  PreparedQueryResultImpl.ok(int affectedRows, int lastInsertId)
      : this._statement = null,
        super.ok(affectedRows, lastInsertId);

  RowIterator _createRowIterator() => new PreparedQueryRowIteratorImpl(this);

  List<ColumnDefinition> get columns => _statement?.columns;
}

abstract class BaseDataIteratorImpl implements DataIterator {
  bool _isClosed;

  BaseDataIteratorImpl() {
    _isClosed = false;
  }

  bool _isDataPacket(Packet packet);
  _readDataResponse();
  _skipDataResponse();
  _free();

  bool get isClosed => _isClosed;

  Future close() async {
    if (!_isClosed) {
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
    if (_isClosed) {
      throw new StateError("Column iterator closed");
    }

    var response = _readDataResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  _skip() {
    var response = _skipDataResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  bool _checkNext(Packet packet) {
    if (_isDataPacket(packet)) {
      return true;
    } else {
      _isClosed = true;
      _free();
      return false;
    }
  }
}

class QueryColumnIteratorImpl extends BaseDataIteratorImpl {
  final ConnectionImpl _connection;

  final int columnCount;

  QueryColumnIteratorImpl(this.columnCount, this._connection);

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

  bool _isDataPacket(Packet packet) =>
      packet is ResultSetColumnDefinitionPacket;

  _readDataResponse() => _connection._protocol.queryCommandTextProtocol
      .readResultSetColumnDefinitionResponse();

  _skipDataResponse() => _connection._protocol.queryCommandTextProtocol
      .skipResultSetColumnDefinitionResponse();

  _free() => _connection._protocol.queryCommandTextProtocol.free();
}

abstract class BaseQueryRowIteratorImpl<T extends QueryResult>
    extends BaseDataIteratorImpl implements RowIterator {
  final T _result;

  BaseQueryRowIteratorImpl(this._result);
}

class CommandQueryRowIteratorImpl extends BaseQueryRowIteratorImpl {
  CommandQueryRowIteratorImpl(CommandQueryResultImpl result) : super(result);

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

  bool _isDataPacket(Packet response) => response is ResultSetRowPacket;

  _readDataResponse() =>
      _result._protocol.queryCommandTextProtocol.readResultSetRowResponse();

  _skipDataResponse() =>
      _result._protocol.queryCommandTextProtocol.skipResultSetRowResponse();

  _free() => _result._connection._protocol.queryCommandTextProtocol.free();
}

class PreparedQueryRowIteratorImpl extends BaseQueryRowIteratorImpl {
  PreparedQueryRowIteratorImpl(PreparedQueryResultImpl result) : super(result);

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
      _result._statement._connection._protocol.preparedStatementProtocol.free();
      return false;
    }
  }

  bool _isDataPacket(Packet response) => response is PreparedResultSetRowPacket;

  _readDataResponse() =>
      _result._statement._connection._protocol.preparedStatementProtocol
          .readResultSetRowResponse(_result._statement._columnTypes);

  _skipDataResponse() =>
      _result._protocol.preparedStatementProtocol.skipResultSetRowResponse();

  _free() => _result._connection._protocol.preparedStatementProtocol.free();
}
