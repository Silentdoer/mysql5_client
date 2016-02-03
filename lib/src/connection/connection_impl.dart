library mysql_client.connection.impl;

import 'dart:async';
import 'dart:io';
import 'dart:collection';

import 'package:pool/pool.dart';

import '../protocol.dart';
import '../connection.dart';

int _getSqlType(SqlType sqlType) {
  switch (sqlType) {
    case SqlType.DATETIME:
      return MYSQL_TYPE_DATETIME;
    case SqlType.DOUBLE:
      return MYSQL_TYPE_DOUBLE;
    case SqlType.LONG:
      return MYSQL_TYPE_LONG;
    case SqlType.LONGLONG:
      return MYSQL_TYPE_LONGLONG;
    case SqlType.NULL:
      return MYSQL_TYPE_NULL;
    case SqlType.TIMESTAMP:
      return MYSQL_TYPE_TIMESTAMP;
    case SqlType.TINY:
      return MYSQL_TYPE_TINY;
    case SqlType.VAR_STRING:
      return MYSQL_TYPE_VAR_STRING;
  }
}

class ConnectionPoolImpl implements ConnectionPool {
  final ConnectionFactory _factory;
  final Pool _pool;
  final _host;
  final int _port;
  final String _userName;
  final String _password;
  final String _database;

  final Queue<Connection> _releasedConnections = new Queue();
  final Map<PooledConnectionImpl, PoolResource> _assignedResources = new Map();
  final Map<PooledConnectionImpl, Connection> _assignedConnections = new Map();

  ConnectionPoolImpl(
      {host,
      int port,
      String userName,
      String password,
      String database,
      int maxConnections,
      Duration connectionTimeout})
      : this._host = host,
        this._port = port,
        this._userName = userName,
        this._password = password,
        this._database = database,
        this._factory = new ConnectionFactory(),
        this._pool = new Pool(maxConnections, timeout: connectionTimeout);

  @override
  bool get isClosed => _pool.isClosed;

  @override
  Future<Connection> request() async {
    if (isClosed) {
      throw new StateError("Connection pool closed");
    }

    var resource = await _pool.request();

    var connection = _releasedConnections.isNotEmpty ? _releasedConnections.removeLast() : null;

    if (connection == null) {
      connection =
          await _factory.connect(_host, _port, _userName, _password, _database);
    }

    var pooledConnection = new PooledConnectionImpl(connection, this);

    _assignedConnections[pooledConnection] = connection;
    _assignedResources[pooledConnection] = resource;

    return pooledConnection;
  }

  @override
  Future close() async {
    await Future.wait(_assignedConnections.keys
        .map((pooledConnection) => _release(pooledConnection)));

    await _pool.close();

    await Future
        .wait(_releasedConnections.map((connection) => connection.close()));

    _releasedConnections.clear();
  }

  Future _release(PooledConnectionImpl pooledConnection) async {
    var connection = _assignedConnections.remove(pooledConnection);
    var resource = _assignedResources.remove(pooledConnection);

    await connection.free();
    _releasedConnections.add(connection);

    resource.release();
  }
}

class ConnectionFactoryImpl implements ConnectionFactory {
  @override
  Future<Connection> connect(host, int port, String userName, String password,
      [String database]) async {
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

      return new ConnectionImpl(socket, protocol);
    } finally {
      protocol.connectionProtocol.free();
    }
  }
}

class PooledConnectionImpl implements Connection {
  ConnectionPoolImpl _connectionPool;

  ConnectionImpl _connection;

  PooledConnectionImpl(this._connection, this._connectionPool);

  @override
  bool get isClosed => _connection == null;

  @override
  Future<QueryResult> executeQuery(String query) {
    if (isClosed) {
      throw new StateError("Connection released");
    }

    return _connection.executeQuery(query);
  }

  @override
  Future<PreparedStatement> prepareQuery(String query) {
    if (isClosed) {
      throw new StateError("Connection released");
    }

    return _connection.prepareQuery(query);
  }

  @override
  Future close() async {
    if (isClosed) {
      throw new StateError("Connection released");
    }

    _connection = null;

    var connectionPool = _connectionPool;

    _connectionPool = null;

    await connectionPool._release(this);
  }
}

class ConnectionImpl implements Connection {
  RawSocket _socket;

  Protocol _protocol;

  CommandResult _lastProtocolResult;

  ConnectionImpl(this._socket, this._protocol);

  @override
  bool get isClosed => _protocol == null;

  @override
  Future<QueryResult> executeQuery(String query) async {
    await free();

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
      var hasColumn = columns.length > 0;
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

  @override
  Future<PreparedStatement> prepareQuery(String query) async {
    await free();

    try {
      _protocol.preparedStatementProtocol
          .writeCommandStatementPreparePacket(query);

      var response = await _protocol.preparedStatementProtocol
          .readCommandStatementPrepareResponse();

      if (response is! CommandStatementPrepareOkResponsePacket) {
        throw new PreparedStatementError(response.errorMessage);
      }

      List<ColumnDefinition> parameters = new List(response.numParams);
      var parameterIterator =
          new QueryColumnIteratorImpl(parameters.length, this);
      var hasParameter = parameters.length > 0;
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
      var hasColumn = columns.length > 0;
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

  @override
  Future close() async {
    if (isClosed) {
      throw new StateError("Connection closed");
    }

    var lastProtocolResult = _lastProtocolResult;

    _lastProtocolResult = null;

    await lastProtocolResult?.close();

    // TODO chiudo tutti gli eventuali statement ancora aperti (senza inviare la richiesta di chiusura del protocollo)

    var socket = _socket;

    _socket = null;
    _protocol = null;

    await socket.close();
  }

  Future free() async {
    // TODO cosa vuol dire liberare la connessione?

    if (isClosed) {
      throw new StateError("Connection closed");
    }

    var lastProtocolResult = _lastProtocolResult;

    _lastProtocolResult = null;

    await lastProtocolResult?.free();
  }
}

abstract class BaseQueryResultImpl implements QueryResult {
  @override
  final int affectedRows;

  @override
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

  @override
  int get columnCount => columns?.length;

  @override
  bool get isClosed => _rowIterator == null || _rowIterator.isClosed;

  @override
  Future<bool> next() => _rowIterator.next();

  @override
  rawNext() => _rowIterator.rawNext();

  @override
  String getStringValue(int index) => _rowIterator.getStringValue(index);

  @override
  num getNumValue(int index) => _rowIterator.getNumValue(index);

  @override
  bool getBoolValue(int index) => _rowIterator.getBoolValue(index);

  @override
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

  @override
  final List<ColumnDefinition> columns;

  CommandQueryResultImpl.resultSet(this.columns, this._connection)
      : super.resultSet();

  CommandQueryResultImpl.ok(
      int affectedRows, int lastInsertId, this._connection)
      : this.columns = null,
        super.ok(affectedRows, lastInsertId);

  @override
  RowIterator _createRowIterator() => new CommandQueryRowIteratorImpl(this);

  Protocol get _protocol => _connection._protocol;
}

class PreparedStatementImpl implements PreparedStatement {
  final ConnectionImpl _connection;

  final int _statementId;

  @override
  final List<ColumnDefinition> parameters;

  @override
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

  @override
  int get parameterCount => parameters.length;

  @override
  int get columnCount => columns.length;

  @override
  bool get isClosed => _isClosed;

  @override
  void setParameter(int index, value, [SqlType sqlType]) {
    if (_isClosed) {
      throw new StateError("Prepared statement closed");
    }

    if (index >= parameterCount) {
      throw new IndexError(index, _parameterValues);
    }

    var type = _getSqlType(sqlType);

    type ??= _connection._protocol.preparedStatementProtocol
        .getSqlTypeFromValue(value);

    if (type != null && _parameterTypes[index] != type) {
      _parameterTypes[index] = type;
      _isNewParamsBoundFlag = true;
    }

    _parameterValues[index] = value;
  }

  @override
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
        var columnIterator =
            new QueryColumnIteratorImpl(columnCount, _connection);
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

  @override
  RowIterator _createRowIterator() => new PreparedQueryRowIteratorImpl(this);

  @override
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

  @override
  bool get isClosed => _isClosed;

  @override
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

  @override
  Future<bool> next() {
    var value = rawNext();
    return value is Future ? value : new Future.value(value);
  }

  @override
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

  @override
  bool _isDataPacket(Packet packet) =>
      packet is ResultSetColumnDefinitionPacket;

  @override
  _readDataResponse() => _connection._protocol.queryCommandTextProtocol
      .readResultSetColumnDefinitionResponse();

  @override
  _skipDataResponse() => _connection._protocol.queryCommandTextProtocol
      .skipResultSetColumnDefinitionResponse();

  @override
  _free() => _connection._protocol.queryCommandTextProtocol.free();
}

abstract class BaseQueryRowIteratorImpl<T extends QueryResult>
    extends BaseDataIteratorImpl implements RowIterator {
  final T _result;

  BaseQueryRowIteratorImpl(this._result);
}

class CommandQueryRowIteratorImpl extends BaseQueryRowIteratorImpl {
  CommandQueryRowIteratorImpl(CommandQueryResultImpl result) : super(result);

  @override
  String getStringValue(int index) => _result._connection._protocol
      .queryCommandTextProtocol.reusableRowPacket.getUTF8String(index);

  @override
  num getNumValue(int index) {
    var formatted = _result._connection._protocol.queryCommandTextProtocol
        .reusableRowPacket.getString(index);
    return formatted != null ? num.parse(formatted) : null;
  }

  @override
  bool getBoolValue(int index) {
    var formatted = getNumValue(index);
    return formatted != null ? formatted != 0 : null;
  }

  @override
  bool _isDataPacket(Packet response) => response is ResultSetRowPacket;

  @override
  _readDataResponse() =>
      _result._protocol.queryCommandTextProtocol.readResultSetRowResponse();

  @override
  _skipDataResponse() =>
      _result._protocol.queryCommandTextProtocol.skipResultSetRowResponse();

  @override
  _free() => _result._connection._protocol.queryCommandTextProtocol.free();
}

class PreparedQueryRowIteratorImpl extends BaseQueryRowIteratorImpl {
  PreparedQueryRowIteratorImpl(PreparedQueryResultImpl result) : super(result);

  @override
  String getStringValue(int index) => _result._statement._connection._protocol
      .preparedStatementProtocol.reusableRowPacket.getUTF8String(index);

  @override
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

  @override
  bool getBoolValue(int index) {
    var formatted = getNumValue(index);
    return formatted != null ? formatted != 0 : null;
  }

  @override
  _skip() {
    var response = _result._statement._connection._protocol
        .preparedStatementProtocol.skipResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  @override
  bool _checkNext(Packet response) {
    if (response is PreparedResultSetRowPacket) {
      return true;
    } else {
      _isClosed = true;
      _result._statement._connection._protocol.preparedStatementProtocol.free();
      return false;
    }
  }

  @override
  bool _isDataPacket(Packet response) => response is PreparedResultSetRowPacket;

  @override
  _readDataResponse() =>
      _result._statement._connection._protocol.preparedStatementProtocol
          .readResultSetRowResponse(_result._statement._columnTypes);

  @override
  _skipDataResponse() =>
      _result._protocol.preparedStatementProtocol.skipResultSetRowResponse();

  @override
  _free() => _result._connection._protocol.preparedStatementProtocol.free();
}
