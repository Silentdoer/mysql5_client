part of mysql_client.protocol;

const int COM_STMT_PREPARE = 0x16;
const int COM_STMT_EXECUTE = 0x17;
const int COM_STMT_CLOSE = 0x19;
const int COM_STMT_RESET = 0x1a;

const int CURSOR_TYPE_NO_CURSOR = 0x00;

enum DataType {
  STRING,
  INTEGER_8,
  INTEGER_4,
  INTEGER_2,
  INTEGER_1,
  DOUBLE,
  FLOAT,
  DATETIME
}

class PrepareStatementError extends Error {
  final String message;

  PrepareStatementError(this.message);

  String toString() => "PrepareStatementError: $message";
}

class PreparedStatementProtocol extends ProtocolDelegate {
  PreparedResultSetRowPacket _reusableRowPacket;

  PreparedStatementProtocol(Protocol protocol) : super(protocol);

  Future<PreparedStatement> prepareQuery(String query) async {
    _writeCommandStatementPreparePacket(query);

    var response = await _readCommandStatementPrepareResponse();

    if (response is! CommandStatementPrepareOkResponsePacket) {
      throw new PrepareStatementError(response.errorMessage);
    }

    List<ColumnDefinition> parameters = new List(response.numParams);
    var parameterIterator =
        new QueryColumnIterator(parameters.length, _protocol);
    var hasParameter = true;
    var i = 0;
    while (hasParameter) {
      hasParameter = await parameterIterator.next();
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
      hasColumn = await columnIterator.next();
      if (hasColumn) {
        columns[l++] =
            new ColumnDefinition(columnIterator.name, columnIterator.type);
      }
    }

    return new PreparedStatement(
        response.statementId, parameters, columns, _protocol);
  }

  Future<PreparedQueryResult> executeQuery(PreparedStatement statement) async {
    _writeCommandStatementExecutePacket(statement);

    var response = await _readCommandStatementExecuteResponse();

    if (response is ErrorPacket) {
      throw new QueryError(response.errorMessage);
    }

    statement._isNewParamsBoundFlag = false;

    if (response is OkPacket) {
      return new PreparedQueryResult.ok(
          response.affectedRows, response.lastInsertId);
    } else {
      var columnIterator =
          new QueryColumnIterator(statement.columnCount, _protocol);
      var hasColumn = true;
      while (hasColumn) {
        hasColumn = await columnIterator._skip();
      }

      return new PreparedQueryResult.resultSet(statement);
    }
  }

  void _writeCommandStatementPreparePacket(String query) {
    WriterBuffer buffer = _protocol._createBuffer();

    var sequenceId = 0x00;

    // command (1) -- [16] the COM_STMT_PREPARE command
    buffer.writeFixedLengthInteger(COM_STMT_PREPARE, 1);
    // query (string.EOF) -- the query to prepare
    buffer.writeFixedLengthUTF8String(query);

    var headerBuffer = _protocol._createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _protocol._writeBuffer(headerBuffer);
    _protocol._writeBuffer(buffer);
  }

  void _writeCommandStatementExecutePacket(PreparedStatement statement) {
    WriterBuffer buffer = _protocol._createBuffer();

    var sequenceId = 0x00;

    // 1              [17] COM_STMT_EXECUTE
    buffer.writeFixedLengthInteger(COM_STMT_EXECUTE, 1);
    // 4              stmt-id
    buffer.writeFixedLengthInteger(statement._statementId, 4);
    // 1              flags
    buffer.writeFixedLengthInteger(CURSOR_TYPE_NO_CURSOR, 1);
    // 4              iteration-count
    buffer.writeFixedLengthInteger(1, 4);
    // if num-params > 0:
    if (statement._parameterValues.isNotEmpty) {
      //   n              NULL-bitmap, length: (num-params+7)/8
      buffer.writeBytes(_encodeNullBitmap(statement._parameterValues));
      //   1              new-params-bound-flag
      buffer.writeFixedLengthInteger(
          statement._isNewParamsBoundFlag ? 1 : 0, 1);
      //   if new-params-bound-flag == 1:
      if (statement._isNewParamsBoundFlag) {
        //     n              type of each parameter, length: num-params * 2
        for (var type in statement._parameterTypes) {
          // the type as in Protocol::ColumnType
          buffer.writeFixedLengthInteger(type, 1);
          // a flag byte which has the highest bit set if the type is unsigned [80]
          buffer.writeFixedLengthInteger(0, 1);
        }
      }
      //   n              value of each parameter
      for (int i = 0; i < statement._parameterTypes.length; i++) {
        var value = statement._parameterValues[i];
        if (value != null) {
          var dataType = _getDataTypeFromSqlType(statement._parameterTypes[i]);
          switch (dataType) {
            case DataType.INTEGER_1:
              // value (1) -- integer
              buffer.writeFixedLengthInteger(value ? 1 : 0, 1);
              break;
            case DataType.DOUBLE:
              // value (string.fix_len) -- (len=8) double
              buffer.writeBytes(_encodeDouble(value));
              break;
            case DataType.INTEGER_8:
              // value (8) -- integer
              buffer.writeFixedLengthInteger(value, 8);
              break;
            case DataType.DATETIME:
              throw new UnsupportedError("DateTime not supported yet");
            case DataType.STRING:
              // value (lenenc_str) -- string
              buffer.writeLengthEncodedUTF8String(value);
              break;
            default:
              throw new UnsupportedError("Data type not supported $dataType");
          }
        }
      }
    }

    var headerBuffer = _protocol._createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _protocol._writeBuffer(headerBuffer);
    _protocol._writeBuffer(buffer);
  }

  void _writeCommandStatementResetPacket(int statementId) {
    WriterBuffer buffer = _protocol._createBuffer();

    var sequenceId = 0x00;

    // 1              [1a] COM_STMT_RESET
    buffer.writeFixedLengthInteger(COM_STMT_RESET, 1);
    // 4              statement-id
    buffer.writeFixedLengthInteger(statementId, 4);

    var headerBuffer = _protocol._createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _protocol._writeBuffer(headerBuffer);
    _protocol._writeBuffer(buffer);
  }

  void _writeCommandStatementClosePacket(int statementId) {
    WriterBuffer buffer = _protocol._createBuffer();

    var sequenceId = 0x00;

    // 1              [19] COM_STMT_CLOSE
    buffer.writeFixedLengthInteger(COM_STMT_CLOSE, 1);
    // 4              statement-id
    buffer.writeFixedLengthInteger(statementId, 4);

    var headerBuffer = _protocol._createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _protocol._writeBuffer(headerBuffer);
    _protocol._writeBuffer(buffer);
  }

  Future<Packet> _readCommandStatementPrepareResponse() {
    var value = _protocol._readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandStatementPrepareResponsePacket())
        : _readCommandStatementPrepareResponsePacket();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  Future<Packet> _readCommandStatementExecuteResponse() {
    var value = _protocol._readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandStatementExecuteResponsePacket())
        : _readCommandStatementExecuteResponsePacket();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  _skipResultSetRowResponse(PreparedQueryResult result) {
    var value = _protocol._readPacketBuffer();
    return value is Future
        ? value.then((_) => _skipResultSetRowResponsePacket(result))
        : _skipResultSetRowResponsePacket(result);
  }

  _readResultSetRowResponse(PreparedQueryResult result) {
    var value = _protocol._readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetRowResponsePacket(result))
        : _readResultSetRowResponsePacket(result);
  }

  Packet _readCommandStatementPrepareResponsePacket() {
    if (_protocol._isErrorPacket()) {
      return _protocol._readErrorPacket();
    } else {
      return _readCommandStatementPrepareOkResponsePacket();
    }
  }

  Packet _readCommandStatementExecuteResponsePacket() {
    if (_protocol._isOkPacket()) {
      return _protocol._readOkPacket();
    } else if (_protocol._isErrorPacket()) {
      return _protocol._readErrorPacket();
    } else {
      return _readResultSetColumnCountPacket();
    }
  }

  Packet _skipResultSetRowResponsePacket(PreparedQueryResult result) {
    if (_protocol._isErrorPacket()) {
      _reusableRowPacket.free();

      return _protocol._readErrorPacket();
    } else if (_protocol._isEOFPacket()) {
      _reusableRowPacket.free();

      return _protocol._readEOFPacket();
    } else {
      return _skipResultSetRowPacket(result);
    }
  }

  Packet _readResultSetRowResponsePacket(PreparedQueryResult result) {
    if (_protocol._isErrorPacket()) {
      _reusableRowPacket.free();

      return _protocol._readErrorPacket();
    } else if (_protocol._isEOFPacket()) {
      _reusableRowPacket.free();

      return _protocol._readEOFPacket();
    } else {
      return _readResultSetRowPacket(result);
    }
  }

  CommandStatementPrepareOkResponsePacket _readCommandStatementPrepareOkResponsePacket() {
    var packet = new CommandStatementPrepareOkResponsePacket(
        _protocol._reusablePacketBuffer.sequenceId,
        _protocol._reusablePacketBuffer.payloadLength);

    // status (1) -- [00] OK
    packet._status = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _protocol._reusableDataRange)
        .toInt();
    // statement_id (4) -- statement-id
    packet._statementId = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(4, _protocol._reusableDataRange)
        .toInt();
    // num_columns (2) -- number of columns
    packet._numColumns = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _protocol._reusableDataRange)
        .toInt();
    // num_params (2) -- number of params
    packet._numParams = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _protocol._reusableDataRange)
        .toInt();
    // reserved_1 (1) -- [00] filler
    _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _protocol._reusableDataRange);
    // warning_count (2) -- number of warnings
    packet._warningCount = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _protocol._reusableDataRange)
        .toInt();

    _protocol._reusablePacketBuffer.free();
    _protocol._reusableDataRange.free();

    return packet;
  }

  ResultSetColumnCountPacket _readResultSetColumnCountPacket() {
    var packet = new ResultSetColumnCountPacket(
        _protocol._reusablePacketBuffer.sequenceId,
        _protocol._reusablePacketBuffer.payloadLength);

    // A packet containing a Protocol::LengthEncodedInteger column_count
    packet._columnCount = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _protocol._reusableDataRange)
        .toInt();

    _protocol._reusablePacketBuffer.free();
    _protocol._reusableDataRange.free();

    _reusableRowPacket =
        new PreparedResultSetRowPacket.reusable(packet._columnCount);

    return packet;
  }

  PreparedResultSetRowPacket _skipResultSetRowPacket(
      PreparedQueryResult result) {
    var packet = _reusableRowPacket.reuse(
        _protocol._reusablePacketBuffer.sequenceId,
        _protocol._reusablePacketBuffer.payloadLength);

    _protocol._reusablePacketBuffer.payload
        .skipBytes(_protocol._reusablePacketBuffer.payloadLength);

    _protocol._reusablePacketBuffer.free();

    return packet;
  }

  PreparedResultSetRowPacket _readResultSetRowPacket(
      PreparedQueryResult result) {
    var packet = _reusableRowPacket.reuse(
        _protocol._reusablePacketBuffer.sequenceId,
        _protocol._reusablePacketBuffer.payloadLength);

    var header = _protocol._reusablePacketBuffer.payload.readOneLengthInteger();

    var nullBitmap = _protocol._reusablePacketBuffer.payload
        .readFixedLengthString((result.columnCount + 7 + 2) ~/ 8)
        .codeUnits;

    for (var i = 0; i < result.columnCount; i++) {
      var reusableRange = _reusableRowPacket._getDataRange(i);

      if (!_isNullInNullBitmap(nullBitmap, i, 2)) {
        var column = result._statement.columns[i];
        var dataType = _getDataTypeFromSqlType(column.type);
        switch (dataType) {
          case DataType.STRING:
            _protocol._reusablePacketBuffer.payload.readFixedLengthDataRange(
                _protocol._reusablePacketBuffer.payload
                    .readLengthEncodedDataRange(reusableRange)
                    .toInt(),
                reusableRange);
            break;
          case DataType.DOUBLE:
            _protocol._reusablePacketBuffer.payload
                .readFixedLengthDataRange(8, reusableRange);
            break;
          case DataType.INTEGER_4:
            _protocol._reusablePacketBuffer.payload
                .readFixedLengthDataRange(4, reusableRange);
            break;
          case DataType.INTEGER_1:
            _protocol._reusablePacketBuffer.payload
                .readFixedLengthDataRange(1, reusableRange);
            break;
          default:
            throw new UnsupportedError("Data type not supported $dataType");
        }
      } else {
        reusableRange.reuseNil();
      }
    }

    _protocol._reusablePacketBuffer.free();
    _protocol._reusableDataRange.free();

    return packet;
  }

  int _getSqlTypeFromValue(value) {
    if (value == null) {
      return null;
    } else if (value is String) {
      return MYSQL_TYPE_VAR_STRING;
    } else if (value is int) {
      return MYSQL_TYPE_LONGLONG;
    } else if (value is double) {
      return MYSQL_TYPE_DOUBLE;
    } else if (value is bool) {
      return MYSQL_TYPE_TINY;
    } else if (value is DateTime) {
      return MYSQL_TYPE_DATETIME;
    } else {
      throw new UnsupportedError(
          "Value type not supported ${value.runtimeType}");
    }
  }

  DataType _getDataTypeFromSqlType(int sqlType) {
    if (sqlType == null) {
      return null;
    } else {
      switch (sqlType) {
        case MYSQL_TYPE_VAR_STRING:
          return DataType.STRING;
        case MYSQL_TYPE_LONG:
          return DataType.INTEGER_4;
        case MYSQL_TYPE_LONGLONG:
          return DataType.INTEGER_8;
        case MYSQL_TYPE_DOUBLE:
          return DataType.DOUBLE;
        case MYSQL_TYPE_TINY:
          return DataType.INTEGER_1;
        case MYSQL_TYPE_DATETIME:
          return DataType.DATETIME;
        case MYSQL_TYPE_TIMESTAMP:
          return DataType.DATETIME;
        default:
          throw new UnsupportedError("Sql type not supported $sqlType");
      }
    }
  }

  // TODO verificare se esistono conversioni più snelle
  List<int> _encodeDouble(double value) =>
      new Float64List.fromList([value]).buffer.asUint8List();

  // TODO verificare se esistono conversioni più snelle
  double _decodeDouble(List<int> data) =>
      new Uint8List.fromList(data).buffer.asFloat64List()[0];

  List<int> _encodeNullBitmap(List parameters, [int offset = 0]) {
    //   n              NULL-bitmap, length: (num-params+7)/8
    var bitmap = new List.filled((parameters.length + 7 + offset) ~/ 8, 0);

    var l = offset ~/ 8;
    var i = offset % 8;
    var mask = 1 << i;
    for (var parameter in parameters) {
      if (parameter == null) {
        bitmap[l] |= mask;
      }

      i++;
      if (i == 8) {
        i = 0;
        l++;
        mask = 1;
      } else {
        mask <<= 1;
      }
    }

    return bitmap;
  }

  bool _isNullInNullBitmap(List<int> nullBitmap, int index, [int offset = 0]) {
    var l = (index + offset) ~/ 8;
    var i = (index + offset) % 8;
    var mask = 1 << i;
    return (nullBitmap[l] & mask) != 0;
  }
}

class PreparedStatement implements ProtocolResult {
  final Protocol _protocol;

  final int _statementId;

  final List<ColumnDefinition> parameters;
  final List<ColumnDefinition> columns;

  final List<int> _parameterTypes;
  final List _parameterValues;

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
  }

  int get parameterCount => parameters.length;

  int get columnCount => columns.length;

  bool get isClosed => _isClosed;

  void setParameter(int index, value, [int sqlType]) {
    if (index >= parameterCount) {
      throw new IndexError(index, _parameterValues);
    }

    sqlType ??=
        _protocol._preparedStatementProtocol._getSqlTypeFromValue(value);

    if (sqlType != null && _parameterTypes[index] != sqlType) {
      _parameterTypes[index] = sqlType;
      _isNewParamsBoundFlag = true;
    }

    _parameterValues[index] = value;
  }

  Future<PreparedQueryResult> executeQuery() async {
    // TODO check dello stato

    var result = await _protocol.preparedStatementProtocol.executeQuery(this);

    return result;
  }

  @override
  Future free() async {}

  @override
  Future close() async {
    // TODO implementare PreparedStatement.close

    _protocol._preparedStatementProtocol
        ._writeCommandStatementClosePacket(_statementId);
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

class PreparedQueryRowIterator extends PacketIterator {
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

  Future<bool> nextAsFuture() {
    var value = next();
    return value is Future ? value : new Future.value(value);
  }

  next() {
    if (isClosed) {
      throw new StateError("Column iterator closed");
    }

    var response = _result._statement._protocol._preparedStatementProtocol
        ._readResultSetRowResponse(_result);

    return response is Future
        ? response.then((response) => _checkLast(response))
        : _checkLast(response);
  }

  String getStringValue(int index) => _result._statement._protocol
      ._preparedStatementProtocol._reusableRowPacket._getUTF8String(index);

  num getNumValue(int index) {
    var column = _result._statement.columns[index];
    switch (column.type) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
        return _result._statement._protocol._preparedStatementProtocol
            ._reusableRowPacket._getInt(index);
      case MYSQL_TYPE_DOUBLE:
        return _result._statement._protocol._preparedStatementProtocol
            ._reusableRowPacket._getDataRange(index).toDouble();
      default:
        throw new UnsupportedError("Sql type not supported ${column.type}");
    }
  }

  bool getBoolValue(int index) {
    var formatted = getNumValue(index);
    return formatted != null ? formatted != 0 : null;
  }

  _skip() {
    var response = _result._statement._protocol._preparedStatementProtocol
        ._skipResultSetRowResponse(_result);

    return response is Future
        ? response.then((response) => _checkLast(response))
        : _checkLast(response);
  }

  bool _checkLast(Packet response) {
    _isClosed = response is! PreparedResultSetRowPacket;
    return !_isClosed;
  }
}

class CommandStatementPrepareOkResponsePacket extends Packet {
  int _status;
  int _statementId;
  int _numColumns;
  int _numParams;
  int _warningCount;

  CommandStatementPrepareOkResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);

  int get status => _status;
  int get statementId => _statementId;
  int get numColumns => _numColumns;
  int get numParams => _numParams;
  int get warningCount => _warningCount;
}

class PreparedResultSetRowPacket extends ReusablePacket {
  PreparedResultSetRowPacket.reusable(int columnCount)
      : super.reusable(columnCount);

  PreparedResultSetRowPacket reuse(int payloadLength, int sequenceId) =>
      _reuse(payloadLength, sequenceId);
}
