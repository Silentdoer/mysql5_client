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

class PreparedStatementProtocol extends ProtocolDelegate {
  PreparedResultSetRowPacket _reusableRowPacket;

  PreparedStatementProtocol(Protocol protocol) : super(protocol);

  PreparedResultSetRowPacket get reusableRowPacket => _reusableRowPacket;

  void freeReusables() {
    super.freeReusables();

    _protocol.queryCommandTextProtocol.freeReusables();

    _reusableRowPacket?._free();
  }

  void writeCommandStatementPreparePacket(String query) {
    _createWriterBuffer();

    var sequenceId = 0x00;

    // command (1) -- [16] the COM_STMT_PREPARE command
    _writeFixedLengthInteger(COM_STMT_PREPARE, 1);
    // query (string.EOF) -- the query to prepare
    _writeFixedLengthUTF8String(query);

    _writePacket(sequenceId);
  }

  void writeCommandStatementExecutePacket(int statementId, List parameterValues,
      bool isNewParamsBoundFlag, List<int> parameterTypes) {
    _createWriterBuffer();

    var sequenceId = 0x00;

    // 1              [17] COM_STMT_EXECUTE
    _writeFixedLengthInteger(COM_STMT_EXECUTE, 1);
    // 4              stmt-id
    _writeFixedLengthInteger(statementId, 4);
    // 1              flags
    _writeFixedLengthInteger(CURSOR_TYPE_NO_CURSOR, 1);
    // 4              iteration-count
    _writeFixedLengthInteger(1, 4);
    // if num-params > 0:
    if (parameterValues.isNotEmpty) {
      //   n              NULL-bitmap, length: (num-params+7)/8
      _writeBytes(_encodeNullBitmap(parameterValues));
      //   1              new-params-bound-flag
      _writeFixedLengthInteger(isNewParamsBoundFlag ? 1 : 0, 1);
      //   if new-params-bound-flag == 1:
      if (isNewParamsBoundFlag) {
        //     n              type of each parameter, length: num-params * 2
        for (var type in parameterTypes) {
          // the type as in Protocol::ColumnType
          _writeFixedLengthInteger(type, 1);
          // a flag byte which has the highest bit set if the type is unsigned [80]
          _writeFixedLengthInteger(0, 1);
        }
      }
      //   n              value of each parameter
      for (int i = 0; i < parameterTypes.length; i++) {
        var value = parameterValues[i];
        if (value != null) {
          var dataType = _getDataTypeFromSqlType(parameterTypes[i]);
          switch (dataType) {
            case DataType.INTEGER_1:
              // value (1) -- integer
              _writeFixedLengthInteger(value ? 1 : 0, 1);
              break;
            case DataType.DOUBLE:
              // value (string.fix_len) -- (len=8) double
              _writeDouble(value);
              break;
            case DataType.INTEGER_8:
              // value (8) -- integer
              _writeFixedLengthInteger(value, 8);
              break;
            case DataType.DATETIME:
              throw new UnsupportedError("DateTime not supported yet");
            case DataType.STRING:
              // value (lenenc_str) -- string
              _writeLengthEncodedUTF8String(value);
              break;
            default:
              throw new UnsupportedError("Data type not supported $dataType");
          }
        }
      }
    }

    _writePacket(sequenceId);
  }

  void writeCommandStatementResetPacket(int statementId) {
    _createWriterBuffer();

    var sequenceId = 0x00;

    // 1              [1a] COM_STMT_RESET
    _writeFixedLengthInteger(COM_STMT_RESET, 1);
    // 4              statement-id
    _writeFixedLengthInteger(statementId, 4);

    _writePacket(sequenceId);
  }

  void writeCommandStatementClosePacket(int statementId) {
    _createWriterBuffer();

    var sequenceId = 0x00;

    // 1              [19] COM_STMT_CLOSE
    _writeFixedLengthInteger(COM_STMT_CLOSE, 1);
    // 4              statement-id
    _writeFixedLengthInteger(statementId, 4);

    _writePacket(sequenceId);
  }

  Future<Packet> readCommandStatementPrepareResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandStatementPrepareResponsePacket())
        : _readCommandStatementPrepareResponsePacket();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  Future<Packet> readCommandStatementExecuteResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandStatementExecuteResponsePacket())
        : _readCommandStatementExecuteResponsePacket();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  skipResultSetRowResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _skipResultSetRowResponsePacket())
        : _skipResultSetRowResponsePacket();
  }

  readResultSetRowResponse(List<int> columnTypes) {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetRowResponsePacket(columnTypes))
        : _readResultSetRowResponsePacket(columnTypes);
  }

  Packet _readCommandStatementPrepareResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else {
      return _readCommandStatementPrepareOkResponsePacket();
    }
  }

  Packet _readCommandStatementExecuteResponsePacket() {
    if (_isOkPacket()) {
      return _readOkPacket();
    } else if (_isErrorPacket()) {
      return _readErrorPacket();
    } else {
      return _readResultSetColumnCountPacket();
    }
  }

  Packet _skipResultSetRowResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _skipResultSetRowPacket();
    }
  }

  Packet _readResultSetRowResponsePacket(List<int> columnTypes) {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _readResultSetRowPacket(columnTypes);
    }
  }

  CommandStatementPrepareOkResponsePacket _readCommandStatementPrepareOkResponsePacket() {
    var packet = new CommandStatementPrepareOkResponsePacket(
        _sequenceId, _payloadLength);

    // status (1) -- [00] OK
    packet._status = _readByte();
    // statement_id (4) -- statement-id
    packet._statementId = _readFixedLengthInteger(4);
    // num_columns (2) -- number of columns
    packet._numColumns = _readFixedLengthInteger(2);
    // num_params (2) -- number of params
    packet._numParams = _readFixedLengthInteger(2);
    // reserved_1 (1) -- [00] filler
    _skipByte();
    // warning_count (2) -- number of warnings
    packet._warningCount = _readFixedLengthInteger(2);

    return packet;
  }

  ResultSetColumnCountPacket _readResultSetColumnCountPacket() {
    var packet = new ResultSetColumnCountPacket(_sequenceId, _payloadLength);

    // A packet containing a Protocol::LengthEncodedInteger column_count
    packet._columnCount = _readByte();

    _reusableRowPacket =
        new PreparedResultSetRowPacket.reusable(_protocol, packet._columnCount);

    return packet;
  }

  PreparedResultSetRowPacket _skipResultSetRowPacket() {
    var packet = _reusableRowPacket.reuse(_sequenceId, _payloadLength);

    _skipBytes(_payloadLength);

    return packet;
  }

  PreparedResultSetRowPacket _readResultSetRowPacket(List<int> columnTypes) {
    var packet = _reusableRowPacket.reuse(_sequenceId, _payloadLength);

    // header
    _skipByte();

    var nullBitmap =
        _readFixedLengthString((columnTypes.length + 7 + 2) ~/ 8).codeUnits;

    for (var i = 0; i < columnTypes.length; i++) {
      var reusableRange = _reusableRowPacket._getReusableDataRange(i);

      // TODO ottimizzare la verifica null
      if (!_isNullInNullBitmap(nullBitmap, i, 2)) {
        var dataType = _getDataTypeFromSqlType(columnTypes[i]);
        switch (dataType) {
          case DataType.STRING:
            _readFixedLengthDataRange(
                _readLengthEncodedInteger(), reusableRange);
            break;
          case DataType.DOUBLE:
            _readFixedLengthDataRange(8, reusableRange);
            break;
          case DataType.INTEGER_4:
            _readFixedLengthDataRange(4, reusableRange);
            break;
          case DataType.INTEGER_1:
            _readFixedLengthDataRange(1, reusableRange);
            break;
          default:
            throw new UnsupportedError("Data type not supported $dataType");
        }
      } else {
        reusableRange.reuseNil();
      }
    }

    return packet;
  }

  int getSqlTypeFromValue(value) {
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
  PreparedResultSetRowPacket.reusable(Protocol protocol, int columnCount)
      : super.reusable(protocol, columnCount);

  PreparedResultSetRowPacket reuse(int payloadLength, int sequenceId) =>
      _reuse(payloadLength, sequenceId);
}
