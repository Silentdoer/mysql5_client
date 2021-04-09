part of mysql_client.protocol;

const int COM_STMT_PREPARE = 0x16;
const int COM_STMT_EXECUTE = 0x17;
const int COM_STMT_CLOSE = 0x19;
const int COM_STMT_RESET = 0x1a;

const int CURSOR_TYPE_NO_CURSOR = 0x00;

enum DataType {
  string,
  integer8,
  integer4,
  integer2,
  integer1,
  double,
  float,
  dateTime
}

class PreparedStatementProtocol extends ProtocolDelegate {
  PreparedResultSetRowPacket? _reusableRowPacket;

  PreparedStatementProtocol(Protocol protocol) : super(protocol);

  PreparedResultSetRowPacket? get reusableRowPacket => _reusableRowPacket;

  void free() {
    super.free();

    _protocol.queryCommandTextProtocol.free();

    _reusableRowPacket?._free();
  }

  int? getSqlTypeFromValue(dynamic value) {
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

  void writeCommandStatementPreparePacket(String query) {
    _resetSequenceId();

    // command (1) -- [16] the COM_STMT_PREPARE command
    _writeFixedLengthInteger(COM_STMT_PREPARE, 1);
    // query (string.EOF) -- the query to prepare
    _writeFixedLengthUTF8String(query);

    _writePacket();
  }

  void writeCommandStatementExecutePacket(int statementId, List parameterValues,
      bool isNewParamsBoundFlag, List<int> parameterTypes) {
    _resetSequenceId();

    _createWriterBuffer();

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
            case DataType.integer1:
              // value (1) -- integer
              _writeFixedLengthInteger(value ? 1 : 0, 1);
              break;
            case DataType.double:
              // value (string.fix_len) -- (len=8) double
              _writeDouble(value);
              break;
            case DataType.integer8:
              // value (8) -- integer
              _writeFixedLengthInteger(value, 8);
              break;
            case DataType.dateTime:
              throw new UnsupportedError("DateTime not supported yet");
            case DataType.string:
              // value (lenenc_str) -- string
              _writeLengthEncodedUTF8String(value);
              break;
            default:
              throw new UnsupportedError("Data type not supported $dataType");
          }
        }
      }
    }

    _writePacket();
  }

  void writeCommandStatementResetPacket(int statementId) {
    _resetSequenceId();

    _createWriterBuffer();

    // 1              [1a] COM_STMT_RESET
    _writeFixedLengthInteger(COM_STMT_RESET, 1);
    // 4              statement-id
    _writeFixedLengthInteger(statementId, 4);

    _writePacket();
  }

  void writeCommandStatementClosePacket(int statementId) {
    _resetSequenceId();

    _createWriterBuffer();

    // 1              [19] COM_STMT_CLOSE
    _writeFixedLengthInteger(COM_STMT_CLOSE, 1);
    // 4              statement-id
    _writeFixedLengthInteger(statementId, 4);

    _writePacket();
  }

  Future<Packet> readCommandStatementPrepareResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandStatementPrepareResponsePacket())
        : _readCommandStatementPrepareResponsePacket();
    return value2 is Future<Packet> ? value2 : new Future.value(value2 as Packet);
  }

  Future<Packet> readCommandStatementExecuteResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandStatementExecuteResponsePacket())
        : _readCommandStatementExecuteResponsePacket();
    return value2 is Future<Packet> ? value2 : new Future.value(value2 as Packet);
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
        _payloadLength, _sequenceId);

    if (_header != 0x00) {
      throw new StateError("Invalid packet header: $_header != 0x00");
    }

    // status (1) -- [00] OK
    _skipByte();
    packet._status = _header;
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
    var packet = new ResultSetColumnCountPacket(_payloadLength, _sequenceId);

    // A packet containing a Protocol::LengthEncodedInteger column_count
    packet._columnCount = _readByte();

    _reusableRowPacket =
        new PreparedResultSetRowPacket.reusable(_protocol, packet._columnCount!);

    return packet;
  }

  PreparedResultSetRowPacket _skipResultSetRowPacket() {
    var packet = _reusableRowPacket!.reuse(_payloadLength, _sequenceId);

    _skipBytes(_payloadLength);

    return packet;
  }

  PreparedResultSetRowPacket _readResultSetRowPacket(List<int> columnTypes) {
    var packet = _reusableRowPacket!.reuse(_payloadLength, _sequenceId);

    // header
    _skipByte();

    var offset = 2;
    var nullBitmap =
        _readFixedLengthString((columnTypes.length + 7 + offset) ~/ 8)!
            .codeUnits;

    var l = offset ~/ 8;
    var n = offset % 8;
    var mask = 1 << n;
    for (var i = 0; i < columnTypes.length; i++) {
      var reusableRange = _reusableRowPacket!._getReusableDataRange(i);
      if ((nullBitmap[l] & mask) == 0) {
        var dataType = _getDataTypeFromSqlType(columnTypes[i]);
        switch (dataType) {
          case DataType.string:
            _readFixedLengthDataRange(
                _readLengthEncodedInteger(), reusableRange);
            break;
          case DataType.double:
            _readFixedLengthDataRange(8, reusableRange);
            break;
          case DataType.integer4:
            _readFixedLengthDataRange(4, reusableRange);
            break;
          case DataType.integer1:
            _readFixedLengthDataRange(1, reusableRange);
            break;
          default:
            throw new UnsupportedError("Data type not supported $dataType");
        }
      } else {
        reusableRange.reuseNil();
      }

      n++;
      if (n == 8) {
        l++;
        n = 0;
        mask = 1;
      } else {
        mask <<= 1;
      }
    }

    return packet;
  }

  DataType? _getDataTypeFromSqlType(int? sqlType) {
    if (sqlType == null) {
      return null;
    } else {
      switch (sqlType) {
        case MYSQL_TYPE_VAR_STRING:
          return DataType.string;
        case MYSQL_TYPE_LONG:
          return DataType.integer4;
        case MYSQL_TYPE_LONGLONG:
          return DataType.integer8;
        case MYSQL_TYPE_DOUBLE:
          return DataType.double;
        case MYSQL_TYPE_TINY:
          return DataType.integer1;
        case MYSQL_TYPE_DATETIME:
          return DataType.dateTime;
        case MYSQL_TYPE_TIMESTAMP:
          return DataType.dateTime;
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
}

class CommandStatementPrepareOkResponsePacket extends Packet {
  int? _status;
  int? _statementId;
  int? _numColumns;
  int? _numParams;
  int? _warningCount;

  CommandStatementPrepareOkResponsePacket(int? payloadLength, int? sequenceId)
      : super(payloadLength, sequenceId);

  int? get status => _status;
  int? get statementId => _statementId;
  int? get numColumns => _numColumns;
  int? get numParams => _numParams;
  int? get warningCount => _warningCount;
}

class PreparedResultSetRowPacket extends ReusablePacket {
  PreparedResultSetRowPacket.reusable(Protocol protocol, int columnCount)
      : super.reusable(protocol, columnCount);

  PreparedResultSetRowPacket reuse(int? payloadLength, int? sequenceId) =>
      _reuse(payloadLength, sequenceId) as PreparedResultSetRowPacket;
}
