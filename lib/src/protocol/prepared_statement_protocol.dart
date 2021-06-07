part of mysql_client.protocol;

const int COM_STMT_PREPARE = 0x16;
const int COM_STMT_EXECUTE = 0x17;
const int COM_STMT_CLOSE = 0x19;
const int COM_STMT_RESET = 0x1a;

const int CURSOR_TYPE_NO_CURSOR = 0x00;

class PreparedStatementProtocol extends ProtocolDelegate {
  PreparedResultSetRowPacket? _reusableRowPacket;

  PreparedStatementProtocol(Protocol protocol) : super(protocol);

  PreparedResultSetRowPacket? get reusableRowPacket => _reusableRowPacket;

  void free() {
    super.free();

    _protocol.queryCommandTextProtocol.free();

    _reusableRowPacket?._free();
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
        //if (value != null) {
        var sqlType = getSqlTypeFromMysqlType(parameterTypes[i]);
        //print('####${sqlType}');
        // 这里可以正确识别NULL类型和DateTime等类型；
        switch (sqlType) {
          case SqlType.NULL:
            _writeLengthEncodedUTF8String('NULL');
            break;
          case SqlType.TINY:
            // value (1) -- integer
            _writeFixedLengthInteger(value ? 1 : 0, 1);
            break;
          case SqlType.DECIMAL:
            print('not exe decimal');
            _writeLengthEncodedUTF8String((value as Decimal).toString());
            break;
          case SqlType.DOUBLE:
            // value (string.fix_len) -- (len=8) double
            _writeDouble(value);
            break;
          case SqlType.LONG:
            // value (8) -- integer
            _writeFixedLengthInteger(value, 4);
            break;
          case SqlType.LONGLONG:
            // value (8) -- integer
            _writeFixedLengthInteger(value, 8);
            break;
          case SqlType.DATETIME:
            print('not exec datetime');
            final dt = value as DateTime;
            var month =
                dt.month.toString().length == 1 ? '0${dt.month}' : dt.month;
            var day = dt.day.toString().length == 1 ? '0${dt.day}' : dt.day;
            var hour = dt.hour.toString().length == 1 ? '0${dt.hour}' : dt.hour;
            var minute =
                dt.minute.toString().length == 1 ? '0${dt.minute}' : dt.minute;
            var second =
                dt.second.toString().length == 1 ? '0${dt.second}' : dt.second;
            var formatted =
                '${dt.year}-${month}-${day} ${hour}:${minute}:${second}.${dt.millisecond}';
            //print(formatted);
            // 成功执行
            _writeLengthEncodedUTF8String(formatted);
            break;
          //throw new UnsupportedError("DateTime not supported yet");
          case SqlType.VAR_STRING:
            var formattedVal = '';
            if (value is String) {
              formattedVal = value;
            } else if (value is DateTime) {
              // FLAG 如果要格式化特定的时间格式这里改
              formattedVal = value.toString();
            } else if (value is Decimal) {
              formattedVal = value.toString();
            }
            // value (lenenc_str) -- string
            _writeLengthEncodedUTF8String(formattedVal);
            break;
          default:
            throw new UnsupportedError("sql type not full supported $sqlType");
        }
        //}
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
    return value2 is Future<Packet>
        ? value2
        : new Future.value(value2 as Packet);
  }

  Future<Packet> readCommandStatementExecuteResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandStatementExecuteResponsePacket())
        : _readCommandStatementExecuteResponsePacket();
    return value2 is Future<Packet>
        ? value2
        : new Future.value(value2 as Packet);
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

  CommandStatementPrepareOkResponsePacket
      _readCommandStatementPrepareOkResponsePacket() {
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

    _reusableRowPacket = new PreparedResultSetRowPacket.reusable(
        _protocol, packet._columnCount!);

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
        var sqlType = getSqlTypeFromMysqlType(columnTypes[i]);
        switch (sqlType) {
          case SqlType.VAR_STRING:
            _readFixedLengthDataRange(
                _readLengthEncodedInteger(), reusableRange);
            break;
          case SqlType.DOUBLE:
            _readFixedLengthDataRange(8, reusableRange);
            break;
          case SqlType.LONG:
            _readFixedLengthDataRange(4, reusableRange);
            break;
          case SqlType.TINY:
            _readFixedLengthDataRange(1, reusableRange);
            break;
          //region 这些本质上其实都是没有实现的
          case SqlType.LONGLONG:
            _readFixedLengthDataRange(8, reusableRange);
            //_readFixedLengthInteger(8);
            break;
          // null decimal 不知道怎么实现，因为不清楚要读取多少字节。。
          // 但是可以通过上面的String实现，DateTime也不弄了用string实现
          // 但是用string实现是有问题的，比如null，数据库字段VarString是null值，但是以Num类型获取不会报错
          //endregion
          default:
            throw new UnsupportedError("sql type not full supported $sqlType");
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
