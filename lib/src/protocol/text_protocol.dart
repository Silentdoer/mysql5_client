part of mysql_client.protocol;

const int COM_QUERY = 0x03;

const int MYSQL_TYPE_TINY = 0x01;
const int MYSQL_TYPE_LONG = 0x03;
const int MYSQL_TYPE_DOUBLE = 0x05;
const int MYSQL_TYPE_NULL = 0x06;
const int MYSQL_TYPE_TIMESTAMP = 0x07;
const int MYSQL_TYPE_LONGLONG = 0x08;
const int MYSQL_TYPE_DATETIME = 0x0c;
const int MYSQL_TYPE_VAR_STRING = 0xfd;

class QueryCommandTextProtocol extends ProtocolDelegate {
  final ResultSetColumnDefinitionPacket _reusableColumnPacket;

  ResultSetRowPacket _reusableRowPacket;

  QueryCommandTextProtocol(Protocol protocol)
      : _reusableColumnPacket =
            new ResultSetColumnDefinitionPacket.reusable(protocol),
        super(protocol);

  ResultSetColumnDefinitionPacket get reusableColumnPacket =>
      _reusableColumnPacket;

  ResultSetRowPacket get reusableRowPacket => _reusableRowPacket;

  void free() {
    super.free();

    _reusableColumnPacket._free();
    _reusableRowPacket?._free();
  }

  void writeCommandQueryPacket(String query) {
    _createWriterBuffer();

    var sequenceId = 0x00;

    // 1              [03] COM_QUERY
    _writeFixedLengthInteger(COM_QUERY, 1);
    // string[EOF]    the query the server shall execute
    _writeFixedLengthUTF8String(query);

    _writePacket(sequenceId);
  }

  Future<Packet> readCommandQueryResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandQueryResponsePacket())
        : _readCommandQueryResponsePacket();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  skipResultSetColumnDefinitionResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _skipResultSetColumnDefinitionResponsePacket())
        : _skipResultSetColumnDefinitionResponsePacket();
  }

  readResultSetColumnDefinitionResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetColumnDefinitionResponsePacket())
        : _readResultSetColumnDefinitionResponsePacket();
  }

  skipResultSetRowResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _skipResultSetRowResponsePacket())
        : _skipResultSetRowResponsePacket();
  }

  readResultSetRowResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetRowResponsePacket())
        : _readResultSetRowResponsePacket();
  }

  Packet _readCommandQueryResponsePacket() {
    if (_isOkPacket()) {
      return _readOkPacket();
    } else if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isLocalInFilePacket()) {
      throw new UnsupportedError("Protocol::LOCAL_INFILE_Data");
    } else {
      return _readResultSetColumnCountPacket();
    }
  }

  Packet _skipResultSetColumnDefinitionResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _skipResultSetColumnDefinitionPacket();
    }
  }

  Packet _readResultSetColumnDefinitionResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _readResultSetColumnDefinitionPacket();
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

  Packet _readResultSetRowResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _readResultSetRowPacket();
    }
  }

  ResultSetColumnCountPacket _readResultSetColumnCountPacket() {
    var packet = new ResultSetColumnCountPacket(_payloadLength, _sequenceId);

    // A packet containing a Protocol::LengthEncodedInteger column_count
    packet._columnCount = _readLengthEncodedInteger();

    _reusableRowPacket =
        new ResultSetRowPacket.reusable(_protocol, packet._columnCount);

    return packet;
  }

  ResultSetColumnDefinitionPacket _skipResultSetColumnDefinitionPacket() {
    var packet = _reusableColumnPacket.reuse(_payloadLength, _sequenceId);

    _skipBytes(_payloadLength);

    return packet;
  }

  ResultSetColumnDefinitionPacket _readResultSetColumnDefinitionPacket() {
    var packet = _reusableColumnPacket.reuse(_payloadLength, _sequenceId);

    var dataRange;
    var i = 0;
    // lenenc_str     catalog
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     schema
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     table
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     org_table
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     name
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     org_name
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_int     length of fixed-length fields [0c]
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readLengthEncodedDataRange(dataRange);
    // 2              character set
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(2, dataRange);
    // 4              column length
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(4, dataRange);
    // 1              type
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(1, dataRange);
    // 2              flags
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(2, dataRange);
    // 1              decimals
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(1, dataRange);
    // 2              filler [00] [00]
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(2, dataRange);

    return packet;
  }

  ResultSetRowPacket _skipResultSetRowPacket() {
    var packet = _reusableRowPacket.reuse(_payloadLength, _sequenceId);

    _skipBytes(_payloadLength);

    return packet;
  }

  ResultSetRowPacket _readResultSetRowPacket() {
    var packet = _reusableRowPacket.reuse(_payloadLength, _sequenceId);

    var i = 0;
    while (!_isAllRead) {
      var reusableRange = _reusableRowPacket._getReusableDataRange(i++);
      if (_checkByte() != PREFIX_NULL) {
        _readFixedLengthDataRange(_readLengthEncodedInteger(), reusableRange);
      } else {
        _skipByte();
        reusableRange.reuseNil();
      }
    }

    return packet;
  }

  bool _isLocalInFilePacket() => _header == 0xfb;
}

class ResultSetColumnCountPacket extends Packet {
  int _columnCount;

  ResultSetColumnCountPacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);

  int get columnCount => _columnCount;
}

class ResultSetColumnDefinitionPacket extends ReusablePacket {
  ResultSetColumnDefinitionPacket.reusable(Protocol protocol)
      : super.reusable(protocol, 13);

  ResultSetColumnDefinitionPacket reuse(int payloadLength, int sequenceId) =>
      _reuse(payloadLength, sequenceId);

  String get catalog => getString(0);
  String get schema => getString(1);
  String get table => getString(2);
  String get orgTable => getString(3);
  String get name => getString(4);
  String get orgName => getString(5);
  int get fieldsLength => getInteger(6);
  int get characterSet => getInteger(7);
  int get columnLength => getInteger(8);
  int get type => getInteger(9);
  int get flags => getInteger(10);
  int get decimals => getInteger(11);
}

class ResultSetRowPacket extends ReusablePacket {
  ResultSetRowPacket.reusable(Protocol protocol, int columnCount)
      : super.reusable(protocol, columnCount);

  ResultSetRowPacket reuse(int payloadLength, int sequenceId) =>
      _reuse(payloadLength, sequenceId);
}
