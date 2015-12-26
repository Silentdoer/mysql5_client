part of mysql_client.protocol;

class QueryError extends Error {
  final String message;

  QueryError(this.message);

  String toString() => "QueryError: $message";
}

class QueryCommandTextProtocol extends Protocol {
  final ResultSetColumnDefinitionResponsePacket _reusableColumnPacket =
      new ResultSetColumnDefinitionResponsePacket.reusable();

  ResultSetRowResponsePacket _reusableRowPacket;

  QueryCommandTextProtocol(DataWriter writer, DataReader reader,
      int serverCapabilityFlags, int clientCapabilityFlags)
      : super(writer, reader, serverCapabilityFlags, clientCapabilityFlags);

  Future<QueryResult> executeQuery(String query) async {
    _writeCommandQueryPacket(query);

    var response = await _readCommandQueryResponse();

    if (response is OkPacket) {
      return new QueryResult.ok(response.affectedRows);
    }

    if (response is! ResultSetColumnCountResponsePacket) {
      throw new QueryError(response.errorMessage);
    }

    return new QueryResult.resultSet(response.columnCount, this);
  }

  void _writeCommandQueryPacket(String query) {
    WriterBuffer buffer = _writer.createBuffer();

    var sequenceId = 0x00;

    // 1              [03] COM_QUERY
    buffer.writeFixedLengthInteger(COM_QUERY, 1);
    // string[EOF]    the query the server shall execute
    buffer.writeFixedLengthUTF8String(query);

    var headerBuffer = _writer.createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _writer.writeBuffer(headerBuffer);
    _writer.writeBuffer(buffer);
  }

  Future<Packet> _readCommandQueryResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandQueryResponseInternal())
        : _readCommandQueryResponseInternal();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  _readResultSetColumnDefinitionResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetColumnDefinitionResponseInternal())
        : _readResultSetColumnDefinitionResponseInternal();
  }

  _readResultSetRowResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetRowResponseInternal())
        : _readResultSetRowResponseInternal();
  }

  Packet _readCommandQueryResponseInternal() {
    if (_isOkPacket()) {
      return _readOkPacket();
    } else if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isLocalInFilePacket()) {
      throw new UnsupportedError("Protocol::LOCAL_INFILE_Data");
    } else {
      return _readResultSetColumnCountResponsePacket();
    }
  }

  Packet _readResultSetColumnDefinitionResponseInternal() {
    if (_isErrorPacket()) {
      _reusableColumnPacket.free();

      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      _reusableColumnPacket.free();

      return _readEOFPacket();
    } else {
      return _readResultSetColumnDefinitionResponsePacket();
    }
  }

  Packet _readResultSetRowResponseInternal() {
    if (_isErrorPacket()) {
      _reusableRowPacket.free();

      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      _reusableRowPacket.free();

      return _readEOFPacket();
    } else {
      return _readResultSetRowResponsePacket();
    }
  }

  ResultSetColumnCountResponsePacket _readResultSetColumnCountResponsePacket() {
    var packet = new ResultSetColumnCountResponsePacket(
        _reusablePacketBuffer.sequenceId, _reusablePacketBuffer.payloadLength);

    // A packet containing a Protocol::LengthEncodedInteger column_count
    packet._columnCount = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _reusableDataRange)
        .toInt();

    _reusablePacketBuffer.free();
    _reusableDataRange.free();

    _reusableRowPacket =
        new ResultSetRowResponsePacket.reusable(packet._columnCount);

    return packet;
  }

  ResultSetColumnDefinitionResponsePacket _readResultSetColumnDefinitionResponsePacket() {
    var packet = _reusableColumnPacket.reuse(
        _reusablePacketBuffer.sequenceId, _reusablePacketBuffer.payloadLength);

    var dataRange;
    var i = 0;
    // lenenc_str     catalog
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     schema
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     table
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     org_table
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     name
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     org_name
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_int     length of fixed-length fields [0c]
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readLengthEncodedDataRange(dataRange);
    // 2              character set
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(2, dataRange);
    // 4              column length
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(4, dataRange);
    // 1              type
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(1, dataRange);
    // 2              flags
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(2, dataRange);
    // 1              decimals
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(1, dataRange);
    // 2              filler [00] [00]
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(2, dataRange);

    _reusablePacketBuffer.free();
    _reusableDataRange.free();

    return packet;
  }

  ResultSetRowResponsePacket _readResultSetRowResponsePacket() {
    var packet = _reusableRowPacket.reuse(
        _reusablePacketBuffer.sequenceId, _reusablePacketBuffer.payloadLength);

    var i = 0;
    while (!_reusablePacketBuffer.payload.isAllRead) {
      var reusableRange = _reusableRowPacket.getReusableRange(i++);
      if (_reusablePacketBuffer.payload.checkOneLengthInteger() !=
          PREFIX_NULL) {
        _reusablePacketBuffer.payload.readFixedLengthDataRange(
            _reusablePacketBuffer.payload
                .readLengthEncodedDataRange(reusableRange)
                .toInt(),
            reusableRange);
      } else {
        _reusablePacketBuffer.payload.skipByte();
        reusableRange.reuseNil();
      }
    }

    _reusablePacketBuffer.free();
    _reusableDataRange.free();

    return packet;
  }
}

class QueryResult {
  final int affectedRows;

  final int columnCount;

  final QueryCommandTextProtocol _protocol;

  QueryColumnIterator _columnIterator;

  QueryRowIterator _rowIterator;

  QueryResult.resultSet(this.columnCount, this._protocol)
      : this.affectedRows = 0;

  QueryResult.ok(this.affectedRows)
      : this.columnCount = 0,
        this._protocol = null;

  QueryColumnIterator get columnIterator {
    // TODO check dello stato

    // TODO riutilizzare il QueryColumnIterator

    _columnIterator = new QueryColumnIterator(_protocol);

    return _columnIterator;
  }

  QueryRowIterator get rowIterator {
    // TODO check dello stato

    // TODO riutilizzare il QueryRowIterator

    _rowIterator = new QueryRowIterator(_protocol);

    return _rowIterator;
  }
}

class QueryColumnIterator extends PacketIterator {
  final QueryCommandTextProtocol _protocol;

  QueryColumnIterator(this._protocol);

  Future<bool> next() {
    var value = internalNext();
    return value is Future ? value : new Future.value(value);
  }

  internalNext() {
    // TODO check dello stato

    var response = _protocol._readResultSetColumnDefinitionResponse();

    return response is Future
        ? response.then((response) => _checkLast(response))
        : _checkLast(response);
  }

  String get name => _protocol._reusableColumnPacket.orgName;

  bool _checkLast(Packet response) {
    if (response is ResultSetColumnDefinitionResponsePacket) {
      return true;
    } else {
      _protocol._reusableColumnPacket.free();

      return false;
    }
  }
}

class QueryRowIterator extends PacketIterator {
  final QueryCommandTextProtocol _protocol;

  QueryRowIterator(this._protocol);

  Future<bool> next() {
    var value = internalNext();
    return value is Future ? value : new Future.value(value);
  }

  internalNext() {
    // TODO check dello stato

    var response = _protocol._readResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkLast(response))
        : _checkLast(response);
  }

  String getString(int index) => _protocol._reusableRowPacket.getString(index);

  String getUTF8String(int index) =>
      _protocol._reusableRowPacket.getUTF8String(index);

  bool _checkLast(Packet response) {
    if (response is ResultSetRowResponsePacket) {
      return true;
    } else {
      _protocol._reusableRowPacket.free();

      return false;
    }
  }
}

class ResultSetColumnCountResponsePacket extends Packet {
  int _columnCount;

  ResultSetColumnCountResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);

  int get columnCount => _columnCount;
}

class ResultSetColumnDefinitionResponsePacket extends ReusablePacket {
  ResultSetColumnDefinitionResponsePacket.reusable() : super.reusable(13);

  ResultSetColumnDefinitionResponsePacket reuse(
          int payloadLength, int sequenceId) =>
      _reuse(payloadLength, sequenceId);

  String get catalog => _getString(0);
  String get schema => _getString(1);
  String get table => _getString(2);
  String get orgTable => _getString(3);
  String get name => _getString(4);
  String get orgName => _getString(5);
  int get fieldsLength => _getInt(6);
  int get characterSet => _getInt(7);
  int get columnLength => _getInt(8);
  int get type => _getInt(9);
  int get flags => _getInt(10);
  int get decimals => _getInt(11);
}

class ResultSetRowResponsePacket extends ReusablePacket {
  ResultSetRowResponsePacket.reusable(int columnCount)
      : super.reusable(columnCount);

  ResultSetRowResponsePacket reuse(int payloadLength, int sequenceId) =>
      _reuse(payloadLength, sequenceId);

  String getString(int index) => _getString(index);

  String getUTF8String(int index) => _getUTF8String(index);
}
