part of mysql_client.protocol;

class QueryError extends Error {
  final String message;

  QueryError(this.message);

  String toString() => "QueryError: $message";
}

class QueryCommandTextProtocol extends Protocol {
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

  _readResultSetColumnDefinitionResponse(
      ResultSetColumnDefinitionResponsePacket reusablePacket) {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) =>
            _readResultSetColumnDefinitionResponseInternal(reusablePacket))
        : _readResultSetColumnDefinitionResponseInternal(reusablePacket);
  }

  _readResultSetRowResponse(ResultSetRowResponsePacket reusablePacket) {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetRowResponseInternal(reusablePacket))
        : _readResultSetRowResponseInternal(reusablePacket);
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

  Packet _readResultSetColumnDefinitionResponseInternal(
      ResultSetColumnDefinitionResponsePacket reusablePacket) {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _readResultSetColumnDefinitionResponsePacket(reusablePacket);
    }
  }

  Packet _readResultSetRowResponseInternal(
      ResultSetRowResponsePacket reusablePacket) {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _readResultSetRowResponsePacket(reusablePacket);
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

    return packet;
  }

  ResultSetColumnDefinitionResponsePacket _readResultSetColumnDefinitionResponsePacket(
      ResultSetColumnDefinitionResponsePacket reusablePacket) {
    var packet = reusablePacket.reuse(
        _reusablePacketBuffer.sequenceId, _reusablePacketBuffer.payloadLength);

    var dataRange;
    var i = 0;
    // lenenc_str     catalog
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     schema
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     table
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     org_table
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     name
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     org_name
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_int     length of fixed-length fields [0c]
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readLengthEncodedDataRange(dataRange);
    // 2              character set
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(2, dataRange);
    // 4              column length
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(4, dataRange);
    // 1              type
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(1, dataRange);
    // 2              flags
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(2, dataRange);
    // 1              decimals
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(1, dataRange);
    // 2              filler [00] [00]
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(2, dataRange);

    _reusablePacketBuffer.free();
    _reusableDataRange.free();

    return packet;
  }

  ResultSetRowResponsePacket _readResultSetRowResponsePacket(
      ResultSetRowResponsePacket reusablePacket) {
    var packet = reusablePacket.reuse(
        _reusablePacketBuffer.sequenceId, _reusablePacketBuffer.payloadLength);

    var i = 0;
    while (!_reusablePacketBuffer.payload.isAllRead) {
      var reusableRange = reusablePacket.getReusableRange(i++);
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

  QueryColumnSetReader _columnSetReader;

  QueryRowSetReader _rowSetReader;

  QueryResult.resultSet(this.columnCount, this._protocol)
      : this.affectedRows = 0;

  QueryResult.ok(this.affectedRows)
      : this.columnCount = 0,
        this._protocol = null;

  QueryColumnSetReader get columnSetReader {
    // TODO check dello stato

    _columnSetReader = new QueryColumnSetReader(columnCount, _protocol);

    return _columnSetReader;
  }

  QueryRowSetReader get rowSetReader {
    // TODO check dello stato

    _rowSetReader = new QueryRowSetReader(columnCount, _protocol);

    return _rowSetReader;
  }

  void close() {
    if (_columnSetReader != null) {
      _columnSetReader.close();
    }
    if (_rowSetReader != null) {
      _rowSetReader.close();
    }
  }
}

class QueryColumnSetReader extends SetReader {
  final int _columnCount;

  final QueryCommandTextProtocol _protocol;

  final ResultSetColumnDefinitionResponsePacket _reusableColumnPacket;

  QueryColumnSetReader(this._columnCount, this._protocol)
      : this._reusableColumnPacket =
            new ResultSetColumnDefinitionResponsePacket.reusable();

  Future<bool> next() {
    var value = internalNext();
    return value is Future ? value : new Future.value(value);
  }

  internalNext() {
    // TODO check dello stato

    var response =
        _protocol._readResultSetColumnDefinitionResponse(_reusableColumnPacket);

    return response is Future
        ? response.then(
            (response) => response is ResultSetColumnDefinitionResponsePacket)
        : response is ResultSetColumnDefinitionResponsePacket;
  }

  String get name => _reusableColumnPacket.orgName;

  void close() {
    // TODO check dello stato

    _reusableColumnPacket.free();
  }
}

class QueryRowSetReader extends SetReader {
  final int _columnCount;

  final QueryCommandTextProtocol _protocol;

  final ResultSetRowResponsePacket _reusableRowPacket;

  QueryRowSetReader(int columnCount, this._protocol)
      : this._columnCount = columnCount,
        this._reusableRowPacket =
            new ResultSetRowResponsePacket.reusable(columnCount);

  Future<bool> next() {
    var value = internalNext();
    return value is Future ? value : new Future.value(value);
  }

  internalNext() {
    // TODO check dello stato

    var response = _protocol._readResultSetRowResponse(_reusableRowPacket);

    return response is Future
        ? response.then((response) => response is ResultSetRowResponsePacket)
        : response is ResultSetRowResponsePacket;
  }

  String getString(int index) => _reusableRowPacket.getString(index);

  String getUTF8String(int index) => _reusableRowPacket.getUTF8String(index);

  void close() {
    // TODO check dello stato

    _reusableRowPacket.free();
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
