part of mysql_client.protocol;

const int COM_QUERY = 0x03;

class QueryError extends Error {
  final String message;

  QueryError(this.message);

  String toString() => "QueryError: $message";
}

class QueryCommandTextProtocol extends ProtocolDelegate {
  final ResultSetColumnDefinitionPacket _reusableColumnPacket =
      new ResultSetColumnDefinitionPacket.reusable();

  ResultSetRowPacket _reusableRowPacket;

  QueryCommandTextProtocol(Protocol protocol) : super(protocol);

  Future<QueryResult> executeQuery(String query) async {
    _writeCommandQueryPacket(query);

    var response = await _readCommandQueryResponse();

    if (response is OkPacket) {
      return new QueryResult.ok(response.affectedRows);
    }

    if (response is! ResultSetColumnCountPacket) {
      throw new QueryError(response.errorMessage);
    }

    return new QueryResult.resultSet(response.columnCount, _protocol);
  }

  void _writeCommandQueryPacket(String query) {
    WriterBuffer buffer = _protocol._createBuffer();

    var sequenceId = 0x00;

    // 1              [03] COM_QUERY
    buffer.writeFixedLengthInteger(COM_QUERY, 1);
    // string[EOF]    the query the server shall execute
    buffer.writeFixedLengthUTF8String(query);

    var headerBuffer = _protocol._createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _protocol._writeBuffer(headerBuffer);
    _protocol._writeBuffer(buffer);
  }

  Future<Packet> _readCommandQueryResponse() {
    var value = _protocol._readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandQueryResponsePacket())
        : _readCommandQueryResponsePacket();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  _skipResultSetColumnDefinitionResponse() {
    var value = _protocol._readPacketBuffer();
    return value is Future
        ? value.then((_) => _skipResultSetColumnDefinitionResponsePacket())
        : _skipResultSetColumnDefinitionResponsePacket();
  }

  _readResultSetColumnDefinitionResponse() {
    var value = _protocol._readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetColumnDefinitionResponsePacket())
        : _readResultSetColumnDefinitionResponsePacket();
  }

  _skipResultSetRowResponse() {
    var value = _protocol._readPacketBuffer();
    return value is Future
        ? value.then((_) => _skipResultSetRowResponsePacket())
        : _skipResultSetRowResponsePacket();
  }

  _readResultSetRowResponse() {
    var value = _protocol._readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetRowResponsePacket())
        : _readResultSetRowResponsePacket();
  }

  Packet _readCommandQueryResponsePacket() {
    if (_protocol._isOkPacket()) {
      return _protocol._readOkPacket();
    } else if (_protocol._isErrorPacket()) {
      return _protocol._readErrorPacket();
    } else if (_isLocalInFilePacket()) {
      throw new UnsupportedError("Protocol::LOCAL_INFILE_Data");
    } else {
      return _readResultSetColumnCountPacket();
    }
  }

  Packet _skipResultSetColumnDefinitionResponsePacket() {
    if (_protocol._isErrorPacket()) {
      _reusableColumnPacket.free();

      return _protocol._readErrorPacket();
    } else if (_protocol._isEOFPacket()) {
      _reusableColumnPacket.free();

      return _protocol._readEOFPacket();
    } else {
      return _skipResultSetColumnDefinitionPacket();
    }
  }

  Packet _readResultSetColumnDefinitionResponsePacket() {
    if (_protocol._isErrorPacket()) {
      _reusableColumnPacket.free();

      return _protocol._readErrorPacket();
    } else if (_protocol._isEOFPacket()) {
      _reusableColumnPacket.free();

      return _protocol._readEOFPacket();
    } else {
      return _readResultSetColumnDefinitionPacket();
    }
  }

  Packet _skipResultSetRowResponsePacket() {
    if (_protocol._isErrorPacket()) {
      _reusableRowPacket.free();

      return _protocol._readErrorPacket();
    } else if (_protocol._isEOFPacket()) {
      _reusableRowPacket.free();

      return _protocol._readEOFPacket();
    } else {
      return _skipResultSetRowPacket();
    }
  }

  Packet _readResultSetRowResponsePacket() {
    if (_protocol._isErrorPacket()) {
      _reusableRowPacket.free();

      return _protocol._readErrorPacket();
    } else if (_protocol._isEOFPacket()) {
      _reusableRowPacket.free();

      return _protocol._readEOFPacket();
    } else {
      return _readResultSetRowPacket();
    }
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

    _reusableRowPacket = new ResultSetRowPacket.reusable(packet._columnCount);

    return packet;
  }

  ResultSetColumnDefinitionPacket _skipResultSetColumnDefinitionPacket() {
    var packet = _reusableColumnPacket.reuse(
        _protocol._reusablePacketBuffer.sequenceId,
        _protocol._reusablePacketBuffer.payloadLength);

    _protocol._reusablePacketBuffer.payload
        .skipBytes(_protocol._reusablePacketBuffer.payloadLength);

    _protocol._reusablePacketBuffer.free();

    return packet;
  }

  ResultSetColumnDefinitionPacket _readResultSetColumnDefinitionPacket() {
    var packet = _reusableColumnPacket.reuse(
        _protocol._reusablePacketBuffer.sequenceId,
        _protocol._reusablePacketBuffer.payloadLength);

    var dataRange;
    var i = 0;
    // lenenc_str     catalog
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload.readFixedLengthDataRange(
        _protocol._reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     schema
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload.readFixedLengthDataRange(
        _protocol._reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     table
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload.readFixedLengthDataRange(
        _protocol._reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     org_table
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload.readFixedLengthDataRange(
        _protocol._reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     name
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload.readFixedLengthDataRange(
        _protocol._reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     org_name
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload.readFixedLengthDataRange(
        _protocol._reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_int     length of fixed-length fields [0c]
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload
        .readLengthEncodedDataRange(dataRange);
    // 2              character set
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, dataRange);
    // 4              column length
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(4, dataRange);
    // 1              type
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, dataRange);
    // 2              flags
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, dataRange);
    // 1              decimals
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, dataRange);
    // 2              filler [00] [00]
    dataRange = _reusableColumnPacket.getReusableRange(i++);
    _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, dataRange);

    _protocol._reusablePacketBuffer.free();
    _protocol._reusableDataRange.free();

    return packet;
  }

  ResultSetRowPacket _skipResultSetRowPacket() {
    var packet = _reusableRowPacket.reuse(
        _protocol._reusablePacketBuffer.sequenceId,
        _protocol._reusablePacketBuffer.payloadLength);

    _protocol._reusablePacketBuffer.payload
        .skipBytes(_protocol._reusablePacketBuffer.payloadLength);

    _protocol._reusablePacketBuffer.free();

    return packet;
  }

  ResultSetRowPacket _readResultSetRowPacket() {
    var packet = _reusableRowPacket.reuse(
        _protocol._reusablePacketBuffer.sequenceId,
        _protocol._reusablePacketBuffer.payloadLength);

    var i = 0;
    while (!_protocol._reusablePacketBuffer.payload.isAllRead) {
      var reusableRange = _reusableRowPacket.getReusableRange(i++);
      if (_protocol._reusablePacketBuffer.payload.checkOneLengthInteger() !=
          PREFIX_NULL) {
        _protocol._reusablePacketBuffer.payload.readFixedLengthDataRange(
            _protocol._reusablePacketBuffer.payload
                .readLengthEncodedDataRange(reusableRange)
                .toInt(),
            reusableRange);
      } else {
        _protocol._reusablePacketBuffer.payload.skipByte();
        reusableRange.reuseNil();
      }
    }

    _protocol._reusablePacketBuffer.free();
    _protocol._reusableDataRange.free();

    return packet;
  }

  bool _isLocalInFilePacket() => _protocol._reusablePacketBuffer.header == 0xfb;
}

class QueryResult implements ProtocolResult {
  final Protocol _protocol;

  final int affectedRows;

  final int columnCount;

  final QueryColumnIterator _columnIterator;

  final QueryRowIterator _rowIterator;

  QueryResult.resultSet(int columnCount, Protocol protocol)
      : this.columnCount = columnCount,
        this.affectedRows = null,
        this._protocol = protocol,
        this._columnIterator = new QueryColumnIterator(columnCount, protocol),
        this._rowIterator = new QueryRowIterator(protocol);

  QueryResult.ok(this.affectedRows)
      : this.columnCount = null,
        this._columnIterator = null,
        this._rowIterator = null,
        this._protocol = null;

  bool get isClosed => _rowIterator == null || _rowIterator.isClosed;

  Future<QueryColumnIterator> columnIterator() async {
    if (isClosed) {
      throw new StateError("Query result closed");
    } else if (_columnIterator.isClosed) {
      throw new StateError("Column iterator closed");
    }

    return _columnIterator;
  }

  Future<QueryRowIterator> rowIterator() async {
    if (isClosed) {
      throw new StateError("Query result closed");
    }

    if (!_columnIterator.isClosed) {
      await _columnIterator.close();
    }

    return _rowIterator;
  }

  @override
  Future free() async {
    await close();
  }

  @override
  Future close() async {
    if (_columnIterator != null && !_columnIterator.isClosed) {
      await _columnIterator.close();
    }
    if (_rowIterator != null && !_rowIterator.isClosed) {
      await _rowIterator.close();
    }
  }
}

class QueryColumnIterator extends PacketIterator {
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

  Future<bool> nextAsFuture() {
    var value = next();
    return value is Future ? value : new Future.value(value);
  }

  next() {
    if (isClosed) {
      throw new StateError("Column iterator closed");
    }

    if (columnCount > 0) {
      var response = _protocol._queryCommandTextProtocol
          ._readResultSetColumnDefinitionResponse();

      return response is Future
          ? response.then((response) => _checkLast(response))
          : _checkLast(response);
    } else {
      return false;
    }
  }

  String get catalog =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.catalog;
  String get schema =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.schema;
  String get table =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.table;
  String get orgTable =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.orgTable;
  String get name =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.name;
  String get orgName =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.orgName;
  int get fieldsLength =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.fieldsLength;
  int get characterSet =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.characterSet;
  int get columnLength =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.columnLength;
  int get type =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.type;
  int get flags =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.flags;
  int get decimals =>
      _protocol._queryCommandTextProtocol._reusableColumnPacket.decimals;

  _skip() {
    var response = _protocol._queryCommandTextProtocol
        ._skipResultSetColumnDefinitionResponse();

    return response is Future
        ? response.then((response) => _checkLast(response))
        : _checkLast(response);
  }

  bool _checkLast(Packet response) {
    _isClosed = response is! ResultSetColumnDefinitionPacket;
    return !_isClosed;
  }
}

class QueryRowIterator extends PacketIterator {
  final Protocol _protocol;

  bool _isClosed;

  QueryRowIterator(this._protocol) {
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

    var response =
        _protocol._queryCommandTextProtocol._readResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkLast(response))
        : _checkLast(response);
  }

  String getString(int index) =>
      _protocol._queryCommandTextProtocol._reusableRowPacket._getString(index);

  String getUTF8String(int index) => _protocol
      ._queryCommandTextProtocol._reusableRowPacket._getUTF8String(index);

  _skip() {
    var response =
        _protocol._queryCommandTextProtocol._skipResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkLast(response))
        : _checkLast(response);
  }

  bool _checkLast(Packet response) {
    _isClosed = response is! ResultSetRowPacket;
    return !_isClosed;
  }
}

class ResultSetColumnCountPacket extends Packet {
  int _columnCount;

  ResultSetColumnCountPacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);

  int get columnCount => _columnCount;
}

class ResultSetColumnDefinitionPacket extends ReusablePacket {
  ResultSetColumnDefinitionPacket.reusable() : super.reusable(13);

  ResultSetColumnDefinitionPacket reuse(int payloadLength, int sequenceId) =>
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

class ResultSetRowPacket extends ReusablePacket {
  ResultSetRowPacket.reusable(int columnCount) : super.reusable(columnCount);

  ResultSetRowPacket reuse(int payloadLength, int sequenceId) =>
      _reuse(payloadLength, sequenceId);
}
