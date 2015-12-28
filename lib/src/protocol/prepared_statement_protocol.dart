part of mysql_client.protocol;

class PrepareStatementError extends Error {
  final String message;

  PrepareStatementError(this.message);

  String toString() => "PrepareStatementError: $message";
}

class PreparedStatementProtocol extends ProtocolDelegate {
  PreparedStatementProtocol(Protocol protocol) : super(protocol);

  Future<PreparedStatement> prepareQuery(String query) async {
    _writeCommandStatementPreparePacket(query);

    var response = await _readCommandStatementPrepareResponse();

    if (response is! CommandStatementPrepareOkResponsePacket) {
      throw new PrepareStatementError(response.errorMessage);
    }

    return new PreparedStatement(response.statementId, response.numParams,
        response.numColumns, _protocol);
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

  _skipResultSetColumnDefinitionResponse() => _protocol
      ._queryCommandTextProtocol._skipResultSetColumnDefinitionResponse();

  _readResultSetColumnDefinitionResponse() => _protocol
      ._queryCommandTextProtocol._readResultSetColumnDefinitionResponse();

  Packet _readCommandStatementPrepareResponsePacket() {
    if (_protocol._isErrorPacket()) {
      return _protocol._readErrorPacket();
    } else {
      return _readCommandStatementPrepareOkResponsePacket();
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
}

class PreparedStatement implements ProtocolResult {
  final Protocol _protocol;

  final int _statementId;
  final int parameterCount;
  final int columnCount;

  final QueryColumnIterator _parameterIterator;
  final QueryColumnIterator _columnIterator;

  PreparedStatement(
      this._statementId, int parameterCount, int columnCount, Protocol protocol)
      : this.parameterCount = parameterCount,
        this.columnCount = columnCount,
        this._protocol = protocol,
        this._parameterIterator =
            new QueryColumnIterator(parameterCount, protocol),
        this._columnIterator = new QueryColumnIterator(columnCount, protocol);

  bool get isClosed {
    // TODO implementare PreparedStatement.isClosed
    return false;
  }

  Future<QueryColumnIterator> parameterIterator() async {
    // TODO check dello stato

    return _parameterIterator;
  }

  Future<QueryColumnIterator> columnIterator() async {
    // TODO check dello stato

    return _columnIterator;
  }

  @override
  Future close() async {
    // TODO implementare PreparedStatement.close

    _protocol._preparedStatementProtocol
        ._writeCommandStatementClosePacket(_statementId);
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
