part of mysql_client.protocol;

const int CLIENT_PLUGIN_AUTH = 0x00080000;
const int CLIENT_SECURE_CONNECTION = 0x00008000;
const int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
const int CLIENT_CONNECT_WITH_DB = 0x00000008;
const int CLIENT_CONNECT_ATTRS = 0x00100000;

const int CLIENT_PROTOCOL_41 = 0x00000200;
const int CLIENT_TRANSACTIONS = 0x00002000;
const int CLIENT_SESSION_TRACK = 0x00800000;

const int SERVER_SESSION_STATE_CHANGED = 0x4000;

const int COM_QUERY = 0x03;
const int COM_STMT_PREPARE = 0x16;
const int COM_STMT_CLOSE = 0x19;

abstract class Protocol {
  final PacketBuffer _reusablePacketBuffer = new PacketBuffer.reusable();

  final DataRange _reusableDataRange = new DataRange.reusable();

  DataWriter _writer;

  DataReader _reader;

  int _serverCapabilityFlags;

  int _clientCapabilityFlags;

  Protocol(this._writer, this._reader, this._serverCapabilityFlags,
      this._clientCapabilityFlags);

  Protocol.reusable(DataWriter writer, DataReader reader,
      int serverCapabilityFlags, int clientCapabilityFlags) {
    _writer = writer;
    _reader = reader;
    _serverCapabilityFlags = serverCapabilityFlags;
    _clientCapabilityFlags = clientCapabilityFlags;
  }

  Protocol reuse() {
    return this;
  }

  void free() {
    _reusablePacketBuffer.free();
    _reusableDataRange.free();
  }

  _readPacketBuffer() {
    var value = _reader.readBuffer(4);
    return value is Future
        ? value.then((headerReaderBuffer) =>
            _readPacketBufferPayload(headerReaderBuffer))
        : _readPacketBufferPayload(value);
  }

  _readPacketBufferPayload(ReaderBuffer headerReaderBuffer) {
    var payloadLength = headerReaderBuffer
        .readFixedLengthDataRange(3, _reusableDataRange)
        .toInt();
    var sequenceId = headerReaderBuffer.readOneLengthInteger();

    _reusableDataRange.free();

    var value = _reader.readBuffer(payloadLength);
    if (value is Future) {
      return value.then((payloadReaderBuffer) =>
          _reusablePacketBuffer.reuse(sequenceId, payloadReaderBuffer));
    } else {
      return _reusablePacketBuffer.reuse(sequenceId, value);
    }
  }

  Future<Packet> _readCommandResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readCommandResponseInternal())
        : _readCommandResponseInternal();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  bool _isOkPacket() => _reusablePacketBuffer.header == 0 &&
      _reusablePacketBuffer.payloadLength >= 7;

  bool _isEOFPacket() => _reusablePacketBuffer.header == 0xfe &&
      _reusablePacketBuffer.payloadLength < 9;

  bool _isErrorPacket() => _reusablePacketBuffer.header == 0xff;

  bool _isLocalInFilePacket() => _reusablePacketBuffer.header == 0xfb;

  Packet _readCommandResponseInternal() {
    if (_isOkPacket()) {
      return _readOkPacket();
    } else if (_isErrorPacket()) {
      return _readErrorPacket();
    } else {
      throw new UnsupportedError("header: ${_reusablePacketBuffer.header}");
    }
  }

  Packet _readEOFResponseInternal() {
    if (_isEOFPacket()) {
      return _readEOFPacket();
    } else if (_isErrorPacket()) {
      return _readErrorPacket();
    } else {
      throw new UnsupportedError("header: ${_reusablePacketBuffer.header}");
    }
  }

  OkPacket _readOkPacket() {
    var packet = new OkPacket(
        _reusablePacketBuffer.sequenceId, _reusablePacketBuffer.payloadLength);

    _completeSuccessResponsePacket(packet);

    _reusablePacketBuffer.free();
    _reusableDataRange.free();

    return packet;
  }

  EOFPacket _readEOFPacket() {
    var packet = new EOFPacket(
        _reusablePacketBuffer.sequenceId, _reusablePacketBuffer.payloadLength);

    // TODO check CLIENT_DEPRECATE_EOF flag
    bool isEOFDeprecated = false;

    if (isEOFDeprecated) {
      _completeSuccessResponsePacket(packet);
    } else {
      // EOF packet
      // int<1>	header	[00] or [fe] the OK packet header
      packet._header = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(1, _reusableDataRange)
          .toInt();
      // if capabilities & CLIENT_PROTOCOL_41 {
      if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
        // int<2>	warnings	number of warnings
        packet._warnings = _reusablePacketBuffer.payload
            .readFixedLengthDataRange(2, _reusableDataRange)
            .toInt();
        // int<2>	status_flags	Status Flags
        packet._statusFlags = _reusablePacketBuffer.payload
            .readFixedLengthDataRange(2, _reusableDataRange)
            .toInt();
      }
    }

    _reusablePacketBuffer.free();
    _reusableDataRange.free();

    return packet;
  }

  ErrorPacket _readErrorPacket() {
    var packet = new ErrorPacket(
        _reusablePacketBuffer.sequenceId, _reusablePacketBuffer.payloadLength);
    // int<1>	header	[00] or [fe] the OK packet header
    packet._header = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _reusableDataRange)
        .toInt();
    // int<2>	error_code	error-code
    packet._errorCode = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _reusableDataRange)
        .toInt();
    // if capabilities & CLIENT_PROTOCOL_41 {
    if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // string[1]	sql_state_marker	# marker of the SQL State
      packet._sqlStateMarker = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(1, _reusableDataRange)
          .toString();
      // string[5]	sql_state	SQL State
      packet._sqlState = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(5, _reusableDataRange)
          .toString();
    }
    // string<EOF>	error_message	human readable error message
    packet._errorMessage = _reusablePacketBuffer.payload
        .readRestOfPacketDataRange(_reusableDataRange)
        .toString();

    _reusablePacketBuffer.free();
    _reusableDataRange.free();

    return packet;
  }

  void _completeSuccessResponsePacket(SuccessResponsePacket packet) {
    // int<1>	header	[00] or [fe] the OK packet header
    packet._header = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _reusableDataRange)
        .toInt();
    // int<lenenc>	affected_rows	affected rows
    packet._affectedRows = _reusablePacketBuffer.payload
        .readLengthEncodedDataRange(_reusableDataRange)
        .toInt();
    // int<lenenc>	last_insert_id	last insert-id
    packet._lastInsertId = _reusablePacketBuffer.payload
        .readLengthEncodedDataRange(_reusableDataRange)
        .toInt();

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	status_flags	Status Flags
      packet._statusFlags = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _reusableDataRange)
          .toInt();
      // int<2>	warnings	number of warnings
      packet._warnings = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _reusableDataRange)
          .toInt();
      // } elseif capabilities & CLIENT_TRANSACTIONS {
    } else if (_serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
      // int<2>	status_flags	Status Flags
      packet._statusFlags = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _reusableDataRange)
          .toInt();
    } else {
      packet._statusFlags = 0;
    }

    // if capabilities & CLIENT_SESSION_TRACK {
    if (_serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
      // string<lenenc>	info	human readable status information
      if (!_reusablePacketBuffer.payload.isAllRead) {
        packet._info = _reusablePacketBuffer.payload
            .readFixedLengthDataRange(
                _reusablePacketBuffer.payload
                    .readLengthEncodedDataRange(_reusableDataRange)
                    .toInt(),
                _reusableDataRange)
            .toString();
      }

      // if status_flags & SERVER_SESSION_STATE_CHANGED {
      if (packet.statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
        // string<lenenc>	session_state_changes	session state info
        if (!_reusablePacketBuffer.payload.isAllRead) {
          packet._sessionStateChanges = _reusablePacketBuffer.payload
              .readFixedLengthDataRange(
                  _reusablePacketBuffer.payload
                      .readLengthEncodedDataRange(_reusableDataRange)
                      .toInt(),
                  _reusableDataRange)
              .toString();
        }
      }
      // } else {
    } else {
      // string<EOF>	info	human readable status information
      packet._info = _reusablePacketBuffer.payload
          .readRestOfPacketDataRange(_reusableDataRange)
          .toString();
    }
  }
}

abstract class Packet {
  int _payloadLength;
  int _sequenceId;

  Packet(this._payloadLength, this._sequenceId);

  int get payloadLength => _payloadLength;
  int get sequenceId => _sequenceId;
}

class ReusablePacket extends Packet {
  final List<DataRange> _dataRanges;

  ReusablePacket.reusable(int rangeCount)
      : _dataRanges = new List<DataRange>.generate(
            rangeCount, (_) => new DataRange.reusable(),
            growable: false),
        super(null, null);

  ReusablePacket _reuse(int payloadLength, int sequenceId) {
    _payloadLength = payloadLength;
    _sequenceId = sequenceId;
    return this;
  }

  DataRange getReusableRange(int i) => _dataRanges[i];

  void free() {
    for (var range in _dataRanges) {
      range?.free();
    }
  }

  int _getInt(int index) => _dataRanges[index].toInt();

  String _getString(int index) => _dataRanges[index].toString();

  String _getUTF8String(int index) => _dataRanges[index].toUTF8String();
}

abstract class GenericResponsePacket extends Packet {
  int _header;
  String _info;
  String _sessionStateChanges;

  GenericResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);

  int get header => _header;
  String get info => _info;
  String get sessionStateChanges => _sessionStateChanges;
}

abstract class SuccessResponsePacket extends GenericResponsePacket {
  int _affectedRows;
  int _lastInsertId;
  int _statusFlags;
  int _warnings;

  SuccessResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);

  int get affectedRows => _affectedRows;
  int get lastInsertId => _lastInsertId;
  int get statusFlags => _statusFlags;
  int get warnings => _warnings;
}

class OkPacket extends SuccessResponsePacket {
  OkPacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class EOFPacket extends SuccessResponsePacket {
  EOFPacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class ErrorPacket extends GenericResponsePacket {
  int _errorCode;
  String _sqlStateMarker;
  String _sqlState;
  String _errorMessage;

  ErrorPacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);

  int get errorCode => _errorCode;
  String get sqlStateMarker => _sqlStateMarker;
  String get sqlState => _sqlState;
  String get errorMessage => _errorMessage;
}

abstract class PacketIterator {
  Future<bool> next();

  internalNext();
}
