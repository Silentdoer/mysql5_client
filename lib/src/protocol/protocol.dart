part of mysql_client.protocol;

const int CLIENT_CONNECT_WITH_DB = 0x00000008;
const int CLIENT_PROTOCOL_41 = 0x00000200;
const int CLIENT_TRANSACTIONS = 0x00002000;
const int CLIENT_SECURE_CONNECTION = 0x00008000;
const int CLIENT_PLUGIN_AUTH = 0x00080000;
const int CLIENT_CONNECT_ATTRS = 0x00100000;
const int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
const int CLIENT_SESSION_TRACK = 0x00800000;

const int SERVER_SESSION_STATE_CHANGED = 0x4000;

class NullError extends Error {
  String toString() => "Null value";
}

class UndefinedError extends Error {
  String toString() => "Undefined value";
}

class EOFError extends Error {
  String toString() => "EOF value";
}

abstract class ProtocolDelegate {
  Protocol _protocol;

  ProtocolDelegate(this._protocol);

  void freeReusables() {
    _protocol._freeReusables();
  }

  int get _clientCapabilityFlags => _protocol._clientCapabilityFlags;

  void set _clientCapabilityFlags(int clientCapabilityFlags) {
    _protocol._clientCapabilityFlags = clientCapabilityFlags;
  }

  int get _serverCapabilityFlags => _protocol._serverCapabilityFlags;

  void set _serverCapabilityFlags(int serverCapabilityFlags) {
    _protocol._serverCapabilityFlags = serverCapabilityFlags;
  }

  int get _sequenceId => _protocol._sequenceId;

  int get _payloadLength => _protocol._payloadLength;

  bool get _isAllRead => _protocol._isAllRead;

  int get _header => _protocol._header;

  void _skipByte() {
    _protocol._skipByte();
  }

  void _skipBytes(int length) {
    _protocol._skipBytes(length);
  }

  int _checkByte() => _protocol._checkByte();

  int _readByte() => _protocol._readByte();

  int _readFixedLengthInteger(int length) =>
      _protocol._readFixedLengthInteger(length);

  int _readLengthEncodedInteger() => _protocol._readLengthEncodedInteger();

  String _readFixedLengthString(int length) =>
      _protocol._readFixedLengthString(length);

  String _readFixedLengthUTF8String(int length) =>
      _protocol._readFixedLengthUTF8String(length);

  String _readLengthEncodedString() => _protocol._readLengthEncodedString();

  String _readLengthEncodedUTF8String() =>
      _protocol._readLengthEncodedUTF8String();

  String _readNulTerminatedString() => _protocol._readNulTerminatedString();

  String _readNulTerminatedUTF8String() =>
      _protocol._readNulTerminatedUTF8String();

  String _readRestOfPacketString() => _protocol._readRestOfPacketString();

  String _readRestOfPacketUTF8String() =>
      _protocol._readRestOfPacketUTF8String();

  DataRange _readFixedLengthDataRange(int length, DataRange reusable) =>
      _protocol._readFixedLengthDataRange(length, reusable);

  DataRange _readLengthEncodedDataRange(DataRange reusable) =>
      _protocol._readLengthEncodedDataRange(reusable);

  _readPacketBuffer() => _protocol._readPacketBuffer();

  bool _isOkPacket() => _protocol._isOkPacket();

  bool _isEOFPacket() => _protocol._isEOFPacket();

  bool _isErrorPacket() => _protocol._isErrorPacket();

  OkPacket _readOkPacket() => _protocol._readOkPacket();

  EOFPacket _readEOFPacket() => _protocol._readEOFPacket();

  ErrorPacket _readErrorPacket() => _protocol._readErrorPacket();

  WriterBuffer _createBuffer() => _protocol._createBuffer();

  void _writeBuffer(WriterBuffer buffer) {
    _protocol._writeBuffer(buffer);
  }
}

class Protocol {
  int _serverCapabilityFlags;
  int _clientCapabilityFlags;

  final PacketBuffer __reusablePacketBuffer = new PacketBuffer.reusable();
  final DataRange __reusableDataRange = new DataRange.reusable();

  DataWriter __writer;
  DataReader __reader;

  ConnectionProtocol __connectionProtocol;
  QueryCommandTextProtocol __queryCommandTextProtocol;
  PreparedStatementProtocol __preparedStatementProtocol;

  Protocol(Socket socket) {
    __reader = new DataReader(socket);
    __writer = new DataWriter(socket);

    __connectionProtocol = new ConnectionProtocol(this);
    __queryCommandTextProtocol = new QueryCommandTextProtocol(this);
    __preparedStatementProtocol = new PreparedStatementProtocol(this);
  }

  ConnectionProtocol get connectionProtocol => __connectionProtocol;

  QueryCommandTextProtocol get queryCommandTextProtocol =>
      __queryCommandTextProtocol;

  PreparedStatementProtocol get preparedStatementProtocol =>
      __preparedStatementProtocol;

  Future<Packet> readCommandResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => __readCommandResponsePacket())
        : __readCommandResponsePacket();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  void _freeReusables() {
    __reusablePacketBuffer.free();
    __reusableDataRange.free();
  }

  int get _sequenceId => __reusablePacketBuffer.sequenceId;

  int get _payloadLength => __reusablePacketBuffer.payloadLength;

  bool get _isAllRead => __reusablePacketBuffer.payload.isNotDataLeft;

  int get _header => __reusablePacketBuffer.header;

  WriterBuffer _createBuffer() => __writer.createBuffer();

  void _writeBuffer(WriterBuffer buffer) => __writer.writeBuffer(buffer);

  _readPacketBuffer() {
    var value = __reader.readBuffer(4);
    return value is Future
        ? value.then((headerReaderBuffer) =>
            __readPacketBufferPayload(headerReaderBuffer))
        : __readPacketBufferPayload(value);
  }

  bool _isOkPacket() => _header == 0 && _payloadLength >= 7;

  bool _isEOFPacket() => _header == 0xfe && _payloadLength < 9;

  bool _isErrorPacket() => _header == 0xff;

  OkPacket _readOkPacket() => __completeSuccessResponsePacket(
      new OkPacket(_sequenceId, _payloadLength));

  EOFPacket _readEOFPacket() {
    // TODO check CLIENT_DEPRECATE_EOF flag
    bool isEOFDeprecated = false;

    if (isEOFDeprecated) {
      return __completeSuccessResponsePacket(
          new EOFPacket(_sequenceId, _payloadLength));
    } else {
      var packet = new EOFPacket(_sequenceId, _payloadLength);
      // EOF packet
      // int<1>	header	[00] or [fe] the OK packet header
      packet._header = _readByte();
      // if capabilities & CLIENT_PROTOCOL_41 {
      if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
        // int<2>	warnings	number of warnings
        packet._warnings = _readFixedLengthInteger(2);
        // int<2>	status_flags	Status Flags
        packet._statusFlags = _readFixedLengthInteger(2);
      }
      return packet;
    }
  }

  ErrorPacket _readErrorPacket() {
    var packet = new ErrorPacket(_sequenceId, _payloadLength);
    // int<1>	header	[00] or [fe] the OK packet header
    packet._header = _readByte();
    // int<2>	error_code	error-code
    packet._errorCode = _readFixedLengthInteger(2);
    // if capabilities & CLIENT_PROTOCOL_41 {
    if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // string[1]	sql_state_marker	# marker of the SQL State
      packet._sqlStateMarker = _readFixedLengthString(1);
      // string[5]	sql_state	SQL State
      packet._sqlState = _readFixedLengthString(5);
    }
    // string<EOF>	error_message	human readable error message
    packet._errorMessage = _readRestOfPacketString();

    return packet;
  }

  int _getInteger(DataRange range) {
    if (range.isByte) {
      return range.byteValue;
    }

    range.mergeExtraRanges();

    var i = range.start;
    switch (range.length) {
      case 1:
        return range.data[i++];
      case 2:
        return range.data[i++] | range.data[i++] << 8;
      case 3:
        return range.data[i++] | range.data[i++] << 8 | range.data[i++] << 16;
      case 4:
        return range.data[i++] |
            range.data[i++] << 8 |
            range.data[i++] << 16 |
            range.data[i++] << 24;
      case 5:
        return range.data[i++] |
            range.data[i++] << 8 |
            range.data[i++] << 16 |
            range.data[i++] << 24 |
            range.data[i++] << 32;
      case 6:
        return range.data[i++] |
            range.data[i++] << 8 |
            range.data[i++] << 16 |
            range.data[i++] << 24 |
            range.data[i++] << 32 |
            range.data[i++] << 40;
      case 7:
        return range.data[i++] |
            range.data[i++] << 8 |
            range.data[i++] << 16 |
            range.data[i++] << 24 |
            range.data[i++] << 32 |
            range.data[i++] << 40 |
            range.data[i++] << 48;
      case 8:
        return range.data[i++] |
            range.data[i++] << 8 |
            range.data[i++] << 16 |
            range.data[i++] << 24 |
            range.data[i++] << 32 |
            range.data[i++] << 40 |
            range.data[i++] << 48 |
            range.data[i++] << 56;
    }

    throw new UnsupportedError("${range.data.length} length");
  }

  // TODO uniformare con getInteger
  int _decodeFixedLengthInteger(List<int> data) {
    switch (data.length) {
      case 1:
        return data[0];
      case 2:
        return data[0] | data[1] << 8;
      case 3:
        return data[0] | data[1] << 8 | data[2] << 16;
      case 4:
        return data[0] | data[1] << 8 | data[2] << 16 | data[3] << 24;
      case 5:
        return data[0] |
            data[1] << 8 |
            data[2] << 16 |
            data[3] << 24 |
            data[4] << 32;
      case 6:
        return data[0] |
            data[1] << 8 |
            data[2] << 16 |
            data[3] << 24 |
            data[4] << 32 |
            data[5] << 40;
      case 7:
        return data[0] |
            data[1] << 8 |
            data[2] << 16 |
            data[3] << 24 |
            data[4] << 32 |
            data[5] << 40 |
            data[6] << 48;
      case 8:
        return data[0] |
            data[1] << 8 |
            data[2] << 16 |
            data[3] << 24 |
            data[4] << 32 |
            data[5] << 40 |
            data[6] << 48 |
            data[7] << 56;
    }

    throw new UnsupportedError("${data.length} length");
  }

  double _getDouble(DataRange range) {
    range.mergeExtraRanges();

    if (!range.isNil) {
      // TODO verificare se esistono conversioni più snelle
      return new Uint8List.fromList(range.data.sublist(range.start, range.end))
          .buffer
          .asFloat64List()[0];
    } else {
      return null;
    }
  }

  String _getString(DataRange range) {
    range.mergeExtraRanges();

    if (!range.isNil) {
      return new String.fromCharCodes(range.data, range.start, range.end);
    } else {
      return null;
    }
  }

  String _getUTF8String(DataRange range) {
    range.mergeExtraRanges();

    if (!range.isNil) {
      return UTF8.decoder.convert(range.data, range.start, range.end);
    } else {
      return null;
    }
  }

  int _checkByte() => __reusablePacketBuffer.payload.checkByte();

  void _skipByte() {
    __reusablePacketBuffer.payload.readByte();
  }

  void _skipBytes(int length) {
    __reusablePacketBuffer.payload
        .readFixedLengthDataRange(length, __reusableDataRange);
  }

  int _readByte() => __reusablePacketBuffer.payload.readByte();

  int _readFixedLengthInteger(int length) => _getInteger(__reusablePacketBuffer
      .payload.readFixedLengthDataRange(length, __reusableDataRange));

  int _readLengthEncodedInteger() =>
      _getInteger(_readLengthEncodedDataRange(__reusableDataRange));

  String _readFixedLengthString(int length) => _getString(__reusablePacketBuffer
      .payload.readFixedLengthDataRange(length, __reusableDataRange));

  String _readFixedLengthUTF8String(int length) =>
      _getUTF8String(__reusablePacketBuffer.payload
          .readFixedLengthDataRange(length, __reusableDataRange));

  String _readLengthEncodedString() =>
      _getString(_readLengthEncodedDataRange(__reusableDataRange));

  String _readLengthEncodedUTF8String() =>
      _getUTF8String(_readLengthEncodedDataRange(__reusableDataRange));

  String _readNulTerminatedString() => _getString(__reusablePacketBuffer.payload
      .readUpToDataRange(NULL_TERMINATOR, __reusableDataRange));

  String _readNulTerminatedUTF8String() => _getUTF8String(__reusablePacketBuffer
      .payload.readUpToDataRange(NULL_TERMINATOR, __reusableDataRange));

  String _readRestOfPacketString() =>
      _getString(_readRestOfPacketDataRange(__reusableDataRange));

  String _readRestOfPacketUTF8String() =>
      _getUTF8String(_readRestOfPacketDataRange(__reusableDataRange));

  DataRange _readFixedLengthDataRange(int length, DataRange reusable) =>
      __reusablePacketBuffer.payload.readFixedLengthDataRange(length, reusable);

  DataRange _readRestOfPacketDataRange(DataRange reusableRange) =>
      __reusablePacketBuffer.payload.readFixedLengthDataRange(
          __reusablePacketBuffer.payload.leftCount, reusableRange);

  DataRange _readLengthEncodedDataRange(DataRange reusableRange) {
    var firstByte = _readByte();
    var bytesLength;
    switch (firstByte) {
      case PREFIX_INT_2:
        bytesLength = 3;
        break;
      case PREFIX_INT_3:
        bytesLength = 4;
        break;
      case PREFIX_INT_8:
        if (__reusablePacketBuffer.payload.leftCount >= 8) {
          bytesLength = 9;
        } else {
          throw new EOFError();
        }
        break;
      case PREFIX_NULL:
        throw new NullError();
      case PREFIX_UNDEFINED:
        throw new UndefinedError();
      default:
        return reusableRange.reuseByte(firstByte);
    }
    return __reusablePacketBuffer.payload
        .readFixedLengthDataRange(bytesLength - 1, reusableRange);
  }

  __readPacketBufferPayload(ReaderBuffer headerReaderBuffer) {
    var payloadLength = _getInteger(
        headerReaderBuffer.readFixedLengthDataRange(3, __reusableDataRange));
    var sequenceId = headerReaderBuffer.readByte();

    var value = __reader.readBuffer(payloadLength);
    if (value is Future) {
      return value.then((payloadReaderBuffer) {
        var header = payloadReaderBuffer.checkByte();
        return __reusablePacketBuffer.reuse(
            sequenceId, header, payloadReaderBuffer);
      });
    } else {
      var header = value.checkByte();
      return __reusablePacketBuffer.reuse(sequenceId, header, value);
    }
  }

  Packet __readCommandResponsePacket() {
    if (_isOkPacket()) {
      return _readOkPacket();
    } else if (_isErrorPacket()) {
      return _readErrorPacket();
    } else {
      throw new UnsupportedError("header: ${_header}");
    }
  }

  SuccessResponsePacket __completeSuccessResponsePacket(
      SuccessResponsePacket packet) {
    // int<1>	header	[00] or [fe] the OK packet header
    packet._header = _readByte();
    // int<lenenc>	affected_rows	affected rows
    packet._affectedRows = _readLengthEncodedInteger();
    // int<lenenc>	last_insert_id	last insert-id
    packet._lastInsertId = _readLengthEncodedInteger();

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	status_flags	Status Flags
      packet._statusFlags = _readFixedLengthInteger(2);
      // int<2>	warnings	number of warnings
      packet._warnings = _readFixedLengthInteger(2);
      // } elseif capabilities & CLIENT_TRANSACTIONS {
    } else if (_serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
      // int<2>	status_flags	Status Flags
      packet._statusFlags = _readFixedLengthInteger(2);
    } else {
      packet._statusFlags = 0;
    }

    // if capabilities & CLIENT_SESSION_TRACK {
    if (_serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
      // string<lenenc>	info	human readable status information
      if (!_isAllRead) {
        packet._info = _readLengthEncodedString();
      }

      // if status_flags & SERVER_SESSION_STATE_CHANGED {
      if (packet.statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
        // string<lenenc>	session_state_changes	session state info
        if (!_isAllRead) {
          packet._sessionStateChanges = _readLengthEncodedString();
        }
      }
      // } else {
    } else {
      // string<EOF>	info	human readable status information
      packet._info = _readRestOfPacketString();
    }

    return packet;
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
  Protocol _protocol;

  final List<DataRange> _dataRanges;

  ReusablePacket.reusable(this._protocol, int rangeCount)
      : _dataRanges = new List<DataRange>.generate(
            rangeCount, (_) => new DataRange.reusable(),
            growable: false),
        super(null, null);

  ReusablePacket _reuse(int payloadLength, int sequenceId) {
    _payloadLength = payloadLength;
    _sequenceId = sequenceId;
    return this;
  }

  DataRange _getReusableDataRange(int index) => _dataRanges[index];

  void _free() {
    for (int i = 0; i < _dataRanges.length; i++) {
      _dataRanges[i].free();
    }
  }

  double getDouble(int index) => _protocol._getDouble(_dataRanges[index]);

  int getInteger(int index) => _protocol._getInteger(_dataRanges[index]);

  String getString(int index) => _protocol._getString(_dataRanges[index]);

  String getUTF8String(int index) =>
      _protocol._getUTF8String(_dataRanges[index]);
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
