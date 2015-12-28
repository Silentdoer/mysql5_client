part of mysql_client.protocol;

class ConnectionError extends Error {
  final String message;

  ConnectionError(this.message);

  String toString() => "ConnectionError: $message";
}

class ConnectionProtocol extends ProtocolDelegate {
  var _characterSet = 0x21; // corrisponde a utf8_general_ci
  var _maxPacketSize = (2 << (24 - 1)) - 1;
  var _clientConnectAttributes = {};

  ConnectionProtocol(Protocol protocol) : super(protocol) {
    _protocol._clientCapabilityFlags =
        _decodeFixedLengthInteger([0x0d, 0xa2, 0x00, 0x00]); // TODO sistemare
  }

  Future<ConnectionResult> connect(
      host, int port, String userName, String password, String database) async {
    var response = await _readInitialHandshakeResponse();

    if (response is! InitialHandshakePacket) {
      throw new ConnectionError(response.errorMessage);
    }

    _protocol._serverCapabilityFlags = response.serverCapabilityFlags;

    _writeHandshakeResponsePacket(userName, password, database,
        response.authPluginData, response.authPluginName);

    response = await _protocol._readCommandResponse();

    if (response is ErrorPacket) {
      throw new ConnectionError(response.errorMessage);
    }

    return new ConnectionResult(
        _protocol._serverCapabilityFlags, _protocol._clientCapabilityFlags);
  }

  void _writeHandshakeResponsePacket(String userName, String password,
      String database, String authPluginData, String authPluginName) {
    WriterBuffer buffer = _protocol._createBuffer();

    // TODO rivedere utilizzo capability flags
    if (_protocol._clientCapabilityFlags & CLIENT_CONNECT_WITH_DB != 0) {
      if (database == null) {
        _protocol._clientCapabilityFlags ^= CLIENT_CONNECT_WITH_DB;
      }
    } else if (database != null) {
      _protocol._clientCapabilityFlags ^= CLIENT_CONNECT_WITH_DB;
    }

    var sequenceId =
        0x01; // penso dipenda dalla sequenza a cui era arrivato il server

    // 4              capability flags, CLIENT_PROTOCOL_41 always set
    buffer.writeFixedLengthInteger(_protocol._clientCapabilityFlags, 4);
    // 4              max-packet size
    buffer.writeFixedLengthInteger(_maxPacketSize, 4);
    // 1              character set
    buffer.writeFixedLengthInteger(_characterSet, 1);
    // string[23]     reserved (all [0])
    buffer.writeFixedFilledLengthString(0x00, 23);
    // string[NUL]    username
    buffer.writeNulTerminatedUTF8String(userName);

    // if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
    if (_protocol._serverCapabilityFlags &
            CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA !=
        0) {
      // lenenc-int     length of auth-response
      // string[n]      auth-response
      buffer.writeLengthEncodedString(_generateAuthResponse(
          password, authPluginData, authPluginName,
          utf8Encoded: true));
      // else if capabilities & CLIENT_SECURE_CONNECTION {
    } else if (_protocol._serverCapabilityFlags & CLIENT_SECURE_CONNECTION !=
        0) {
      // 1              length of auth-response
      // string[n]      auth-response
      // TODO to implement
      throw new UnsupportedError("TODO to implement");
      // else {
    } else {
      // string[NUL]    auth-response
      // TODO to implement
      throw new UnsupportedError("TODO to implement");
    }
    // if capabilities & CLIENT_CONNECT_WITH_DB {
    if (_protocol._clientCapabilityFlags & CLIENT_CONNECT_WITH_DB != 0) {
      // string[NUL]    database
      buffer.writeNulTerminatedUTF8String(database);
    }
    // if capabilities & CLIENT_PLUGIN_AUTH {
    if (_protocol._serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
      // string[NUL]    auth plugin name
      buffer.writeNulTerminatedUTF8String(authPluginName);
    }
    // if capabilities & CLIENT_CONNECT_ATTRS {
    if (_protocol._serverCapabilityFlags & CLIENT_CONNECT_ATTRS != 0) {
      // lenenc-int     length of all key-values
      // lenenc-str     key
      // lenenc-str     value
      // if-more data in 'length of all key-values', more keys and value pairs
      var valuesBuffer = _protocol._createBuffer();
      _clientConnectAttributes.forEach((key, value) {
        valuesBuffer.writeLengthEncodedString(key);
        valuesBuffer.writeLengthEncodedString(value);
      });
      buffer.writeLengthEncodedInteger(valuesBuffer.length);
      buffer.writeBuffer(valuesBuffer);
    }

    var headerBuffer = _protocol._createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _protocol._writeBuffer(headerBuffer);
    _protocol._writeBuffer(buffer);
  }

  Future<Packet> _readInitialHandshakeResponse() {
    var value = _protocol._readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readInitialHandshakeResponsePacket())
        : _readInitialHandshakeResponsePacket();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  Packet _readInitialHandshakeResponsePacket() {
    if (_protocol._isErrorPacket()) {
      return _protocol._readErrorPacket();
    } else {
      return _readInitialHandshakePacket();
    }
  }

  InitialHandshakePacket _readInitialHandshakePacket() {
    var packet = new InitialHandshakePacket(
        _protocol._reusablePacketBuffer.sequenceId,
        _protocol._reusablePacketBuffer.payloadLength);

    // 1              [0a] protocol version
    packet._protocolVersion = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _protocol._reusableDataRange)
        .toInt();
    // string[NUL]    server version
    packet._serverVersion = _protocol._reusablePacketBuffer.payload
        .readNulTerminatedDataRange(_protocol._reusableDataRange)
        .toString();
    // 4              connection id
    packet._connectionId = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(4, _protocol._reusableDataRange)
        .toInt();
    // string[8]      auth-plugin-data-part-1
    packet._authPluginDataPart1 = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(8, _protocol._reusableDataRange)
        .toString();
    // 1              [00] filler
    _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _protocol._reusableDataRange);
    // 2              capability flags (lower 2 bytes)
    packet._capabilityFlags1 = _protocol._reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _protocol._reusableDataRange)
        .toInt();
    // if more data in the packet:
    if (!_protocol._reusablePacketBuffer.payload.isAllRead) {
      // 1              character set
      packet._characterSet = _protocol._reusablePacketBuffer.payload
          .readFixedLengthDataRange(1, _protocol._reusableDataRange)
          .toInt();
      // 2              status flags
      packet._statusFlags = _protocol._reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _protocol._reusableDataRange)
          .toInt();
      // 2              capability flags (upper 2 bytes)
      packet._capabilityFlags2 = _protocol._reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _protocol._reusableDataRange)
          .toInt();
      packet._serverCapabilityFlags =
          packet.capabilityFlags1 | (packet.capabilityFlags2 << 16);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet._serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // 1              length of auth-plugin-data
        packet._authPluginDataLength = _protocol._reusablePacketBuffer.payload
            .readFixedLengthDataRange(1, _protocol._reusableDataRange)
            .toInt();
      } else {
        // 1              [00]
        _protocol._reusablePacketBuffer.payload
            .readFixedLengthDataRange(1, _protocol._reusableDataRange);
        packet._authPluginDataLength = 0;
      }
      // string[10]     reserved (all [00])
      _protocol._reusablePacketBuffer.payload
          .readFixedLengthDataRange(10, _protocol._reusableDataRange);
      // if capabilities & CLIENT_SECURE_CONNECTION {
      if (packet._serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
        // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
        var len = max(packet._authPluginDataLength - 8, 13);
        packet._authPluginDataPart2 = _protocol._reusablePacketBuffer.payload
            .readFixedLengthDataRange(len, _protocol._reusableDataRange)
            .toString();
      } else {
        packet.authPluginDataPart2 = "";
      }
      packet._authPluginData =
          "${packet._authPluginDataPart1}${packet._authPluginDataPart2}"
              .substring(0, packet._authPluginDataLength - 1);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet._serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // string[NUL]    auth-plugin name
        packet._authPluginName = _protocol._reusablePacketBuffer.payload
            .readNulTerminatedDataRange(_protocol._reusableDataRange)
            .toString();
      }
    }

    _protocol._reusablePacketBuffer.free();
    _protocol._reusableDataRange.free();

    return packet;
  }

  String _generateAuthResponse(
      String password, String authPluginData, String authPluginName,
      {utf8Encoded: false}) {
    var encodedPassword = _encodeString(password, utf8Encoded: utf8Encoded);
    var encodedAuthPluginData =
        _encodeString(authPluginData, utf8Encoded: utf8Encoded);

    var response;

    if (authPluginName == "mysql_native_password") {
      // SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
      var passwordSha1 = (new SHA1()..add(encodedPassword)).close();
      var passwordSha1Sha1 = (new SHA1()..add(passwordSha1)).close();
      var hash = (new SHA1()..add(encodedAuthPluginData)..add(passwordSha1Sha1))
          .close();

      var buffer = new StringBuffer();
      var generatedHash = new List<int>(hash.length);
      for (var i = 0; i < generatedHash.length; i++) {
        buffer.writeCharCode(hash[i] ^ passwordSha1[i]);
      }
      response = buffer.toString();
    } else {
      throw new UnsupportedError(authPluginName);
    }

    return response;
  }

  List<int> _encodeString(String value, {utf8Encoded: false}) {
    return !utf8Encoded ? value.codeUnits : UTF8.encode(value);
  }

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
}

class InitialHandshakePacket extends Packet {
  int _protocolVersion;
  String _serverVersion;
  int _connectionId;
  String _authPluginDataPart1;
  int _capabilityFlags1;
  int _characterSet;
  int _statusFlags;
  int _capabilityFlags2;
  int _serverCapabilityFlags;
  int _authPluginDataLength;
  String _authPluginDataPart2;
  String _authPluginData;
  String _authPluginName;

  InitialHandshakePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);

  int get protocolVersion => _protocolVersion;
  String get serverVersion => _serverVersion;
  int get connectionId => _connectionId;
  String get authPluginDataPart1 => _authPluginDataPart1;
  int get capabilityFlags1 => _capabilityFlags1;
  int get characterSet => _characterSet;
  int get statusFlags => _statusFlags;
  int get capabilityFlags2 => _capabilityFlags2;
  int get serverCapabilityFlags => _serverCapabilityFlags;
  int get authPluginDataLength => _authPluginDataLength;
  String get authPluginDataPart2 => _authPluginDataPart2;
  String get authPluginData => _authPluginData;
  String get authPluginName => _authPluginName;
}

class ConnectionResult {
  final int serverCapabilityFlags;
  final int clientCapabilityFlags;

  ConnectionResult(this.serverCapabilityFlags, this.clientCapabilityFlags);
}
