part of mysql_client.protocol;

class ConnectionProtocol extends ProtocolDelegate {
  var _characterSet = 0x21; // corrisponde a utf8_general_ci
  var _maxPacketSize = (2 << (24 - 1)) - 1;
  var _clientConnectAttributes = {"prova": "ciao"};

  ConnectionProtocol(Protocol protocol) : super(protocol) {
    _clientCapabilityFlags =
        _protocol._decodeInteger([0x0d, 0xa2, 0x00, 0x00]); // TODO sistemare
  }

  // TODO passare clientCapabilityFlags, clientConnectAttributes, characterSet e maxPacketSize come parametri
  void writeHandshakeResponsePacket(String userName, String password,
      String database, String authPluginData, String authPluginName) {
    _createWriterBuffer();

    // TODO rivedere utilizzo capability flags
    if (_clientCapabilityFlags & CLIENT_CONNECT_WITH_DB != 0) {
      if (database == null) {
        _clientCapabilityFlags ^= CLIENT_CONNECT_WITH_DB;
      }
    } else if (database != null) {
      _clientCapabilityFlags ^= CLIENT_CONNECT_WITH_DB;
    }

    var sequenceId =
        0x01; // penso dipenda dalla sequenza a cui era arrivato il server

    // 4              capability flags, CLIENT_PROTOCOL_41 always set
    _writeFixedLengthInteger(_clientCapabilityFlags, 4);
    // 4              max-packet size
    _writeFixedLengthInteger(_maxPacketSize, 4);
    // 1              character set
    _writeByte(_characterSet);
    // string[23]     reserved (all [0])
    _writeFixedFilledLengthString(0x00, 23);
    // string[NUL]    username
    _writeNulTerminatedUTF8String(userName);

    // if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
    if (_serverCapabilityFlags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0) {
      // lenenc-int     length of auth-response
      // string[n]      auth-response
      _writeLengthEncodedString(_generateAuthResponse(
          password, authPluginData, authPluginName,
          utf8Encoded: true));
      // else if capabilities & CLIENT_SECURE_CONNECTION {
    } else if (_serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
      // 1              length of auth-response
      // string[n]      auth-response
      // TODO to implement auth-response
      throw new UnsupportedError("TODO to implement");
      // else {
    } else {
      // string[NUL]    auth-response
      // TODO to implement auth-response
      throw new UnsupportedError("TODO to implement");
    }
    // if capabilities & CLIENT_CONNECT_WITH_DB {
    if (_clientCapabilityFlags & CLIENT_CONNECT_WITH_DB != 0) {
      // string[NUL]    database
      _writeNulTerminatedUTF8String(database);
    }
    // if capabilities & CLIENT_PLUGIN_AUTH {
    if (_serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
      // string[NUL]    auth plugin name
      _writeNulTerminatedUTF8String(authPluginName);
    }
    // if capabilities & CLIENT_CONNECT_ATTRS {
    if (_serverCapabilityFlags & CLIENT_CONNECT_ATTRS != 0) {
      // lenenc-int     length of all key-values
      // lenenc-str     key
      // lenenc-str     value
      // if-more data in 'length of all key-values', more keys and value pairs
      var tempBuffer = _writerBuffer;
      _createWriterBuffer();
      _clientConnectAttributes.forEach((key, value) {
        _writeLengthEncodedString(key);
        _writeLengthEncodedString(value);
      });
      var valuesBuffer = _writerBuffer;
      _writerBuffer = tempBuffer;
      _writeLengthEncodedInteger(valuesBuffer.length);
      _writeBytes(valuesBuffer);
    }

    _writePacket(sequenceId);
  }

  Future<Packet> readInitialHandshakeResponse() {
    var value = _readPacketBuffer();
    var value2 = value is Future
        ? value.then((_) => _readInitialHandshakeResponsePacket())
        : _readInitialHandshakeResponsePacket();
    return value2 is Future ? value2 : new Future.value(value2);
  }

  Packet _readInitialHandshakeResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else {
      return _readInitialHandshakePacket();
    }
  }

  InitialHandshakePacket _readInitialHandshakePacket() {
    var packet = new InitialHandshakePacket(_payloadLength, _sequenceId);

    if (_header != 0x0a) {
      throw new StateError("Invalid packet header: $_header != 0x0a");
    }

    // 1              [0a] protocol version
    packet._protocolVersion = _header;
    // string[NUL]    server version
    packet._serverVersion = _readNulTerminatedString();
    // 4              connection id
    packet._connectionId = _readFixedLengthInteger(4);
    // string[8]      auth-plugin-data-part-1
    packet._authPluginDataPart1 = _readFixedLengthString(8);
    // 1              [00] filler
    _skipByte();
    // 2              capability flags (lower 2 bytes)
    packet._capabilityFlags1 = _readFixedLengthInteger(2);
    // if more data in the packet:
    if (!_isAllRead) {
      // 1              character set
      packet._characterSet = _readByte();
      // 2              status flags
      packet._statusFlags = _readFixedLengthInteger(2);
      // 2              capability flags (upper 2 bytes)
      packet._capabilityFlags2 = _readFixedLengthInteger(2);
      packet._serverCapabilityFlags =
          packet.capabilityFlags1 | (packet.capabilityFlags2 << 16);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet._serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // 1              length of auth-plugin-data
        packet._authPluginDataLength = _readByte();
      } else {
        // 1              [00]
        _skipByte();
        packet._authPluginDataLength = 0;
      }
      // string[10]     reserved (all [00])
      _skipBytes(10);
      // if capabilities & CLIENT_SECURE_CONNECTION {
      if (packet._serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
        // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
        var len = max(packet._authPluginDataLength - 8, 13);
        packet._authPluginDataPart2 = _readFixedLengthString(len);
      } else {
        packet.authPluginDataPart2 = "";
      }
      packet._authPluginData =
          "${packet._authPluginDataPart1}${packet._authPluginDataPart2}"
              .substring(0, packet._authPluginDataLength - 1);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet._serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // string[NUL]    auth-plugin name
        packet._authPluginName = _readNulTerminatedString();
      }
    }

    _serverCapabilityFlags = packet._serverCapabilityFlags;

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
