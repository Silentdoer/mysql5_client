library mysql_client.connection;

import "dart:async";
import "dart:io";
import "dart:math";
import "dart:convert";

import "package:crypto/crypto.dart";

import "data_reader.dart";
import "data_writer.dart";

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

class InitialHandshakePacket {
  int payloadLength;
  int sequenceId;
  int protocolVersion;
  int serverVersion;
  int connectionId;
  String authPluginDataPart1;
  int capabilityFlags1;
  int characterSet;
  int statusFlags;
  int capabilityFlags2;
  int serverCapabilityFlags;
  int authPluginDataLength;
  String authPluginDataPart2;
  String authPluginData;
  String authPluginName;
}

abstract class Connection {
  Future executeQuery(String query);

  Future close();
}

class ConnectionImpl implements Connection {
  int _serverCapabilityFlags;
  int _clientCapabilityFlags;

  var _characterSet = 0x21; // corrisponde a utf8_general_ci
  var _maxPacketSize = pow(2, 24) - 1;
  var _clientConnectAttributes = {};

  Socket _socket;
  DataReader _reader;
  DataWriter _writer;

  ConnectionImpl() {
    _clientCapabilityFlags =
        _decodeFixedLengthInteger([0x0d, 0xa2, 0x00, 0x00]); // TODO sistemare
  }

  Future connect(host, int port, String userName, String password, String database) async {
    _socket = await Socket.connect(host, port);
    _socket.setOption(SocketOption.TCP_NODELAY, true);

    _reader = new DataReader(_socket);
    _writer = new DataWriter(_socket);

    var initialHandshakePacket = await _readInitialHandshakePacket();

    _serverCapabilityFlags = initialHandshakePacket.serverCapabilityFlags;

    await _writeHandshakeResponsePacket(userName, password, database, initialHandshakePacket.authPluginData, initialHandshakePacket.authPluginName);

    await _readCommandResponsePacket();
  }

  @override
  Future close() async {
    await _socket.close();

    _socket.destroy();

    _socket = null;
  }

  @override
  Future executeQuery(String query) async {
    await _writeCommandQueryPacket(query);

    await _readCommandQueryResponsePacket();
  }

  Future<InitialHandshakePacket> _readInitialHandshakePacket() async {
    var packet = new InitialHandshakePacket();

    packet.payloadLength = await _reader.readFixedLengthInteger(3);

    packet.sequenceId = await _reader.readOneLengthInteger();

    _reader.resetExpectedPayloadLength(packet.payloadLength);

    // 1              [0a] protocol version
    packet.protocolVersion = await _reader.readOneLengthInteger();

    // string[NUL]    server version
    packet.serverVersion = await _reader.readNulTerminatedString();

    // 4              connection id
    packet.connectionId = await _reader.readFixedLengthInteger(4);

    // string[8]      auth-plugin-data-part-1
    packet.authPluginDataPart1 = await _reader.readFixedLengthString(8);

    // 1              [00] filler
    await _reader.skipByte();

    // 2              capability flags (lower 2 bytes)
    packet.capabilityFlags1 = await _reader.readFixedLengthInteger(2);

    // if more data in the packet:
    if (_reader.isAvailable) {
      // 1              character set
      packet.characterSet = await _reader.readOneLengthInteger();

      // 2              status flags
      packet.statusFlags = await _reader.readFixedLengthInteger(2);

      // 2              capability flags (upper 2 bytes)
      packet.capabilityFlags2 = await _reader.readFixedLengthInteger(2);

      packet.serverCapabilityFlags = packet.capabilityFlags1 | (packet.capabilityFlags2 << 16);
      print("serverCapabilityFlags: $_serverCapabilityFlags");

      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // 1              length of auth-plugin-data
        packet.authPluginDataLength = await _reader.readOneLengthInteger();
      } else {
        // 1              [00]
        await _reader.skipByte();
        packet.authPluginDataLength = 0;
      }

      // string[10]     reserved (all [00])
      await _reader.skipBytes(10);

      // if capabilities & CLIENT_SECURE_CONNECTION {
      if (packet.serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
        // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
        var len = max(packet.authPluginDataLength - 8, 13);
        packet.authPluginDataPart2 = await _reader.readFixedLengthString(len);
      } else {
        packet.authPluginDataPart2 = "";
      }

      packet.authPluginData = "${packet.authPluginDataPart1}${packet.authPluginDataPart2}"
          .substring(0, packet.authPluginDataLength - 1);

      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // string[NUL]    auth-plugin name
        packet.authPluginName = await _reader.readNulTerminatedString();
      }
    }
    return packet;
  }

  Future _writeHandshakeResponsePacket(String userName, String password, String database, String authPluginData, String authPluginName) async {
    WriterBuffer buffer = _writer.createBuffer();

    var sequenceId =
        0x01; // penso dipenda dalla sequenza a cui era arrivato il server

    // 4              capability flags, CLIENT_PROTOCOL_41 always set
    buffer.writeFixedLengthInteger(_clientCapabilityFlags, 4);
    // 4              max-packet size
    buffer.writeFixedLengthInteger(_maxPacketSize, 4);
    // 1              character set
    buffer.writeFixedLengthInteger(_characterSet, 1);
    // string[23]     reserved (all [0])
    buffer.writeFixedFilledLengthString(0x00, 23);
    // string[NUL]    username
    buffer.writeNulTerminatedUTF8String(userName);

    // if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
    if (_serverCapabilityFlags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0) {
      // lenenc-int     length of auth-response
      // string[n]      auth-response
      buffer.writeLengthEncodedString(_generateAuthResponse(
          password, authPluginData, authPluginName,
          utf8Encoded: true));
      // else if capabilities & CLIENT_SECURE_CONNECTION {
    } else if (_serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
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
    if (_serverCapabilityFlags & CLIENT_CONNECT_WITH_DB != 0) {
      // string[NUL]    database
      buffer.writeNulTerminatedUTF8String(database);
    }
    // if capabilities & CLIENT_PLUGIN_AUTH {
    if (_serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
      // string[NUL]    auth plugin name
      buffer.writeNulTerminatedUTF8String(authPluginName);
    }
    // if capabilities & CLIENT_CONNECT_ATTRS {
    if (_serverCapabilityFlags & CLIENT_CONNECT_ATTRS != 0) {
      // lenenc-int     length of all key-values
      // lenenc-str     key
      // lenenc-str     value
      // if-more data in 'length of all key-values', more keys and value pairs
      var valuesBuffer = _writer.createBuffer();
      _clientConnectAttributes.forEach((key, value) {
        valuesBuffer.writeLengthEncodedString(key);
        valuesBuffer.writeLengthEncodedString(value);
      });
      buffer.writeLengthEncodedInteger(valuesBuffer.length);
      buffer.writeBuffer(valuesBuffer);
    }

    var headerBuffer = _writer.createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _writer.writeBuffer(headerBuffer);
    _writer.writeBuffer(buffer);
  }

  Future _readCommandResponsePacket() async {
    var payloadLength = await _reader.readFixedLengthInteger(3);
    print("payloadLength: $payloadLength");

    var sequenceId = await _reader.readOneLengthInteger();
    print("sequenceId: $sequenceId");

    _reader.resetExpectedPayloadLength(payloadLength);

    // int<1>	header	[00] or [fe] the OK packet header
    var header = await _reader.readOneLengthInteger();
    print("header: $header");

    // TODO distinguere il pacchetto OK, ERROR

    // int<lenenc>	affected_rows	affected rows
    var affectedRows = await _reader.readLengthEncodedInteger();
    print("affectedRows: $affectedRows");

    // int<lenenc>	last_insert_id	last insert-id
    var lastInsertId = await _reader.readLengthEncodedInteger();
    print("lastInsertId: $lastInsertId");

    var statusFlags;
    // if capabilities & CLIENT_PROTOCOL_41 {
    if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	status_flags	Status Flags
      statusFlags = await _reader.readFixedLengthInteger(2);
      print("statusFlags: $statusFlags");
      // int<2>	warnings	number of warnings
      var warnings = await _reader.readFixedLengthInteger(2);
      print("warnings: $warnings");
      // } elseif capabilities & CLIENT_TRANSACTIONS {
    } else if (_serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
      // int<2>	status_flags	Status Flags
      statusFlags = await _reader.readFixedLengthInteger(2);
      print("statusFlags: $statusFlags");
    } else {
      statusFlags = 0;
      print("statusFlags: $statusFlags");
    }

    // if capabilities & CLIENT_SESSION_TRACK {
    if (_serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
      // string<lenenc>	info	human readable status information
      if (_reader.isAvailable) {
        var info = await _reader.readLengthEncodedString();
        print("info: $info");
      }

      // if status_flags & SERVER_SESSION_STATE_CHANGED {
      if (statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
        // string<lenenc>	session_state_changes	session state info
        if (_reader.isAvailable) {
          var sessionStateChanges = await _reader.readLengthEncodedString();
          print("sessionStateChanges: $sessionStateChanges");
        }
      }
      // } else {
    } else {
      // string<EOF>	info	human readable status information
      var info = await _reader.readRestOfPacketString();
      print("info: $info");
    }
  }

  Future _writeCommandQueryPacket(String query) async {
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

  Future _readCommandQueryResponsePacket() async {
    await _readResultSetColumnCountResponsePacket();

    var columnCount = 3;
    for (var i = 0; i < columnCount; i++) {
      await _readResultSetColumnDefinitionResponsePacket();
    }
    await _readEOFResponsePacket();

    try {
      while (true) {
        await _readResultSetRowResponsePacket();
      }
    } on EOFError {
      if (_reader.isFirstByte) {
        // EOF packet
        // if capabilities & CLIENT_PROTOCOL_41 {
        if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
          // int<2>	warnings	number of warnings
          var warnings = await _reader.readFixedLengthInteger(2);

          // int<2>	status_flags	Status Flags
          var statusFlags = await _reader.readFixedLengthInteger(2);
        }
      } else {
        rethrow;
      }
    } on UndefinedError {
      if (_reader.isFirstByte) {
        // TODO Error packet

        throw new UnsupportedError("IMPLEMENT STARTED ERROR PACKET");
      } else {
        rethrow;
      }
    }
  }

  Future _readResultSetColumnCountResponsePacket() async {
    var payloadLength = await _reader.readFixedLengthInteger(3);

    var sequenceId = await _reader.readOneLengthInteger();

    _reader.resetExpectedPayloadLength(payloadLength);

    // A packet containing a Protocol::LengthEncodedInteger column_count
    var columnCount = await _reader.readOneLengthInteger();
  }

  Future _readResultSetColumnDefinitionResponsePacket() async {
    var payloadLength = await _reader.readFixedLengthInteger(3);

    var sequenceId = await _reader.readOneLengthInteger();

    _reader.resetExpectedPayloadLength(payloadLength);

    // lenenc_str     catalog
    var catalog = await _reader.readLengthEncodedString();

    // lenenc_str     schema
    var schema = await _reader.readLengthEncodedString();

    // lenenc_str     table
    var table = await _reader.readLengthEncodedString();

    // lenenc_str     org_table
    var orgTable = await _reader.readLengthEncodedString();

    // lenenc_str     name
    var name = await _reader.readLengthEncodedString();

    // lenenc_str     org_name
    var orgName = await _reader.readLengthEncodedString();

    // lenenc_int     length of fixed-length fields [0c]
    var fieldsLength = await _reader.readLengthEncodedInteger();

    // 2              character set
    var characterSet = await _reader.readFixedLengthInteger(2);

    // 4              column length
    var columnLength = await _reader.readFixedLengthInteger(4);

    // 1              type
    var type = await _reader.readOneLengthInteger();

    // 2              flags
    var flags = await _reader.readFixedLengthInteger(2);

    // 1              decimals
    var decimals = await _reader.readOneLengthInteger();

    // 2              filler [00] [00]
    await _reader.skipBytes(2);
  }

  Future _readResultSetRowResponsePacket() async {
    var payloadLength = await _reader.readFixedLengthInteger(3);

    var sequenceId = await _reader.readFixedLengthInteger(1);

    _reader.resetExpectedPayloadLength(payloadLength);

    while (_reader.isAvailable) {
      var value;
      try {
        value = await _reader.readLengthEncodedString();
      } on NullError {
        value = null;
      }
    }
  }

  Future _readEOFResponsePacket() async {
    var payloadLength = await _reader.readFixedLengthInteger(3);

    var sequenceId = await _reader.readOneLengthInteger();

    _reader.resetExpectedPayloadLength(payloadLength);

    // int<1>	header	[00] or [fe] the OK packet header
    var header = await _reader.readOneLengthInteger();
    if (header != 0xfe) {
      throw new StateError("$header != 0xfe");
    }

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	warnings	number of warnings
      var warnings = await _reader.readFixedLengthInteger(2);
      // int<2>	status_flags	Status Flags
      var statusFlags = await _reader.readFixedLengthInteger(2);
    }
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
