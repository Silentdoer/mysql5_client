library mysql_client.connection;

import "dart:async";
import "dart:io";
import "dart:math";
import "dart:convert";

import "package:crypto/crypto.dart";

import "data_commons.dart";
import "reader_buffer.dart";
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
  String serverVersion;
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

  Future connect(
      host, int port, String userName, String password, String database) async {
    _socket = await Socket.connect(host, port);
    _socket.setOption(SocketOption.TCP_NODELAY, true);

    _reader = new DataReader(_socket);
    _writer = new DataWriter(_socket);

    var initialHandshakePacket = await _readInitialHandshakePacket();

    _serverCapabilityFlags = initialHandshakePacket.serverCapabilityFlags;

    await _writeHandshakeResponsePacket(
        userName,
        password,
        database,
        initialHandshakePacket.authPluginData,
        initialHandshakePacket.authPluginName);

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

    var headerBuffer = await _reader.readBuffer(4);
    packet.payloadLength = headerBuffer.readFixedLengthInteger(3);
    packet.sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(packet.payloadLength);

    // 1              [0a] protocol version
    packet.protocolVersion = buffer.readOneLengthInteger();
    // string[NUL]    server version
    packet.serverVersion = buffer.readNulTerminatedString();
    // 4              connection id
    packet.connectionId = buffer.readFixedLengthInteger(4);
    // string[8]      auth-plugin-data-part-1
    packet.authPluginDataPart1 = buffer.readFixedLengthString(8);
    // 1              [00] filler
    buffer.skipByte();
    // 2              capability flags (lower 2 bytes)
    packet.capabilityFlags1 = buffer.readFixedLengthInteger(2);

    // if more data in the packet:
    if (!buffer.isAllRead) {
      // 1              character set
      packet.characterSet = buffer.readOneLengthInteger();

      // 2              status flags
      packet.statusFlags = buffer.readFixedLengthInteger(2);

      // 2              capability flags (upper 2 bytes)
      packet.capabilityFlags2 = buffer.readFixedLengthInteger(2);

      packet.serverCapabilityFlags =
          packet.capabilityFlags1 | (packet.capabilityFlags2 << 16);

      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // 1              length of auth-plugin-data
        packet.authPluginDataLength = buffer.readOneLengthInteger();
      } else {
        // 1              [00]
        buffer.skipByte();
        packet.authPluginDataLength = 0;
      }

      // string[10]     reserved (all [00])
      buffer.skipBytes(10);

      // if capabilities & CLIENT_SECURE_CONNECTION {
      if (packet.serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
        // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
        var len = max(packet.authPluginDataLength - 8, 13);
        packet.authPluginDataPart2 = buffer.readFixedLengthString(len);
      } else {
        packet.authPluginDataPart2 = "";
      }

      packet.authPluginData =
          "${packet.authPluginDataPart1}${packet.authPluginDataPart2}"
              .substring(0, packet.authPluginDataLength - 1);

      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // string[NUL]    auth-plugin name
        packet.authPluginName = buffer.readNulTerminatedString();
      }
    }

    buffer.deinitialize();

    return packet;
  }

  Future _writeHandshakeResponsePacket(String userName, String password,
      String database, String authPluginData, String authPluginName) async {
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
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);

    // int<1>	header	[00] or [fe] the OK packet header
    var header = buffer.readOneLengthInteger();

    // TODO distinguere il pacchetto OK, ERROR

    // int<lenenc>	affected_rows	affected rows
    var affectedRows = buffer.readLengthEncodedInteger();
    // int<lenenc>	last_insert_id	last insert-id
    var lastInsertId = buffer.readLengthEncodedInteger();

    var statusFlags;
    // if capabilities & CLIENT_PROTOCOL_41 {
    if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	status_flags	Status Flags
      statusFlags = buffer.readFixedLengthInteger(2);
      // int<2>	warnings	number of warnings
      var warnings = buffer.readFixedLengthInteger(2);
      // } elseif capabilities & CLIENT_TRANSACTIONS {
    } else if (_serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
      // int<2>	status_flags	Status Flags
      statusFlags = buffer.readFixedLengthInteger(2);
    } else {
      statusFlags = 0;
    }

    // if capabilities & CLIENT_SESSION_TRACK {
    if (_serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
      // string<lenenc>	info	human readable status information
      if (!buffer.isAllRead) {
        var info = buffer.readLengthEncodedString();
      }

      // if status_flags & SERVER_SESSION_STATE_CHANGED {
      if (statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
        // string<lenenc>	session_state_changes	session state info
        if (!buffer.isAllRead) {
          var sessionStateChanges = buffer.readLengthEncodedString();
        }
      }
      // } else {
    } else {
      // string<EOF>	info	human readable status information
      var info = buffer.readRestOfPacketString();
    }

    buffer.deinitialize();
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
    } on EOFError catch (e) {
      if (e.buffer.isFirstByte) {
        // EOF packet
        // if capabilities & CLIENT_PROTOCOL_41 {
        if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
          // int<2>	warnings	number of warnings
          var warnings = e.buffer.readFixedLengthInteger(2);

          // int<2>	status_flags	Status Flags
          var statusFlags = e.buffer.readFixedLengthInteger(2);
        }
        e.buffer.deinitialize();
      } else {
        e.buffer.deinitialize();

        rethrow;
      }
    } on UndefinedError catch (e) {
      if (e.buffer.isFirstByte) {
        // TODO Error packet

        e.buffer.deinitialize();

        throw new UnsupportedError("IMPLEMENT STARTED ERROR PACKET");
      } else {
        e.buffer.deinitialize();

        rethrow;
      }
    }
  }

  Future _readResultSetColumnCountResponsePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);

    // A packet containing a Protocol::LengthEncodedInteger column_count
    var columnCount = buffer.readOneLengthInteger();

    buffer.deinitialize();
  }

  Future _readResultSetColumnDefinitionResponsePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);

    // lenenc_str     catalog
    var catalog = buffer.readLengthEncodedString();

    // lenenc_str     schema
    var schema = buffer.readLengthEncodedString();

    // lenenc_str     table
    var table = buffer.readLengthEncodedString();

    // lenenc_str     org_table
    var orgTable = buffer.readLengthEncodedString();

    // lenenc_str     name
    var name = buffer.readLengthEncodedString();

    // lenenc_str     org_name
    var orgName = buffer.readLengthEncodedString();

    // lenenc_int     length of fixed-length fields [0c]
    var fieldsLength = buffer.readLengthEncodedInteger();

    // 2              character set
    var characterSet = buffer.readFixedLengthInteger(2);

    // 4              column length
    var columnLength = buffer.readFixedLengthInteger(4);

    // 1              type
    var type = buffer.readOneLengthInteger();

    // 2              flags
    var flags = buffer.readFixedLengthInteger(2);

    // 1              decimals
    var decimals = buffer.readOneLengthInteger();

    // 2              filler [00] [00]
    buffer.skipBytes(2);

    buffer.deinitialize();
  }

  Future _readResultSetRowResponsePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);

    while (!buffer.isAllRead) {
      var value;
      try {
        value = buffer.readLengthEncodedString();
      } on NullError {
        value = null;
      }
    }

    buffer.deinitialize();
  }

  Future _readEOFResponsePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);

    // int<1>	header	[00] or [fe] the OK packet header
    var header = buffer.readOneLengthInteger();
    if (header != 0xfe) {
      throw new StateError("$header != 0xfe");
    }

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (_serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	warnings	number of warnings
      var warnings = buffer.readFixedLengthInteger(2);
      // int<2>	status_flags	Status Flags
      var statusFlags = buffer.readFixedLengthInteger(2);
    }

    buffer.deinitialize();
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
