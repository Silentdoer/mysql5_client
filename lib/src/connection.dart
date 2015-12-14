library mysql_client.connection;

import "dart:async";
import "dart:io";
import "dart:math";
import "dart:convert";

import "package:crypto/crypto.dart";

import "package:mysql_client/src/data_reader.dart";
import "package:mysql_client/src/data_writer.dart";

import "package:mysql_client/src/packet_reader.dart";

class SqlError extends Error {}

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
  PacketReader _reader;
  DataWriter _writer;

  ConnectionImpl() {
    _clientCapabilityFlags =
        _decodeFixedLengthInteger([0x0d, 0xa2, 0x00, 0x00]); // TODO sistemare
  }

  Future connect(
      host, int port, String userName, String password, String database) async {
    _socket = await Socket.connect(host, port);
    // TODO verifica in rete
    _socket.setOption(SocketOption.TCP_NODELAY, true);

    _reader = new PacketReader(new DataReader(_socket),
        clientCapabilityFlags: _clientCapabilityFlags);
    _writer = new DataWriter(_socket);

    var response1 = await _reader.readInitialHandshakeResponse();

    if (response1 is! InitialHandshakePacket) {
      throw new SqlError();
    }

    _reader.serverCapabilityFlags = response1.serverCapabilityFlags;
    _serverCapabilityFlags = response1.serverCapabilityFlags;

    await _writeHandshakeResponsePacket(userName, password, database,
        response1.authPluginData, response1.authPluginName);

    var response2 = await _reader.readCommandResponse();

    if (response2 is ErrorPacket) {
      throw new SqlError();
    }
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

    var response = await _reader.readCommandQueryResponse();

    if (response is OkPacket) {
      return;
    }

    if (response is! ResultSetColumnCountResponsePacket) {
      throw new SqlError();
    }

    var columnCount = response.columnCount;

    var reusableColumnPacket =
        new ResultSetColumnDefinitionResponsePacket.reusable();
    for (var i = 0; i < columnCount; i++) {
      response =
          _reader.readResultSetColumnDefinitionResponse(reusableColumnPacket);
      response = response is Future ? await response : response;
    }
    reusableColumnPacket.free();

    response = _reader.readEOFResponse();

    var reusableResultSetPacket =
        new ResultSetRowResponsePacket.reusable(columnCount);
    while (true) {
      response = _reader.readResultSetRowResponse(reusableResultSetPacket);
      response = response is Future ? await response : response;
      if (response is! ResultSetRowResponsePacket) {
        break;
      }
    }
    reusableResultSetPacket.free();
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
