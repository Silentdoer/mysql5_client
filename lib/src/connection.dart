library mysql_client.connection;

import "dart:async";
import "dart:io";

import "package:mysql_client/src/data_reader.dart";
import "package:mysql_client/src/data_writer.dart";

import "package:mysql_client/src/protocol.dart";

class SqlError extends Error {}

abstract class Connection {
  Future executeQuery(String query);

  Future close();
}

class ConnectionImpl implements Connection {
  int _serverCapabilityFlags;
  int _clientCapabilityFlags;

  Socket _socket;
  DataReader _reader;
  DataWriter _writer;

  Future connect(
      host, int port, String userName, String password, String database) async {
    _socket = await Socket.connect(host, port);
    // TODO verifica in rete
    _socket.setOption(SocketOption.TCP_NODELAY, true);

    _reader = new DataReader(_socket);
    _writer = new DataWriter(_socket);

    var protocol = new ConnectionProtocol(_writer, _reader);

    var connectionResult =
        await protocol.connect(host, port, userName, password, database);

    _serverCapabilityFlags = connectionResult.serverCapabilityFlags;
    _clientCapabilityFlags = connectionResult.clientCapabilityFlags;

    connectionResult.close();
  }

  @override
  Future executeQuery(String query) async {
    var protocol = new QueryCommandTextProtocol(
        _writer, _reader, _serverCapabilityFlags, _clientCapabilityFlags);

    return protocol.executeQuery(query);
  }

  @override
  Future close() async {
    await _socket.close();

    _socket.destroy();

    _socket = null;
  }
}
