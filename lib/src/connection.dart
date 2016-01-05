library mysql_client.connection;

import "dart:async";
import "dart:io";

import "package:mysql_client/src/protocol.dart";

class ConnectionError extends Error {
  final String message;

  ConnectionError(this.message);

  String toString() => "ConnectionError: $message";
}

class SqlError extends Error {}

abstract class Connection {
  bool get isClosed;

  Future connect(host, int port, String userName, String password,
      [String database]);

  Future<QueryResult> executeQuery(String query);

  Future<PreparedStatement> prepareQuery(String query);

  Future close();
}

class ConnectionImpl implements Connection {
  Socket _socket;

  Protocol _protocol;

  ProtocolResult _lastProtocolResult;

  @override
  bool get isClosed => _protocol == null;

  @override
  Future connect(host, int port, String userName, String password,
      [String database]) async {
    if (!isClosed) {
      throw new StateError("Connection already connected");
    }

    var socket = await Socket.connect(host, port);
    socket.setOption(SocketOption.TCP_NODELAY, true);

    var protocol = new Protocol(socket);

    var response =
        await protocol.connectionProtocol.readInitialHandshakeResponse();

    if (response is! InitialHandshakePacket) {
      throw new ConnectionError(response.errorMessage);
    }

    protocol.serverCapabilityFlags = response.serverCapabilityFlags;

    protocol.connectionProtocol.writeHandshakeResponsePacket(userName, password,
        database, response.authPluginData, response.authPluginName);

    response = await protocol.readCommandResponse();

    if (response is ErrorPacket) {
      throw new ConnectionError(response.errorMessage);
    }

    _socket = socket;
    _protocol = protocol;
  }

  @override
  Future<QueryResult> executeQuery(String query) async {
    if (isClosed) {
      throw new StateError("Connection closed");
    }

    await _lastProtocolResult?.free();

    _lastProtocolResult =
        await _protocol.queryCommandTextProtocol.executeQuery(query);

    return _lastProtocolResult;
  }

  @override
  Future<PreparedStatement> prepareQuery(String query) async {
    if (isClosed) {
      throw new StateError("Connection closed");
    }

    await _lastProtocolResult?.free();

    _lastProtocolResult =
        await _protocol.preparedStatementProtocol.prepareQuery(query);

    return _lastProtocolResult;
  }

  @override
  Future close() async {
    if (isClosed) {
      throw new StateError("Connection closed");
    }

    await _lastProtocolResult?.free();

    var socket = _socket;

    _socket = null;
    _protocol = null;

    await socket.close();
    socket.destroy();
  }
}
