library mysql_client.connection;

import "dart:async";
import "dart:io";

import "package:mysql_client/src/protocol.dart";

class SqlError extends Error {}

// TODO implementare anche un metodo release
abstract class Connection {
  bool get isConnected;

  bool get isClosed;

  Future connect(host, int port, String userName, String password,
      [String database]);

  Future<QueryResult> executeQuery(String query);

  Future<PreparedStatement> prepareQuery(String query);

  void free();

  Future close();
}

class ConnectionImpl implements Connection {
  Socket _socket;

  Protocol _protocol;

  @override
  bool get isConnected => _protocol != null;

  @override
  bool get isClosed => _socket == null;

  @override
  Future connect(host, int port, String userName, String password,
      [String database]) async {
    if (isConnected) {
      throw new StateError("Connection already connected");
    }

    _socket = await Socket.connect(host, port);
    _socket.setOption(SocketOption.TCP_NODELAY, true);

    var protocol = new Protocol(_socket);

    await protocol.connectionProtocol
        .connect(host, port, userName, password, database);

    _protocol = protocol;
  }

  @override
  Future<QueryResult> executeQuery(String query) async {
    if (!isConnected) {
      throw new StateError("Connection not connected");
    }

    return _protocol.queryCommandTextProtocol.executeQuery(query);
  }

  @override
  Future<PreparedStatement> prepareQuery(String query) {
    if (!isConnected) {
      throw new StateError("Connection not connected");
    }

    return _protocol.preparedStatementProtocol.prepareQuery(query);
  }

  @override
  void free() {
    _protocol.free();
  }

  @override
  Future close() async {
    if (isClosed) {
      throw new StateError("Connection already closed");
    }

    if (isConnected) {
      _protocol.destroy();

      _protocol = null;
    }

    await _socket.close();

    _socket.destroy();

    _socket = null;
  }
}
