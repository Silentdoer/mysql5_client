library mysql_client.connection;

import "dart:async";
import "dart:io";

import "package:mysql_client/src/protocol.dart";

class SqlError extends Error {}

// TODO implementare anche un metodo release
abstract class Connection {
  Future<QueryResult> executeQuery(String query);

  Future close();
}

class ConnectionImpl implements Connection {
  Socket _socket;

  Protocol _protocol;

  Future connect(host, int port, String userName, String password,
      [String database]) async {
    _socket = await Socket.connect(host, port);
    _socket.setOption(SocketOption.TCP_NODELAY, true);

    _protocol = new Protocol(_socket);

    await _protocol.connectionProtocol
        .connect(host, port, userName, password, database);
  }

  @override
  Future<QueryResult> executeQuery(String query) async =>
      _protocol.queryCommandTextProtocol.executeQuery(query);

  Future<PreparedStatement> prepareQuery(String query) =>
      _protocol.preparedStatementProtocol.prepareQuery(query);

  @override
  Future close() async {
    // TODO liberare i protocolli precedenti

    await _socket.close();

    _socket.destroy();

    _socket = null;
  }
}
