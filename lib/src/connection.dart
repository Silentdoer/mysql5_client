library mysql_client.connection;

import "dart:async";
import "dart:io";

import "package:mysql_client/src/data_reader.dart";
import "package:mysql_client/src/data_writer.dart";

import "package:mysql_client/src/protocol.dart";

class SqlError extends Error {}

abstract class Connection {
  Future<QueryResult> executeQuery(String query);

  Future close();
}

class ConnectionImpl implements Connection {
  ConnectionProtocol _connectionProtocol;
  QueryCommandTextProtocol _queryCommandTextProtocol;
  PreparedStatementProtocol _preparedStatementProtocol;

  int _serverCapabilityFlags;
  int _clientCapabilityFlags;

  Socket _socket;
  DataReader _reader;
  DataWriter _writer;

  Future connect(host, int port, String userName, String password,
      [String database]) async {
    _socket = await Socket.connect(host, port);
    _socket.setOption(SocketOption.TCP_NODELAY, true);

    _reader = new DataReader(_socket);
    _writer = new DataWriter(_socket);

    _connectionProtocol = new ConnectionProtocol(_writer, _reader);

    var connectionResult = await _connectionProtocol.connect(
        host, port, userName, password, database);

    _serverCapabilityFlags = connectionResult.serverCapabilityFlags;
    _clientCapabilityFlags = connectionResult.clientCapabilityFlags;
  }

  @override
  Future<QueryResult> executeQuery(String query) async {
    if (_queryCommandTextProtocol != null) {
      _queryCommandTextProtocol.reuse();
    } else {
      _queryCommandTextProtocol = new QueryCommandTextProtocol.reusable(
          _writer, _reader, _serverCapabilityFlags, _clientCapabilityFlags);
    }

    return _queryCommandTextProtocol.executeQuery(query);
  }

  Future<PreparedStatement> prepareQuery(String query) {
    if (_preparedStatementProtocol != null) {
      _preparedStatementProtocol.reuse();
    } else {
      _preparedStatementProtocol = new PreparedStatementProtocol.reusable(
          _writer, _reader, _serverCapabilityFlags, _clientCapabilityFlags);
    }

    return _preparedStatementProtocol.prepareQuery(query);
  }

  @override
  Future close() async {
    // TODO liberare i protocolli precedenti

    await _socket.close();

    _socket.destroy();

    _socket = null;
  }
}
