library mysql_client.connection;

import 'dart:async';

import "connection/connection_impl.dart";

class ConnectionError extends Error {
  final String message;

  ConnectionError(this.message);

  String toString() => "ConnectionError: $message";
}

class QueryError extends Error {
  final String message;

  QueryError(this.message);

  String toString() => "QueryError: $message";
}

class PreparedStatementError extends Error {
  final String message;

  PreparedStatementError(this.message);

  String toString() => "PreparedStatementError: $message";
}

class ColumnDefinition {
  final String name;
  final int type;

  ColumnDefinition(this.name, this.type);
}

abstract class ConnectionFactory {
  factory ConnectionFactory() {
    return new ConnectionFactoryImpl();
  }

  Future<Connection> connect(host, int port, String userName, String password,
      [String database]);
}

abstract class Connection {
  bool get isClosed;

  Future<QueryResult> executeQuery(String query);

  Future<PreparedStatement> prepareQuery(String query);

  Future close();
}

abstract class QueryResult implements CommandResult, DataIterator {
  int get affectedRows;

  int get lastInsertId;

  int get columnCount;

  List<ColumnDefinition> get columns;

  String getStringValue(int index);

  num getNumValue(int index);

  bool getBoolValue(int index);

  // TODO aggiungere skip e limit
  // TODO aggiungere hint tipo sql per il recupero
  Future<List<List>> getNextRows();
}

abstract class PreparedStatement implements CommandResult {
  int get parameterCount;

  int get columnCount;

  List<ColumnDefinition> get columns;

  bool get isClosed;

  void setParameter(int index, value, [int sqlType]);

  Future<QueryResult> executeQuery();
}

abstract class CommandResult {
  Future free();

  Future close();
}

abstract class DataIterator {
  bool get isClosed;

  Future<bool> next();

  // TODO qui si potrebbe utilizzare il FutureWrapper
  rawNext();

  Future close();
}

abstract class RowIterator implements DataIterator {
  String getStringValue(int index);

  num getNumValue(int index);

  bool getBoolValue(int index);
}
