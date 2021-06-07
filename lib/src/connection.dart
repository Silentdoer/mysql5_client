library mysql_client.connection;

import 'dart:async';

import 'package:decimal/decimal.dart';

import "connection/connection_impl.dart";
import 'type/sql_type.dart';

class ConnectionError extends Error {
  final String message;

  ConnectionError(this.message);

  @override
  String toString() => "ConnectionError: $message";
}

class QueryError extends Error {
  final String message;

  QueryError(this.message);

  @override
  String toString() => "QueryError: $message";
}

class PreparedStatementError extends Error {
  final String message;

  PreparedStatementError(this.message);

  @override
  String toString() => "PreparedStatementError: $message";
}

class ColumnDefinition {
  final String name;
  final int type;

  ColumnDefinition(this.name, this.type);
}

abstract class ConnectionPool {
  factory ConnectionPool(
          {
          required String host,
          required int port,
          required String userName,
          required String password,
          required String database,
          int maxConnections = 10,
          Duration? connectionTimeout}) =>
      new ConnectionPoolImpl(
          host: host,
          port: port,
          userName: userName,
          password: password,
          database: database,
          maxConnections: maxConnections,
          connectionTimeout: connectionTimeout);

  bool get isClosed;

  Future<Connection> request();

  Future close();
}

abstract class ConnectionFactory {
  factory ConnectionFactory() {
    return new ConnectionFactoryImpl();
  }

  Future<Connection> connect(String host, int port, String userName, String password,
      [String? database]);
}

abstract class Connection {
  bool get isClosed;

  Future<QueryResult> executeQuery(String query);

  Future<PreparedStatement> prepareQuery(String query);

  Future close();
}

abstract class QueryResult implements CommandResult, RowIterator {
  int? get affectedRows;

  int? get lastInsertId;

  int? get columnCount;

  List<ColumnDefinition>? get columns;

  // TODO aggiungere skip e limit
  // TODO aggiungere hint tipo sql per il recupero
  Future<List<List>> getNextRows();
}

abstract class PreparedStatement implements CommandResult {
  int get parameterCount;

  int get columnCount;

  List<ColumnDefinition> get parameters;

  List<ColumnDefinition> get columns;

  bool get isClosed;

  ConnectionImpl get connection;

  List<int> get columnTypes;

  void setParameter(int index, value, [SqlType sqlType]);

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
  dynamic rawNext();

  Future close();
}

abstract class RowIterator implements DataIterator {
  String? getStringValue(int index);

  num? getNumValue(int index);

  bool? getBoolValue(int index);

  DateTime? getDateTimeValue(int index);

  Decimal? getDecimalValue(int index);

  double? getDoubleValue(int index);

  int? getIntegerValue(int index);
}
