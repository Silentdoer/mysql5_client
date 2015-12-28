// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';
import "package:sqljocky/sqljocky.dart";
import "package:sqlconnection/sql_connection_remote.dart";

Future main() async {
  await testRemoteSql();

  await testSqlJocky();

  await testMySqlClient();
}

Future testMySqlClient() async {
  var time;
  var connection;

  try {
    connection = new ConnectionImpl();

    await connection.connect("104.155.81.67", 3306, "sysadmin_vpd", "oracle");

    time = new DateTime.now().millisecondsSinceEpoch;

    await connection.executeQuery(
        "CALL sysadmin_vpd.set_attributes('primeapp', null, 'test')");

    var queryResult = await connection.executeQuery("""
      select * from v_application.node limit 100
    """);

    // column count
    var columnCount = queryResult.columnCount;

    // column definitions
    var columnSetReader = await queryResult.columnIterator();
    while (true) {
      var next = await columnSetReader.nextAsFuture();
      if (!next) {
        break;
      }
    }

    // rows
    var rows = [];
    var rowSetReader = await queryResult.rowIterator();
    while (true) {
      var next = await rowSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      var row = [];
      for (var i = 0; i < columnCount; i++) {
        row.add(rowSetReader.getUTF8String(i));
      }
      rows.add(row);
    }

    print(rows.length);

    // print(rows);
  } finally {
    await connection.close();

    print(
        "Closed connection in ${new DateTime.now().millisecondsSinceEpoch - time} ms");
  }
}

Future testSqlJocky() async {
  var time;
  var pool;
  var connection;

  try {
    pool = new ConnectionPool(
        host: '104.155.81.67',
        port: 3306,
        user: 'sysadmin_vpd',
        password: 'oracle',
        max: 1);

    connection = await pool.getConnection();

    time = new DateTime.now().millisecondsSinceEpoch;

    await connection
        .query("CALL sysadmin_vpd.set_attributes('primeapp', null, 'test')");

    var results = await connection.query("""
      select * from v_application.node limit 100
    """);

    var rows = await results.toList();

    print(rows.length);

    // print(rows);
  } finally {
    await connection.release();

    pool.closeConnectionsWhenNotInUse();

    print(
        "Closed connection in ${new DateTime.now().millisecondsSinceEpoch - time} ms");
  }
}

Future testRemoteSql() async {
  var time;

  var factory = new RemoteSqlConnectionFactory("127.0.0.1", 7777);

  var connection;

  try {
    time = new DateTime.now().millisecondsSinceEpoch;

    connection = await factory.createConnection("application:primeapp");

    var records = await connection.query("""
      select * from v_application.node limit 100
    """);

    print(records.data.length);

    // print(records.data);
  } finally {
    if (connection != null) {
      await connection.close();
    }

    if (factory != null) {
      await factory.closeConnections();
    }

    print(
        "Closed connection in ${new DateTime.now().millisecondsSinceEpoch - time} ms");
  }
}
