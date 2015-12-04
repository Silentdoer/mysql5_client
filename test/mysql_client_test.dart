// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';
import "package:sqljocky/sqljocky.dart";

// sudo ngrep -x -q -d lo0 '' 'port 3306'

Future main() async {
  await test1();
  // await test2();
}

Future test1() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var sw = new Stopwatch()..start();
    for (var i = 0; i < 1; i++) {
      await connection.executeQuery("SELECT * FROM people LIMIT 10");
    }
    print("testMySql: ${sw.elapsedMilliseconds} ms");
  } finally {
    await connection.close();
  }
}

Future test2() async {
  var pool = new ConnectionPool(
      host: 'localhost', port: 3306,
      user: 'root', password: 'mysql',
      db: 'test', max: 1);
  var connection = await pool.getConnection();

  var sw = new Stopwatch()..start();
  for (var i = 0; i < 1; i++) {
    var results = await connection.query('SELECT * FROM people LIMIT 10');
    await results.length;
  }

  print("testMySql: ${sw.elapsedMilliseconds} ms");

  await connection.release();
  pool.closeConnectionsWhenNotInUse();
}