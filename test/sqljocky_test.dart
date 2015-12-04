// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import "package:sqljocky/sqljocky.dart";

Future main() async {
  await run();
}

Future run() async {
  var pool = new ConnectionPool(
      host: 'localhost', port: 3306,
      user: 'root', password: 'mysql',
      db: 'test', max: 1);

  var connection = await pool.getConnection();

  var sw = new Stopwatch()..start();

  for (var i = 0; i < 1; i++) {
    var results = await connection.query('SELECT * FROM people LIMIT 100');
    await results.length;
  }

  print("testMySql: ${sw.elapsedMilliseconds} ms");

  await connection.release();

  pool.closeConnectionsWhenNotInUse();
}