// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';

// sudo ngrep -x -q -d lo0 '' 'port 3306'

Future main() async {
  await run();
}

Future run() async {

  var connection = new ConnectionImpl();

  await connection.connect("localhost", 3306, "root", "mysql", "test");

  var sw = new Stopwatch()..start();

  for (var i = 0; i < 100; i++) {
    await connection.executeQuery("SELECT * FROM people");
  }

  print("testMySql: ${sw.elapsedMilliseconds} ms");

  await connection.close();

  print("CHUNK_COUNTER: $CHUNK_COUNTER");
  print("RANGE_COUNTER: $RANGE_COUNTER");
  print("BUFFER_COUNTER: $BUFFER_COUNTER");
  print("LIST1_COUNTER: $LIST1_COUNTER");
  print("LIST2_COUNTER: $LIST2_COUNTER");
  print("SUBLIST_COUNTER: $SUBLIST_COUNTER");
}