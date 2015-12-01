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

  await connection.connect("localhost", 3306);

  var sw = new Stopwatch()..start();

  for (var i = 0; i < 10; i++) {
    await connection.executeQuery("SELECT * FROM people");
  }

  print("testMySql: ${sw.elapsedMilliseconds} ms");

  await connection.close();
}