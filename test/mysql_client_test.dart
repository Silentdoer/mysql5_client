// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';

Future main() async {
  await test1();
}

Future test1() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var sw = new Stopwatch()..start();
    for (var i = 0; i < 10; i++) {
      await connection.executeQuery("SELECT * FROM people");
    }
    print("testMySql: ${sw.elapsedMilliseconds} ms");
  } finally {
    await connection.close();
  }
}