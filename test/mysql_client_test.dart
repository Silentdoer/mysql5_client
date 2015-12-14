// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';

Future main() async {
  await test1();
}

Future test3() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var queryResult = await connection
        .executeQuery("INSERT INTO people(name, age) VALUES('roby', 42)");

    queryResult.close();
  } finally {
    await connection.close();
  }
}

Future test1() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 10");

    // column count
    var columnCount = queryResult.columnCount;
    print(columnCount);

    // column definitions
    var columnSetReader = queryResult.columnSetReader;
    while (true) {
      var next = await columnSetReader.next();
/*
      var next = columnSetReader.internalNext();
      next = next is Future ? await next : next;
*/
      if (!next) {
        break;
      }

      print(columnSetReader.name);
    }
    columnSetReader.close();

    // rows
    var rowSetReader = queryResult.rowSetReader;
    while (true) {
      var next = await rowSetReader.next();
      if (!next) {
        break;
      }

      print(rowSetReader.getString(0));
    }
    rowSetReader.close();

    queryResult.close();
  } finally {
    await connection.close();
  }
}

Future test2() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    await connection.executeQuery("SELECT * FROM people LIMIT 10");
  } finally {
    await connection.close();
  }
}
