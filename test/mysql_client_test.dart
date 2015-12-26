// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';
import "package:stack_trace/stack_trace.dart";

Future main() async {
  Chain.capture(() async {
    await test5();
  }, onError: (e, s) {
    print(e);
    print(s.terse);
  });
}

Future test5() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql");

    var queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 10");

    // column count
    var columnCount = queryResult.columnCount;
    print(columnCount);

    // column definitions
    var columnSetReader = queryResult.columnIterator;
    while (true) {
      var next = await columnSetReader.next();
      if (!next) {
        break;
      }

      print(columnSetReader.name);
    }

    // rows
    var rowSetReader = queryResult.rowSetIterator;
    while (true) {
      var next = await rowSetReader.next();
      if (!next) {
        break;
      }

      print(rowSetReader.getString(0));
    }
  } finally {
    await connection.close();
  }
}

Future test4() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "");

    var preparedStatement =
        await connection.prepareQuery("SELECT * FROM people LIMIT 10");

    // param definitions
    var paramSetReader = preparedStatement.paramSetReader;
    while (true) {
      var next = await paramSetReader.next();
      if (!next) {
        break;
      }

      print(paramSetReader.name);
    }
    paramSetReader.close();

    // column definitions
    var columnSetReader = preparedStatement.columnSetReader;
    while (true) {
      var next = await columnSetReader.next();
      if (!next) {
        break;
      }

      print(columnSetReader.name);
    }
    columnSetReader.close();

    preparedStatement.close();
  } finally {
    await connection.close();
  }
}

Future test3() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var queryResult = await connection
        .executeQuery("INSERT INTO people(name, age) VALUES('roby', 42)");
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
    var columnSetReader = queryResult.columnIterator;
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

    // rows
    var rowSetReader = queryResult.rowSetIterator;
    while (true) {
      var next = await rowSetReader.next();
      if (!next) {
        break;
      }

      print(rowSetReader.getString(0));
    }
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
