// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';
import "package:stack_trace/stack_trace.dart";

Future main() async {
  Chain.capture(() async {
    await test6();
  }, onError: (e, s) {
    print(e);
    print(s.terse);
  });
}

Future test6() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var queryResult;

    queryResult =
      await connection.executeQuery("SELECT * FROM people LIMIT 100");

    queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 10");

    print(queryResult.columnCount);

    // column definitions
    var columnSetReader = await queryResult.columnIterator();
    while (true) {
      var next = await columnSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(columnSetReader.name);
    }

    // rows
    var rowSetReader = await queryResult.rowIterator();
    while (true) {
      var next = await rowSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(rowSetReader.getString(0));
    }

    queryResult = await connection.executeQuery("SELECT * FROM people LIMIT 5");

    print(queryResult.columnCount);

    // rows
    rowSetReader = await queryResult.rowIterator();
    while (true) {
      var next = await rowSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(rowSetReader.getString(0));
    }
  } catch (e, s) {
    print("Error: $e");
    print(new Chain.forTrace(s).terse);
  } finally {
    await connection.close();
  }
}

Future test5() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 10");

    // column count
    var columnCount = queryResult.columnCount;
    print(columnCount);

    // column definitions
    var columnSetReader = await queryResult.columnIterator();
    while (true) {
      var next = await columnSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(columnSetReader.name);
    }

    // rows
    var rowSetReader = await queryResult.rowIterator();
    while (true) {
      var next = await rowSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(rowSetReader.getString(0));
    }
  } catch (e, s) {
    print("Error: $e");
    print(new Chain.forTrace(s).terse);
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
      var next = await paramSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(paramSetReader.name);
    }
    paramSetReader.close();

    // column definitions
    var columnSetReader = preparedStatement.columnSetReader;
    while (true) {
      var next = await columnSetReader.nextAsFuture();
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
