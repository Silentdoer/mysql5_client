// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';
import "package:stack_trace/stack_trace.dart";

Future main() async {
  Chain.capture(() async {
    await test4();
  }, onError: (e, s) {
    print(e);
    print(s.terse);
  });
}

Future test4() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var preparedStatement =
        await connection.prepareQuery("SELECT * FROM people LIMIT 10");

    print("Parameters: ${preparedStatement.parameterCount}");
    print("Columns: ${preparedStatement.columnCount}");

    // param definitions
    var parameterIterator = await preparedStatement.parameterIterator();
    var hasParameter = true;
    while (hasParameter) {
      hasParameter = await parameterIterator.next();

      if (hasParameter) {
        print("Parameter: ${parameterIterator.name}");

      }
    }

    // column definitions
    var columnIterator = await preparedStatement.columnIterator();
    var hasColumn = true;
    while (hasColumn) {
      hasColumn = await columnIterator.next();

      if (hasColumn) {
        print("Column: ${columnIterator.name}");
      }
    }
  } finally {
    await connection.close();
  }
}

Future test7() async {
  var connection = new ConnectionImpl();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var queryResult;

    queryResult = await connection
        .executeQuery("INSERT INTO people(name, age) values ('hans', 42)");

    print(queryResult.affectedRows);

    queryResult =
        await connection.executeQuery("SELECT * FROM people WHERE age = 42");

    // rows
    var rowSetReader = await queryResult.rowIterator();
    while (true) {
      var next = await rowSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print("${rowSetReader.getString(0)}: ${rowSetReader.getString(1)}");
    }
  } finally {
    await connection.close();
  }
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

    queryResult = await connection.executeQuery("SELECT * FROM people LIMIT 0");
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
