// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';
import "package:stack_trace/stack_trace.dart";

Future main() async {
  Chain.capture(() async {
    try {
      await test10();
    } catch (e, s) {
      print(e);
      print(new Chain.forTrace(s));
    }
  });
}

Future test10() async {
  var connection = new Connection();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    // TODO verificare datetime e timestamp
    await connection.executeQuery("""
      CREATE TEMPORARY TABLE test_table (
        id INTEGER NOT NULL AUTO_INCREMENT,
        string1 VARCHAR(255),
        number1 INTEGER,
        number2 DOUBLE,
        bool1 TINYINT,
        PRIMARY KEY (id)
      )
    """);

    var statement;
    var result;
    var rowIterator;

    // inserimento

    result = await connection.executeQuery("""
      INSERT INTO test_table(string1, number1, number2, bool1)
        VALUES('test1€', 999, 10.6876, 1)
    """);
    print("affectedRows: ${result.affectedRows}");
    print("lastInsertId: ${result.lastInsertId}");

    statement = await connection.prepareQuery("""
      INSERT INTO test_table(string1, number1, number2, bool1)
        VALUES(?, ?, ?, ?)
    """);

    statement.setParameter(0, "test2€");
    statement.setParameter(1, 999);
    statement.setParameter(2, 10.6876);
    statement.setParameter(3, true);

    result = await statement.executeQuery();
    print("affectedRows: ${result.affectedRows}");
    print("lastInsertId: ${result.lastInsertId}");

    statement.setParameter(0, "");
    statement.setParameter(1, -1000);
    statement.setParameter(2, -1000);
    statement.setParameter(3, false);

    result = await statement.executeQuery();
    print("affectedRows: ${result.affectedRows}");
    print("lastInsertId: ${result.lastInsertId}");

    statement.setParameter(0, null);
    statement.setParameter(1, null);
    statement.setParameter(2, null);
    statement.setParameter(3, null);

    result = await statement.executeQuery();
    print("affectedRows: ${result.affectedRows}");
    print("lastInsertId: ${result.lastInsertId}");

    // recupero

    result = await connection.executeQuery("""
      SELECT * FROM test_table
    """);

    // rows
    rowIterator = await result.rowIterator();
    while (await rowIterator.nextAsFuture()) {
      print([
        rowIterator.getNumValue(0),
        rowIterator.getStringValue(1),
        rowIterator.getNumValue(2),
        rowIterator.getNumValue(3),
        rowIterator.getBoolValue(4)
      ].join(","));
    }

    statement = await connection.prepareQuery("""
      SELECT * FROM test_table
    """);

    result = await statement.executeQuery();

    // rows
    rowIterator = await result.rowIterator();
    while (await rowIterator.nextAsFuture()) {
      print([
        rowIterator.getNumValue(0),
        rowIterator.getStringValue(1),
        rowIterator.getNumValue(2),
        rowIterator.getNumValue(3),
        rowIterator.getBoolValue(4)
      ].join(","));
    }
  } finally {
    try {
      // TODO problema nelle catch credo
      await connection.close();
    } catch (e) {
    }
  }
}

Future test9() async {
  var connection = new Connection();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var preparedStatement =
        await connection.prepareQuery("SELECT * FROM people WHERE id = ?");

    print("Parameters: ${preparedStatement.parameterCount}");
    print("Columns: ${preparedStatement.columnCount}");

    preparedStatement.setParameter(0, 10);

    var queryResult = await preparedStatement.executeQuery();

    // column count
    var columnCount = queryResult.columnCount;
    print(columnCount);

    for (var column in preparedStatement.columns) {
      print("Type: ${column.type}");
    }

    // rows
    var rowSetReader = await queryResult.rowIterator();
    while (true) {
      var next = await rowSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(
          "${rowSetReader.getNumValue(0)}: ${rowSetReader.getStringValue(1)}");
    }

    await queryResult.close();

    await preparedStatement.close();
  } finally {
    await connection.close();
  }
}

Future test8() async {
  var connection = new Connection();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var queryResult =
        await connection.executeQuery("SELECT * FROM people WHERE id = 10");

    print(queryResult.columns);
    print(queryResult.columnCount);

    // rows
    var rowSetReader = await queryResult.rowIterator();
    while (true) {
      var next = await rowSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(
          "${rowSetReader.getNumValue(0)}: ${rowSetReader.getStringValue(1)}");
    }
  } catch (e, s) {
    print("Error: $e");
    print(new Chain.forTrace(s).terse);
  } finally {
    await connection.close();
  }
}

Future test4() async {
  var connection = new Connection();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var preparedStatement =
        await connection.prepareQuery("SELECT * FROM people WHERE id = ?");

    print("Parameters: ${preparedStatement.parameterCount}");
    print("Columns: ${preparedStatement.columnCount}");

    await preparedStatement.close();
  } finally {
    await connection.close();
  }
}

Future test7() async {
  var connection = new Connection();

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

      print(
          "${rowSetReader.getNumValue(0)}: ${rowSetReader.getStringValue(1)}");
    }
  } finally {
    await connection.close();
  }
}

Future test6() async {
  var connection = new Connection();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var queryResult;

    queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 100");

    queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 10");

    print(queryResult.columnCount);

    // rows
    var rowSetReader = await queryResult.rowIterator();
    while (true) {
      var next = await rowSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(rowSetReader.getNumValue(0));
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

      print(rowSetReader.getNumValue(0));
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
  var connection = new Connection();

  try {
    await connection.connect("localhost", 3306, "root", "mysql", "test");

    var queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 10");

    // column count
    var columnCount = queryResult.columnCount;
    print(columnCount);

    // rows
    var rowSetReader = await queryResult.rowIterator();
    while (true) {
      var next = await rowSetReader.nextAsFuture();
      if (!next) {
        break;
      }

      print(rowSetReader.getNumValue(0));
    }
  } catch (e, s) {
    print("Error: $e");
    print(new Chain.forTrace(s).terse);
  } finally {
    await connection.close();
  }
}
