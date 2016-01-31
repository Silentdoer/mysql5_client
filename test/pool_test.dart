import "dart:async";

import "package:stack_trace/stack_trace.dart";

import "package:mysql_client/mysql_client.dart";

Future main() async {
  await Chain.capture(() async {
    await capturedMain();
  });
}

Future capturedMain() async {
  await test1();
}

Future test1() async {
  var connection;

  try {
    connection = await new ConnectionFactory()
        .connect("localhost", 3306, "root", "mysql", "test");

    var queryResult =
        await connection.executeQuery("SELECT count(*) FROM people");

    // rows
    while (true) {
      var next = await queryResult.next();
      if (!next) {
        break;
      }

      print("${queryResult.getNumValue(0)}");
    }
  } finally {
    await connection.close();
  }
}

Future test2() async {
  var pool;

  try {
    pool = new ConnectionPool(
        host: "localhost",
        port: 3306,
        userName: "root",
        password: "mysql",
        database: "test",
        maxConnections: 10,
        connectionTimeout: new Duration(seconds: 30));

    var connection;

    try {
      connection = await pool.request();

      var queryResult =
          await connection.executeQuery("SELECT count(*) FROM people");

      // rows
      while (true) {
        var next = await queryResult.next();
        if (!next) {
          break;
        }

        print("${queryResult.getNumValue(0)}");
      }
    } finally {
      await connection.close();
    }
  } finally {
    await pool.close();
  }
}
