import "dart:async";

import "package:stack_trace/stack_trace.dart";

import "../lib/mysql5_client.dart";

Future main() async {
  await Chain.capture(() async {
    await capturedMain();
  });
}

Future capturedMain() async {
  await test2();
}

Future test2() async {
  var pool;

  try {
    pool = new ConnectionPool(
        host: "localhost",
        port: 3306,
        userName: "root",
        password: "wyzpass",
        database: "db_test",
        maxConnections: 10,
        connectionTimeout: new Duration(seconds: 30));

    await testConnection(pool);

    await testConnection(pool);

    await testConnection(pool);

    await testConnection(pool);

    await testConnection(pool);
  } finally {
    await pool.close();
  }
}

Future testConnection(ConnectionPool pool) async {
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
    await connection?.close();
  }
}
