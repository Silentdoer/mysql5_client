// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';

Future main() async {
  await testMySqlClient();
}

Future testMySqlClient() async {
  var time;
  var connection;

  try {
    var factory = new ConnectionFactory();

    connection = await factory.connect("104.155.81.67", 3306, "sysadmin_vpd", "oracle");

    time = new DateTime.now().millisecondsSinceEpoch;

    await connection.executeQuery(
        "CALL sysadmin_vpd.set_attributes('primeapp', null, 'test')");

    var queryResult = await connection.executeQuery("""
      select * from v_application.node limit 100
    """);

    // column count
    var columnCount = queryResult.columnCount;

    // rows
    var rows = [];
    while (true) {
      var next = await queryResult.next();
      if (!next) {
        break;
      }

      var row = [];
      for (var i = 0; i < columnCount; i++) {
        row.add(queryResult.getStringValue(i));
      }
      rows.add(row);
    }

    print(rows.length);

    // print(rows);
  } finally {
    await connection.close();

    print(
        "Closed connection in ${new DateTime.now().millisecondsSinceEpoch - time} ms");
  }
}
