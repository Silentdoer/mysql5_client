// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import '../lib/mysql5_client.dart';

const SIMPLE_INSERTS = 5000;
const SIMPLE_SELECTS = 1;

// sudo ngrep -x -q -d lo0 '' 'port 3306'

Future main() async {
  await new MySqlClientSpeedTest().run();
}

abstract class SpeedTest {
  Future<QueryResult> executeQuery(String sql);

  Future run() async {
    await dropTables();
    await createTables();
    await insertSimpleData();
    await selectSimpleData();
  }

  Future dropTables() async {
    print("dropping tables");
    try {
      await executeQuery("DROP TABLE prova");
    } catch (e) {}
  }

  Future createTables() async {
    print("creating tables");

    await executeQuery("""
    create table prova (id integer not null auto_increment,
        name longtext,
        primary key (id))
  """);
  }

  Future insertSimpleData() async {
    print("inserting simple data");
    var sw = new Stopwatch()..start();
    //var name = new String.fromCharCodes(new List.filled(100000, 0x70));
    for (var i = 0; i < SIMPLE_INSERTS; i++) {
      // 当一次性执行多条insert请求时，这里会卡主，应该是连接数问题？
      // 说明每次insert完是没有释放连接的，有bug
      await executeQuery("insert into prova (name) values ('$i-OK')");
      print('inserted: ${i + 1}');
    }
    logTime("simple insertions", sw);
  }

  Future selectSimpleData() async {
    print("selecting simple data");
    var sw = new Stopwatch()..start();
    for (var i = 0; i < SIMPLE_SELECTS; i++) {
      var queryResult = await executeQuery("select * from prova LIMIT 10");

      // rows，必须得先next()否则用不了
      // 和java里的先用然后判断hasNext()不一样
      // 而且这样更合理，否则万一一条数据都没有直接用就挂了
      while (await queryResult.next()) {
        // queryResult是指向某一行的行标，取当前行第二列的值作为String
        var rows = queryResult.getStringValue(1);
        print('select result: ${rows}');
        await new Future.delayed(new Duration(milliseconds: 100));
      }
    }
    logTime("simple selects", sw);
  }

  void logTime(String operation, Stopwatch sw) {
    var time = sw.elapsedMicroseconds;
    var seconds = time / 1000;
    print("$operation took: ${seconds} ms");
  }
}

class MySqlClientSpeedTest extends SpeedTest {
  Connection? connection;

  Future run() async {
    var factory2 = new ConnectionFactory();

    connection =
        await factory2.connect("localhost", 3306, "root", "wyzpass", "db_test");

    await super.run();

    await connection?.close();
  }

  Future<QueryResult> executeQuery(String sql) {
    return connection!.executeQuery(sql);
  }
}
