// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import '../lib/mysql5_client.dart';

const SIMPLE_INSERTS = 300;
const SIMPLE_SELECTS = 300;

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
      await executeQuery("DROP TABLE pets");
    } catch (e) {}

    try {
      await executeQuery("DROP TABLE people");
    } catch (e) {}
  }

  Future createTables() async {
    print("creating tables");

    await executeQuery("""
    create table people (id int not null auto_increment,
        name varchar(255),
        age integer,
        name2 varchar(255),
        name3 varchar(255),
        name4 varchar(255),
        name5 varchar(255),
        name6 varchar(255),
        primary key (id))
  """);

    await executeQuery("""
    create table pets (id integer not null auto_increment,
        name varchar(255),
        species varchar(255),
        owner_id integer,
        primary key (id),
        foreign key (owner_id) references people (id))
  """);
  }

  Future insertSimpleData() async {
    print("inserting simple data");
    var sw = new Stopwatch()..start();
    for (var i = 0; i < SIMPLE_INSERTS; i++) {
      await executeQuery(
          "insert into people (name, age) values ('person$i', $i)");
      print('inserted ${i + 1}');
    }
    logTime("simple insertions", sw);
  }

  Future selectSimpleData() async {
    print("selecting simple data");
    var sw = new Stopwatch()..start();
    for (var i = 0; i < SIMPLE_SELECTS; i++) {
      var queryResult = await executeQuery("select * from people limit 100");

      // rows
      var hasRow = true;
      while (hasRow) {
        hasRow = await queryResult.next();
//        hasRow = queryResult.rawNext();
//        hasRow = hasRow is Future ? await hasRow : hasRow;
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
  final ConnectionFactory factory2 = new ConnectionFactory();

  Connection? connection;

  Future run() async {
    connection =
        await factory2.connect("localhost", 3306, "root", "wyzpass", "db_test");

    await super.run();

    await connection?.close();

    connection = null;
  }

  Future<QueryResult> executeQuery(String sql) {
    return connection!.executeQuery(sql);
  }
}
