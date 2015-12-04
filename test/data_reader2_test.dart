// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';

Future main() async {
  await run();
}

Future run() async {
  var chunks = [];

  // chunks.add([0x70, 0x71]);
  // chunks.add([0x75, 0x76, 0x00, 0x70, 0x71]);
  // chunks.add([0x70, 0x71]);
  chunks.add([0, 0, 2, 0, 0, 0]);

  var reader = new DataReader(new Stream.fromIterable(chunks));

  var buffer = await reader.readBuffer(6);

  print("left: ${buffer.leftLoadingCount}");

  // TODO distinguere il pacchetto OK, ERROR

  // int<lenenc>	affected_rows	affected rows
  var affectedRows = buffer.readLengthEncodedInteger();
  print("affectedRows: $affectedRows");

  print("left: ${buffer.leftLoadingCount}");

  // int<lenenc>	last_insert_id	last insert-id
  var lastInsertId = buffer.readLengthEncodedInteger();
  print("lastInsertId: $lastInsertId");

  print("left: ${buffer.leftLoadingCount}");

}