// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/src/data_reader.dart';

Future main() async {
  await run();
}

Future run() async {
  var chunks = [];

  chunks.add([0x00, 0x00, 0x00, 0x01]);

  var reader = new DataReader(new Stream.fromIterable(chunks));

  var buffer = await reader.readBuffer(4);

  print(buffer.readFixedLengthInteger(3));
}