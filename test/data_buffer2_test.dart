// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client2.dart';

Future main() async {
  await run();
}

Future run() async {

  var buffer = new ReaderBuffer();

  buffer.add(new DataRange([]));

  print("'${buffer.readRestOfPacketString()}'");

  buffer.add(new DataRange([0x00]));

  print("'${buffer.readNulTerminatedString()}'");

  buffer.add(new DataRange([0x70, 0x00, 0x71]));

  print("'${buffer.readNulTerminatedString()}'");

  print("'${buffer.readFixedLengthString(1)}'");

  buffer.add(new DataRange([0x70, 0x71]));

  buffer.add(new DataRange([0x70, 0x00, 0x72]));

  print("'${buffer.readNulTerminatedString()}'");

  print("'${buffer.readFixedLengthString(1)}'");
}