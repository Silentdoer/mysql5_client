// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";
import "dart:math";
import "dart:typed_data";

import 'package:mysql_client/mysql_client.dart';

Future main() async {
  var controller = new StreamController<List<int>>();
  controller.add([1, 2, 3]);
  controller.add([4, 5, 6]);

  var reader = new DataStreamReader(controller.stream);
  print(await reader.readBytes(2));

  var buffer = await reader.readFixedLengthBuffer(2);
  print(buffer.singleRange.data.sublist(buffer.singleRange.start, buffer.singleRange.end));

  var sw = new Stopwatch();
  sw.start();
  for (var i = 0; i < 4000000; i++) {
    new DataBuffer();
    new DataRange([]);
  }
  print("Elapsed in ${sw.elapsedMilliseconds} ms");
}