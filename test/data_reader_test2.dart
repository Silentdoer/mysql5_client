// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";
import "dart:math";
import "dart:typed_data";

import 'package:mysql_client/mysql_client.dart';

Future main() async {
  var controller = new StreamController<List<int>>();
  controller.add([71, 72, 73]);
  controller.add([74, 75, 76]);
  controller.add([0x01, 70]);
  controller.add([0x00]);
  controller.add([0x01, 70]);
  controller.add([0xfb]);

  var reader = new DataStreamReader(controller.stream);
  // print(await reader.readBytes(2));
  print(await reader.readBytes(6));

  // print(await reader.readFixedLengthString(4));

  // print(await reader.readBytes(3));

  // print(await reader.readLengthEncodedInteger());
  // print(await reader.readLengthEncodedInteger());
  print(await reader.readLengthEncodedString());

  print("***");

  print(await reader.readLengthEncodedString());

  print(await reader.readLengthEncodedString());

  print(await reader.readLengthEncodedString());
/*
  try {
    print(await reader.readLengthEncodedString());
  } on NullError {
    print("Null value");
  } on UndefinedError {
    print("Undefined value");
  }

  print(await reader.readLengthEncodedString());

  try {
    print(await reader.readLengthEncodedString());
  } on NullError {
    print("Null value");
  } on UndefinedError {
    print("Undefined value");
  }
*/
}