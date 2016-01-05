// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_writer;

import "dart:io";

import "package:mysql_client/src/writer_buffer.dart";
export "package:mysql_client/src/writer_buffer.dart" show WriterBuffer;

class DataWriter {
  final IOSink _sink;

  DataWriter(this._sink);

  WriterBuffer createBuffer() => new WriterBuffer();

  void writeBuffer(WriterBuffer buffer) {
    buffer.addToSink(_sink);
  }
}
