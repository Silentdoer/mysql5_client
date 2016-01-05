// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.writer_buffer;

import "dart:io";

class WriterBuffer {
  final List<int> _data = new List();

  int get length => _data.length;

  WriterBuffer();

  void addToSink(IOSink sink) {
    sink.add(_data);
  }

  void writeBytes(List<int> bytes) {
    _data.addAll(bytes);
  }

  void writeBuffer(WriterBuffer buffer) {
    _data.addAll(buffer._data);
  }

  void writeByte(int value) {
    _data.add(value);
  }
}
