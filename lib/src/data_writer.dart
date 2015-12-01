// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_writer;

import "dart:io";

class DataWriter {
  final IOSink _sink;

  DataWriter(this._sink);

  void write(List<int> data) {
    _sink.add(data);
  }
}