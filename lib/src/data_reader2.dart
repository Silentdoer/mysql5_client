// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_reader;

import "dart:async";
import "dart:collection";

import "reader_buffer2.dart";

class NullError extends Error {
  String toString() => "Null value";
}

class UndefinedError extends Error {

  final ReaderBuffer buffer;

  UndefinedError(this.buffer);

  String toString() => "Undefined value";
}

class EOFError extends Error {
  final ReaderBuffer buffer;

  EOFError(this.buffer);

  String toString() => "EOF value";
}

class DataReader {
  final Stream<List<int>> _stream;

  DataReader(this._stream) {
    // this._stream.listen(_onData);
  }

  Future<ReaderBuffer> readBuffer(int length) async {
    throw new UnsupportedError("TOIMPLEMENT");
  }
}