// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_chunk;

import 'dart:math';
import 'data_statistics.dart';

class DataChunk {
  final List<int> _data;

  int _index;

  DataChunk(this._data) : this._index = 0 {
    CHUNK_COUNTER++;
  }

  bool get isEmpty => _data.length - _index == 0;

  void consume(int length, handler(List<int> data, int index, int available)) {
    length = min(_data.length - _index, length);
    handler(_data, _index, length);
    _index += length;
  }
}
