// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_chunk;

import 'dart:math';
import 'data_range.dart';

class DataChunk {
  final List<int> _data;

  int _index;

  DataChunk(this._data) : this._index = 0;

  bool get isEmpty => _data.length - _index == 0;

  DataRange extractDataRange(int length) {
    length = min(_data.length - _index, length);
    var range = new DataRange(_data, _index, length);
    _index += length;
    return range;
  }
}
