// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_chunk;

import "dart:async";
import "dart:collection";
import "dart:convert";
import "dart:math";

import "data_chunk.dart";
import "data_buffer.dart";
import "data_range.dart";

class DataChunk {
  final List<int> _data;

  int _index;

  DataChunk(this._data) : this._index = 0;

  bool get isEmpty => _data.length - _index == 0;

  void skipSingle() {
    _index++;
  }

  int readSingle() => _data[_index++];

  DataRange readFixedRange(int length) {
    var readData = new DataRange(_data, _index, length);
    _index += readData.length;
    return readData;
  }

  DataRange readRangeUpTo(int terminator) {
    var readData;

    var toIndex = _data.indexOf(terminator, _index) + 1;

    if (toIndex > 0) {
      readData = new DataRange(_data, _index, toIndex - _index - 1);
      _index = toIndex;
    } else {
      readData = new DataRange(_data, _index, _data.length - _index + 1);
      _index = _data.length;
    }

    return readData;
  }
}
