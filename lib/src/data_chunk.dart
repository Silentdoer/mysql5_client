// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_chunk;

import 'dart:math';
import 'data_range.dart';

class DataChunk {
  final List<int> _data;
  int _start;
  int _length;

  DataChunk(this._data, [this._start = 0, this._length]) {
    this._length ??= this._data.length - this._start;
  }

  bool get isEmpty => _length == 0;
  int get length => _length;

  int checkOneByte() => _data[_start];

  int extractOneByte() {
    var value = _data[_start];
    _start++;
    _length--;
    return value;
  }

  DataChunk extractDataChunk(int length) {
    length = min(_length, length);
    var chunk = new DataChunk(_data, _start, length);
    _start += length;
    _length -= length;
    return chunk;
  }

  DataRange extractFixedLengthDataRange(int length) {
    DataRange range;
    if (length <= _length) {
      range = new DataRange(_data, _start, length);
      _start += length;
      _length -= length;
    } else {
      range = new DataRange.pending(_data, _start);
      _start += range.length;
      _length -= range.length;
    }
    return range;
  }

  DataRange extractUpToDataRange(int terminator) {
    DataRange range;
    int i = _data.indexOf(terminator, _start);
    if (i != -1) {
      var length = i - _start;
      range = new DataRange(_data, _start, length);
      // skip the terminator
      _start += length + 1;
      _length -= length + 1;
    } else {
      range = new DataRange.pending(_data, _start);
      _start += range.length;
      _length -= range.length;
    }
    return range;
  }
}
