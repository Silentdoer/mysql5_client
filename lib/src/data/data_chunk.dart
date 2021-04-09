// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

part of mysql_client.data;

class DataChunk {
  List<int>? _data;
  int? _start;
  int? _length;

  DataChunk(List<int> data, [int start = 0, int? length]) {
    reuse(data, start, length);
  }

  DataChunk.reusable();

  DataChunk reuse(List<int> data, [int start = 0, int? length]) {
    _data = data;
    _start = start;
    _length = length ?? _data!.length - _start!;
    return this;
  }

  void free() {
    _data = null;
    _start = null;
    _length = null;
  }

  bool get isEmpty => _length == 0;
  int? get length => _length;

  DataChunk extractDataChunk(int length, DataChunk reusableChunk) {
    length = min(_length!, length);
    var chunk = reusableChunk.reuse(_data!, _start!, length);
    _start = _start! + length;
    _length = _length! - length;
    return chunk;
  }

  int checkOneByte() => _data![_start!];

  int extractOneByte() {
    var value = _data![_start!];
    _start = _start! + 1;
    _length = _length! - 1;
    return value;
  }

  DataRange extractFixedLengthDataRange(int length, DataRange reusableRange) {
    DataRange range;
    if (length <= _length!) {
      range = reusableRange.reuse(_data!, _start!, length);
      _start = _start! + length;
      _length = _length! - length;
    } else {
      range = reusableRange.reusePending(_data!, _start!);
      _start = _start! + range.length!;
      _length = _length! - range.length!;
    }
    return range;
  }

  DataRange extractUpToDataRange(int terminator, DataRange reusableRange) {
    DataRange range;
    int i = _data!.indexOf(terminator, _start!);
    if (i != -1) {
      var length = i - _start!;
      range = reusableRange.reuse(_data!, _start!, length);
      // skip the terminator
      _start = _start! + length + 1;
      _length = _length! - length - 1;
    } else {
      range = reusableRange.reusePending(_data!, _start!);
      _start = _start! + range.length!;
      _length = _length! - range.length!;
    }
    return range;
  }
}
