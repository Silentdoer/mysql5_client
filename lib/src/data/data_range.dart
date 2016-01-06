// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_range;

class DataRange {
  bool _isPending;
  List<int> _data;
  int _start;
  int _length;
  List<DataRange> _extraRanges;
  int _mergeLength;

  DataRange(List<int> data, [int start = 0, int length]) {
    reuse(data, start, length);
  }

  DataRange.byte(int byte) {
    reuseByte(byte);
  }

  DataRange.nil() {
    reuseNil();
  }

  DataRange.pending(List<int> data, [int start = 0]) {
    reusePending(data, start);
  }

  DataRange.reusable();

  int get byteValue => _length;

  List<int> get data => _data;

  int get end => _start + _length;

  bool get isByte => _data == null;

  bool get isNil => _data == null;

  bool get isPending => _isPending;
  int get length => _length;
  int get start => _start;
  void addExtraRange(DataRange extraRange) {
    if (_extraRanges == null) {
      _extraRanges = new List<DataRange>();
      _mergeLength = _length;
    }
    _extraRanges.add(extraRange);
    _mergeLength += extraRange._length;
  }

  void free() {
    _isPending = null;
    _data = null;
    _start = null;
    _length = null;
    _extraRanges = null;
    _mergeLength = null;
  }

  void mergeExtraRanges() {
    if (_extraRanges != null) {
      var range = this;
      var start = 0;
      var end = range.length;
      var newData = new List(_mergeLength);
      newData.setRange(start, end, range._data, range._start);
      start = end;

      for (range in _extraRanges) {
        end = start + range.length;
        newData.setRange(start, end, range._data, range._start);
        start = end;
      }

      _isPending = false;
      _data = newData;
      _start = 0;
      _length = _mergeLength;
      _extraRanges = null;
      _mergeLength = null;
    }
  }

  DataRange reuse(List<int> data, [int start = 0, int length]) {
    _isPending = false;
    _data = data;
    _start = start;
    _length = length ?? _data.length - _start;
    _extraRanges = null;
    _mergeLength = null;
    return this;
  }

  DataRange reuseByte(int byte) {
    _isPending = null;
    _data = null;
    _start = null;
    _length = byte;
    _extraRanges = null;
    _mergeLength = null;
    return this;
  }

  DataRange reuseNil() {
    _isPending = null;
    _data = null;
    _start = null;
    _length = null;
    _extraRanges = null;
    _mergeLength = null;
    return this;
  }

  DataRange reusePending(List<int> data, [int start = 0]) {
    _isPending = true;
    _data = data;
    _start = start;
    _length = _data.length - _start;
    _extraRanges = null;
    _mergeLength = null;
    return this;
  }
}
