// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_range;

import "dart:typed_data";
import "dart:convert";

class DataRange {
  bool _isPending;
  List<int> _data;
  int _start;
  int _length;
  List<DataRange> _extraRanges;
  int _mergeLength;

  DataRange(this._data, [this._start = 0, this._length])
      : this._isPending = false {
    _length ??= _data.length - _start;
  }

  DataRange.pending(this._data, [this._start = 0]) : this._isPending = true {
    _length = _data.length - _start;
  }

  DataRange.nil();

  DataRange.byte(int byte) : this._length = byte;

  DataRange.reusable();

  DataRange reuse(List<int> data, [int start = 0, int length]) {
    _isPending = false;
    _data = data;
    _start = start;
    _length = length ?? _data.length - _start;
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

  void free() {
    _isPending = null;
    _data = null;
    _start = null;
    _length = null;
    _extraRanges = null;
    _mergeLength = null;
  }

  bool get isPending => _isPending;
  List<int> get data => _data;
  int get start => _start;
  int get length => _length;
  int get end => _start + _length;
  bool get isByte => _data == null;
  int get byteValue => _length;

  void addExtraRange(DataRange extraRange) {
    if (_extraRanges == null) {
      _extraRanges = new List<DataRange>();
      _mergeLength = _length;
    }
    _extraRanges.add(extraRange);
    _mergeLength += extraRange._length;
  }

  DataRange mergeExtras() {
    if (_data != null) {
      _mergeExtraRanges();

      return this;
    } else {
      return null;
    }
  }

  void _mergeExtraRanges() {
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
}
