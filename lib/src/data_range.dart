// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_range2;

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

  void addExtraRange(DataRange extraRange) {
    if (_extraRanges == null) {
      _extraRanges = new List<DataRange>();
      _mergeLength = _length;
    }
    _extraRanges.add(extraRange);
    _mergeLength += extraRange._length;
  }

  int toInt() {
    if (_data == null) {
      return _length;
    }

    _mergeExtraRanges();

    var i = _start;
    switch (_length) {
      case 1:
        return _data[i++];
      case 2:
        return _data[i++] | _data[i++] << 8;
      case 3:
        return _data[i++] | _data[i++] << 8 | _data[i++] << 16;
      case 4:
        return _data[i++] |
            _data[i++] << 8 |
            _data[i++] << 16 |
            _data[i++] << 24;
      case 5:
        return _data[i++] |
            _data[i++] << 8 |
            _data[i++] << 16 |
            _data[i++] << 24 |
            _data[i++] << 32;
      case 6:
        return _data[i++] |
            _data[i++] << 8 |
            _data[i++] << 16 |
            _data[i++] << 24 |
            _data[i++] << 32 |
            _data[i++] << 40;
      case 7:
        return _data[i++] |
            _data[i++] << 8 |
            _data[i++] << 16 |
            _data[i++] << 24 |
            _data[i++] << 32 |
            _data[i++] << 40 |
            _data[i++] << 48;
      case 8:
        return _data[i++] |
            _data[i++] << 8 |
            _data[i++] << 16 |
            _data[i++] << 24 |
            _data[i++] << 32 |
            _data[i++] << 40 |
            _data[i++] << 48 |
            _data[i++] << 56;
    }

    throw new UnsupportedError("${_data.length} length");
  }

  String toString() {
    if (_data != null) {
      _mergeExtraRanges();

      return new String.fromCharCodes(_data, _start, _start + _length);
    } else {
      return null;
    }
  }

  String toUTF8String() {
    if (_data != null) {
      _mergeExtraRanges();

      return UTF8.decoder.convert(_data, _start, _start + _length);
    } else {
      return null;
    }
  }

  void _mergeExtraRanges() {
    if (_extraRanges != null) {
      var range = this;

      var data = new List(_mergeLength);
      data.setRange(0, range.length, range.data, range.start);
      var start = range.length;
      var leftLength = _mergeLength - range.length;

      do {
        var end = start + _length;
        data.setRange(start, end, range.data, range.start);
        start = end;
        leftLength -= range.length;
      } while (leftLength > 0);

      _isPending = false;
      _data = data;
      _start = 0;
      _length = _mergeLength;
      _extraRanges = null;
      _mergeLength = null;
    }
  }
}
