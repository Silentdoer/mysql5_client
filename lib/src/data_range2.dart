// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_range;

import "dart:convert";

class DataRange {
  static final List<DataRange> _POOL = new List();

  List<int> _data;
  int _start;
  int _length;
  bool _isPending;

  factory DataRange(List<int> data, [int start = 0, int length]) {
    var range = _POOL.isNotEmpty ? _POOL.removeLast() : new DataRange._();

    range._open(data, start, length);

    return range;
  }

  DataRange._();

  bool get isInitialized => _data != null;

  void _open(List<int> data, [int start = 0, int length]) {
    this._data = data;
    this._start = start;
    if (length == null) {
      length = this._data.length - this._start;
    }
    if (this._start + length <= this._data.length) {
      this._isPending = false;
      this._length = length;
    } else {
      this._isPending = true;
      this._length = this._data.length - this._start;
    }
  }

  void close() {
    _data = null;
    _start = null;
    _length = null;
    _isPending = null;

    _POOL.add(this);
  }

  bool get isEmpty => _length == 0;
  int get start => _start;
  int get end => _start + _length;
  int get length => _length;
  bool get isPending => _isPending;
  List<int> get data => _data;

  int extractOneByte() {
    var value = _data[_start];
    _start++;
    _length--;
    return value;
  }

  DataRange extractFixedLengthDataRange(int length) {
    throw new UnsupportedError("TOIMPLEMENT");
  }

  DataRange extractUpToDataRange(int terminator) {
    throw new UnsupportedError("TOIMPLEMENT");
  }

  int toInt() {
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

  String toString() =>
      new String.fromCharCodes(_data, _start, _start + _length);

  String toUTF8String() =>
      UTF8.decoder.convert(_data, _start, _start + _length);
}
