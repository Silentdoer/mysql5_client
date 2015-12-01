// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_range;

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

  int get start => _start;
  int get end => _start + _length;
  int get length => _length;
  List<int> get data => _data;
  bool get isPending => _isPending;
}