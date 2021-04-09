// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

part of mysql_client.data;

class DataRange {
  bool? _isPending;
  List<int>? _data;
  int? _start;
  int? _length;
  List<DataRange>? _extraRanges;
  int? _mergeLength;

  DataRange(List<int> data, [int start = 0, int? length]) {
    reuse(data, start, length);
  }

  DataRange.pending(List<int> data, [int start = 0]) {
    reusePending(data, start);
  }

  DataRange.nil() {
    reuseNil();
  }

  DataRange.byte(int byte) {
    reuseByte(byte);
  }

  DataRange.reusable();

  DataRange reuse(List<int> data, [int start = 0, int? length]) {
    _isPending = false;
    _data = data;
    _start = start;
    _length = length ?? _data!.length - _start!;
    _extraRanges = null;
    _mergeLength = null;
    return this;
  }

  DataRange reusePending(List<int> data, [int start = 0]) {
    _isPending = true;
    _data = data;
    _start = start;
    _length = _data!.length - _start!;
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

  bool? get isPending => _isPending;
  bool get isNil => _data == null;
  bool get isByte => _data == null;
  int? get byteValue => _length;
  int? get start => _start;
  int? get length => _length;
  int get end => _start! + _length!;
  List<int>? get data => _data;

  void addExtraRange(DataRange extraRange) {
    if (_extraRanges == null) {
      _extraRanges = List.empty(growable: true);
      _mergeLength = _length;
    }
    _extraRanges!.add(extraRange);
    _mergeLength = _mergeLength! + extraRange._length!;
  }

  void mergeExtraRanges() {
    if (_extraRanges != null) {
      var range = this;
      var start = 0;
      var end = range.length!;
      var newData = List.filled(_mergeLength!, 0);
      newData.setRange(start, end, range._data!, range._start!);
      start = end;

      for (range in _extraRanges!) {
        end = start + range.length!;
        newData.setRange(start, end, range._data!, range._start!);
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
