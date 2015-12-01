// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_buffer;

import "dart:convert";

import "data_range.dart";

class DataBuffer {
  static const String _EMPTY_STRING = "";

  final List<DataRange> _dataRanges = [];

  DataRange _cachedDataRange;
  List<int> _cachedData;
  int _cachedLength;

  void clean() {
    for (var range in _dataRanges) {
      range.close();
    }
    _dataRanges.clear();

    if (_cachedDataRange != null) {
      if (_cachedDataRange.isInitialized) {
        _cachedDataRange.close();
      }
      _cachedDataRange = null;
    }
    _cachedData = null;
    _cachedLength = null;
  }

  DataRange get lastRange => _dataRanges.last;

  void add(DataRange dataRange) {
    _dataRanges.add(dataRange);
  }

  int get length {
    if (_cachedLength == null) {
      _cachedLength = 0;
      for (var range in _dataRanges) {
        _cachedLength += range.length;
      }
    }
    return _cachedLength;
  }

  DataRange get singleRange {
    if (_cachedDataRange == null) {
      if (_dataRanges.length > 1) {
        _cachedDataRange = new DataRange(data);
      } else if (_dataRanges.length == 1) {
        _cachedDataRange = _dataRanges[0];
      }
    }
    return _cachedDataRange;
  }

  List<int> get data {
    if (_cachedData == null) {
      _cachedData = new List(this.length);
      var start = 0;
      this._dataRanges.forEach((range) {
        var end = start + range.length;
        _cachedData.setRange(start, end, range.data, range.start);
        start = end;
      });
    }
    return _cachedData;
  }

  int toInt() {
    var data = this.singleRange.data;
    var i = this.singleRange.start;
    switch (this.singleRange.length) {
      case 1:
        return data[i++];
      case 2:
        return data[i++] | data[i++] << 8;
      case 3:
        return data[i++] | data[i++] << 8 | data[i++] << 16;
      case 4:
        return data[i++] | data[i++] << 8 | data[i++] << 16 | data[i++] << 24;
      case 5:
        return data[i++] |
            data[i++] << 8 |
            data[i++] << 16 |
            data[i++] << 24 |
            data[i++] << 32;
      case 6:
        return data[i++] |
            data[i++] << 8 |
            data[i++] << 16 |
            data[i++] << 24 |
            data[i++] << 32 |
            data[i++] << 40;
      case 7:
        return data[i++] |
            data[i++] << 8 |
            data[i++] << 16 |
            data[i++] << 24 |
            data[i++] << 32 |
            data[i++] << 40 |
            data[i++] << 48;
      case 8:
        return data[i++] |
            data[i++] << 8 |
            data[i++] << 16 |
            data[i++] << 24 |
            data[i++] << 32 |
            data[i++] << 40 |
            data[i++] << 48 |
            data[i++] << 56;
    }

    throw new UnsupportedError("${data.length} length");
  }

  String toString() => this.singleRange != null
      ? new String.fromCharCodes(
          this.singleRange.data, this.singleRange.start, this.singleRange.end)
      : DataBuffer._EMPTY_STRING;

  String toUTF8() => this.singleRange != null
      ? UTF8.decoder.convert(
          this.singleRange.data, this.singleRange.start, this.singleRange.end)
      : DataBuffer._EMPTY_STRING;
}
