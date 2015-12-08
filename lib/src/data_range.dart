// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_range2;

import "dart:convert";

class DataRange {
  final bool isPending;
  final List<int> data;
  final int start;
  int _length;

  DataRange(this.data, [this.start = 0, this._length])
      : this.isPending = false {
    this._length ??= data.length - start;
  }

  DataRange.pending(this.data, [this.start = 0]) : this.isPending = true {
    this._length = data.length - start;
  }

  int get length => _length;

  int toInt() {
    var i = start;
    switch (_length) {
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

  String toString() => new String.fromCharCodes(data, start, start + _length);

  String toUTF8String() => UTF8.decoder.convert(data, start, start + _length);
}
