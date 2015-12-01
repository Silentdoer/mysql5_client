// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.writer_buffer;

import "dart:convert";

import "data_commons.dart";

class WriterBuffer {
  final List<int> _data = [];

  int get length => _data.length;

  List<int> get data => _data;

  void writeBuffer(WriterBuffer buffer) {
    data.addAll(buffer.data);
  }

  void writeOneLengthInteger(int value) {
    _data.add(value);
  }

  void writeFixedLengthInteger(int value, int length) {
    for (var i = 0, rotation = 0, mask = 0xff;
        i < length;
        i++, rotation += 8, mask <<= 8) {
      _data.add((value & mask) >> rotation);
    }
  }

  void writeFixedFilledLengthString(int value, int length) {
    _data.addAll(new List.filled(length, value));
  }

  void writeFixedLengthString(String value, [int length]) {
    int start = _data.length;

    _data.addAll(value.codeUnits);

    if (length != null && _data.length - start != length) {
      throw new ArgumentError("${_data.length - start} != $length");
    }
  }

  void writeFixedLengthUTF8String(String value, [int length]) {
    int start = _data.length;

    _data.addAll(UTF8.encoder.convert(value));

    if (length != null && _data.length - start != length) {
      throw new ArgumentError("${_data.length - start} != $length");
    }
  }

  void writeLengthEncodedInteger(int value) {
    if (value < MAX_INT_1) {
      _data.add(value);
    } else {
      var bytesLength;
      if (value < MAX_INT_2) {
        bytesLength = 2;
        _data.add(PREFIX_INT_2);
      } else if (value < MAX_INT_3) {
        bytesLength = 3;
        _data.add(PREFIX_INT_3);
      } else if (value < MAX_INT_8) {
        bytesLength = 8;
        _data.add(PREFIX_INT_8);
      } else {
        throw new UnsupportedError("Undefined value");
      }

      writeFixedLengthInteger(value, bytesLength);
    }
  }

  void writeLengthEncodedString(String value) {
    writeLengthEncodedInteger(value.length);

    _data.addAll(value.codeUnits);
  }

  void writeLengthEncodedUTF8String(String value) {
    var encoded = UTF8.encoder.convert(value);

    writeLengthEncodedInteger(encoded.length);

    _data.addAll(encoded);
  }

  void writeNulTerminatedString(String value) {
    _data.addAll(value.codeUnits);

    _data.add(0x00);
  }

  void writeNulTerminatedUTF8String(String value) {
    _data.addAll(UTF8.encoder.convert(value));

    _data.add(0x00);
  }
}
