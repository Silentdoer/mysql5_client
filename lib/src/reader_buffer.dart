// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_buffer;

import "data_chunk.dart";
import "data_range.dart";
import "data_commons.dart";
import 'dart:io';

class UndefinedError extends Error {
  final ReaderBuffer buffer;

  UndefinedError(this.buffer);

  String toString() => "Undefined value";
}

class EOFError extends Error {
  final ReaderBuffer buffer;

  EOFError(this.buffer);

  String toString() => "EOF value";
}

class ReaderBuffer {
  static final List<ReaderBuffer> _POOL = new List();

  final List<DataRange> _dataRanges = new List();

  int _payloadLength;
  int _loadedCount;
  int _readCount;

  factory ReaderBuffer(int payloadLength) {
    var buffer = _POOL.isNotEmpty ? _POOL.removeLast() : new ReaderBuffer._();

    buffer._initialize(payloadLength);

    return buffer;
  }

  ReaderBuffer._() {
    _loadedCount = 0;
    _readCount = 0;
  }

  void _initialize(int payloadLength) {
    _payloadLength = payloadLength;
  }

  void deinitialize() {
    for (var range in _dataRanges) {
      range.deinitialize();
    }
    _dataRanges.clear();
    _payloadLength = null;
    _loadedCount = 0;
    _readCount = 0;

    _POOL.add(this);
  }

  bool get isAllLoaded => _loadLeftCount == 0;

  bool get isAllRead => _readLeftCount == 0;

  bool get isFirstByte => _readCount == 1;

  int get _loadLeftCount => _payloadLength - _loadedCount;

  int get _readLeftCount => _payloadLength - _readCount;

  void loadChunk(DataChunk chunk) {
    chunk.consume(_loadLeftCount, (data, index, available) {
      var range = new DataRange(data, index, available);
      _loadedCount += available;
      _dataRanges.add(range);
    });
  }

  int checkByte() => _dataRanges[0].checkOneByte();

  void skipByte() {
    _readOneByte();
  }

  void skipBytes(int length) {
    _readFixedLengthDataRange(length).deinitialize();
  }

  int readOneLengthInteger() => _readOneByte();

  int readFixedLengthInteger(int length) {
    var range = _readFixedLengthDataRange(length);
    try {
      return range.toInt();
    } finally {
      range.deinitialize();
    }
  }

  int readLengthEncodedInteger() {
    var firstByte = _readOneByte();
    var bytesLength;
    switch (firstByte) {
      case PREFIX_INT_2:
        bytesLength = 3;
        break;
      case PREFIX_INT_3:
        bytesLength = 4;
        break;
      case PREFIX_INT_8:
        if (_readLeftCount >= 8) {
          bytesLength = 9;
        } else {
          throw new EOFError(this);
        }
        break;
      case PREFIX_NULL:
        throw new NullError();
      case PREFIX_UNDEFINED:
        throw new UndefinedError(this);
      default:
        return firstByte;
    }
    return readFixedLengthInteger(bytesLength - 1);
  }

  String readNulTerminatedString() {
    var range = _readUpToDataRange(NULL_TERMINATOR);
    try {
      return range.toString();
    } finally {
      range.deinitialize();
    }
  }

  String readNulTerminatedUTF8String() {
    var range = _readUpToDataRange(NULL_TERMINATOR);
    try {
      return range.toUTF8String();
    } finally {
      range.deinitialize();
    }
  }

  String readFixedLengthString(int length) {
    var range = _readFixedLengthDataRange(length);
    try {
      return range.toString();
    } finally {
      range.deinitialize();
    }
  }

  String readFixedLengthUTF8String(int length) {
    var range = _readFixedLengthDataRange(length);
    try {
      return range.toUTF8String();
    } finally {
      range.deinitialize();
    }
  }

  String readLengthEncodedString() =>
      readFixedLengthString(readLengthEncodedInteger());

  String readLengthEncodedUTF8String() =>
      readFixedLengthUTF8String(readLengthEncodedInteger());

  String readRestOfPacketString() => readFixedLengthString(_readLeftCount);

  String readRestOfPacketUTF8String() =>
      readFixedLengthUTF8String(_readLeftCount);

  int _readOneByte() {
    var byte = _dataRanges[0].extractOneByte();
    if (_dataRanges[0].isEmpty) {
      _dataRanges.removeAt(0).deinitialize();
    }
    _readCount++;
    return byte;
  }

  DataRange _readFixedLengthDataRange(int length) {
    var range = _dataRanges[0].extractFixedLengthDataRange(length);
    if (_dataRanges[0].isEmpty) {
      _dataRanges.removeAt(0).deinitialize();
    }

    var leftLength = length - range.length;
    if (range.isPending) {
      // devo costruire un range da zero
      var data = new List(length);
      data.setRange(0, range.length, range.data, range.start);
      var start = range.length;
      do {
        range.deinitialize();
        range = _dataRanges[0].extractFixedLengthDataRange(leftLength);
        if (_dataRanges[0].isEmpty) {
          _dataRanges.removeAt(0).deinitialize();
        }
        var end = start + range.length;
        data.setRange(start, end, range.data, range.start);
        start = end;
        leftLength -= range.length;
      } while (range.isPending);

      range.deinitialize();
      range = new DataRange(data);
    }
    _readCount += range.length;
    return range;
  }

  DataRange _readUpToDataRange(int terminator) {
    var range = _dataRanges[0].extractUpToDataRange(terminator);
    if (_dataRanges[0].isEmpty) {
      _dataRanges.removeAt(0).deinitialize();
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var builder = new BytesBuilder();
      builder.add(range.data.sublist(range.start, range.start + range.length));
      do {
        range.deinitialize();
        range = _dataRanges[0].extractUpToDataRange(terminator);
        if (_dataRanges[0].isEmpty) {
          _dataRanges.removeAt(0).deinitialize();
        }
        builder
            .add(range.data.sublist(range.start, range.start + range.length));
      } while (range.isPending);

      range.deinitialize();
      range = new DataRange(builder.takeBytes());
    }

    // salto il terminatore
    _readCount += range.length + 1;
    return range;
  }
}
