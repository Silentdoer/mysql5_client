// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_buffer;

import "data_chunk.dart";
import "data_range.dart";
import "data_commons.dart";
import 'dart:io';

class NullError extends Error {
  String toString() => "Null value";
}

class UndefinedError extends Error {
  String toString() => "Undefined value";
}

class EOFError extends Error {
  String toString() => "EOF value";
}

class ReaderBuffer {
  final List<DataRange> _dataRanges = new List();

  int _payloadLength;
  int _loadedCount;
  int _readCount;

  ReaderBuffer(int payloadLength) {
    _payloadLength = payloadLength;
    _loadedCount = 0;
    _readCount = 0;
  }

  int get payloadLength => _payloadLength;

  bool get isAllLoaded => _loadLeftCount == 0;

  bool get isAllRead => _readLeftCount == 0;

  int get first => _dataRanges[0].first;

  void loadChunk(DataChunk chunk) {
    chunk.consume(_loadLeftCount, (data, index, available) {
      var range = new DataRange(data, index, available);
      _loadedCount += available;
      _dataRanges.add(range);
    });
  }

  void skipByte() {
    _readOneByte();
  }

  void skipBytes(int length) {
    _readFixedLengthDataRange(length);
  }

  int readOneLengthInteger() => _readOneByte();

  int readFixedLengthInteger(int length) =>
      _readFixedLengthDataRange(length).toInt();

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
          throw new EOFError();
        }
        break;
      case PREFIX_NULL:
        throw new NullError();
      case PREFIX_UNDEFINED:
        throw new UndefinedError();
      default:
        return firstByte;
    }
    return readFixedLengthInteger(bytesLength - 1);
  }

  String readNulTerminatedString() =>
      _readUpToDataRange(NULL_TERMINATOR).toString();

  String readNulTerminatedUTF8String() =>
      _readUpToDataRange(NULL_TERMINATOR).toUTF8String();

  String readFixedLengthString(int length) =>
      _readFixedLengthDataRange(length).toString();

  String readFixedLengthUTF8String(int length) =>
      _readFixedLengthDataRange(length).toUTF8String();

  String readLengthEncodedString() =>
      readFixedLengthString(readLengthEncodedInteger());

  String readLengthEncodedUTF8String() =>
      readFixedLengthUTF8String(readLengthEncodedInteger());

  String readRestOfPacketString() => readFixedLengthString(_readLeftCount);

  String readRestOfPacketUTF8String() =>
      readFixedLengthUTF8String(_readLeftCount);

  int get _loadLeftCount => _payloadLength - _loadedCount;

  int get _readLeftCount => _payloadLength - _readCount;

  int _readOneByte() {
    var byte = _dataRanges[0].extractOneByte();
    if (_dataRanges[0].isEmpty) {
      _dataRanges.removeAt(0);
    }
    _readCount++;
    return byte;
  }

  DataRange _readFixedLengthDataRange(int length) {
    var range = _dataRanges[0].extractFixedLengthDataRange(length);
    if (_dataRanges[0].isEmpty) {
      _dataRanges.removeAt(0);
    }

    var leftLength = length - range.length;
    if (range.isPending) {
      // devo costruire un range da zero
      var data = new List(length);
      data.setRange(0, range.length, range.data, range.start);
      var start = range.length;
      do {
        range = _dataRanges[0].extractFixedLengthDataRange(leftLength);
        if (_dataRanges[0].isEmpty) {
          _dataRanges.removeAt(0);
        }
        var end = start + range.length;
        data.setRange(start, end, range.data, range.start);
        start = end;
        leftLength -= range.length;
      } while (range.isPending);

      range = new DataRange(data);
    }
    _readCount += range.length;
    return range;
  }

  DataRange _readUpToDataRange(int terminator) {
    var range = _dataRanges[0].extractUpToDataRange(terminator);
    if (_dataRanges[0].isEmpty) {
      _dataRanges.removeAt(0);
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var builder = new BytesBuilder();
      builder.add(range.data.sublist(range.start, range.start + range.length));
      do {
        range = _dataRanges[0].extractUpToDataRange(terminator);
        if (_dataRanges[0].isEmpty) {
          _dataRanges.removeAt(0);
        }
        builder
            .add(range.data.sublist(range.start, range.start + range.length));
      } while (range.isPending);

      range = new DataRange(builder.takeBytes());
    }

    // salto il terminatore
    _readCount += range.length + 1;
    return range;
  }
}
