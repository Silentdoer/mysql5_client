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
  final List<DataChunk> _chunks;
  final int _payloadLength;

  int _readCount;

  ReaderBuffer(this._chunks, this._payloadLength) {
    _readCount = 0;
  }

  int get payloadLength => _payloadLength;

  bool get isAllRead => _payloadLength == _readCount;

  void skipByte() {
    _readOneByte();
  }

  void skipBytes(int length) {
    _readFixedLengthDataRange(length);
  }

  int checkOneLengthInteger() => _chunks[0].checkOneByte();

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
        if (_payloadLength - _readCount >= 8) {
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

  String readRestOfPacketString() => readFixedLengthString(_payloadLength - _readCount);

  String readRestOfPacketUTF8String() =>
      readFixedLengthUTF8String(_payloadLength - _readCount);

  int _readOneByte() {
    var byte = _chunks[0].extractOneByte();
    if (_chunks[0].isEmpty) {
      _chunks.removeAt(0);
    }
    _readCount++;
    return byte;
  }

  DataRange _readFixedLengthDataRange(int length) {
    var range = _chunks[0].extractFixedLengthDataRange(length);
    if (_chunks[0].isEmpty) {
      _chunks.removeAt(0);
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var data = new List(length);
      data.setRange(0, range.length, range.data, range.start);
      var start = range.length;
      var leftLength = length - range.length;
      do {
        range = _chunks[0].extractFixedLengthDataRange(leftLength);
        if (_chunks[0].isEmpty) {
          _chunks.removeAt(0);
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
    var range = _chunks[0].extractUpToDataRange(terminator);
    if (_chunks[0].isEmpty) {
      _chunks.removeAt(0);
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var builder = new BytesBuilder();
      builder.add(range.data.sublist(range.start, range.start + range.length));
      do {
        range = _chunks[0].extractUpToDataRange(terminator);
        if (_chunks[0].isEmpty) {
          _chunks.removeAt(0);
        }
        builder
            .add(range.data.sublist(range.start, range.start + range.length));
      } while (range.isPending);

      range = new DataRange(builder.takeBytes());
    }

    // skip the terminator
    _readCount += range.length + 1;
    return range;
  }
}
