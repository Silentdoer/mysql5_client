// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_buffer;

import "data_chunk.dart";
import "data_range.dart";
import "data_commons.dart";
import "data_statistics.dart";

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
  // TODO capire se esiste una struttura più efficiente
  static final List<ReaderBuffer> _POOL = new List();

  // TODO capire se esiste una struttura più efficiente (DoubleLinkedList?)
  final List<DataRange> _dataRanges = [];

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
    BUFFER_COUNTER++;
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

  bool get isAllLoaded => _loadedCount < _payloadLength;

  bool get isAllRead => _readCount == _payloadLength;

  bool get isFirstByte => _readCount == 1;

  void loadChunk(DataChunk chunk) {
    chunk.consume(_payloadLength - _loadedCount, (data, index, available) {
      var range = new DataRange(data, index, available);
      _loadedCount += available;
      _dataRanges.add(range);
    });
  }

  void skipByte() {
    _readOneByte();
  }

  void skipBytes(int length) {
    _readFixedLengthDataRange(length).deinitialize();
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
        if (_payloadLength - _readCount >= 8) {
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
    return _readFixedLengthDataRange(bytesLength - 1).toInt();
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

  String readRestOfPacketString() =>
      readFixedLengthString(_payloadLength - _readCount);

  String readRestOfPacketUTF8String() =>
      readFixedLengthUTF8String(_payloadLength - _readCount);

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
      LIST1_COUNTER++;
      var data = new List(length);

      data.setRange(0, range.length, range.data, range.start);

      var start = range.length;
      do {
        range = _dataRanges[0].extractFixedLengthDataRange(leftLength);
        if (_dataRanges[0].isEmpty) {
          _dataRanges.removeAt(0).deinitialize();
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
      _dataRanges.removeAt(0).deinitialize();
    }

    if (range.isPending) {
      // devo costruire un range da zero
      LIST2_COUNTER++;
      var data = new List();
      SUBLIST_COUNTER++;
      data.addAll(range.data.sublist(range.start, range.start + range.length));
      do {
        range = _dataRanges[0].extractUpToDataRange(terminator);
        if (_dataRanges[0].isEmpty) {
          _dataRanges.removeAt(0).deinitialize();
        }
        SUBLIST_COUNTER++;
        data.addAll(
            range.data.sublist(range.start, range.start + range.length));
      } while (range.isPending);

      range = new DataRange(data);
    }

    // salto il terminatore
    _readCount += range.length + 1;
    return range;
  }
}
