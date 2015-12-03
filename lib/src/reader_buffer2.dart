// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_buffer;

import "data_range2.dart";
import "data_commons.dart";

class ReaderBuffer {
  static const String _EMPTY_STRING = "";

  // TODO capire se esiste una struttura più efficiente (DoubleLinkedList?)
  final List<DataRange> _dataRanges = [];

  int _payloadLength;
  int _loadedCount;

  ReaderBuffer() {
    _payloadLength = 0;
    _loadedCount = 0;
  }

  int get payloadLength => _payloadLength;

  int get leftLoadingCount => _payloadLength - _loadedCount;

  bool get isFirstByte => _loadedCount == 1;

  bool get isAvailable => leftLoadingCount > 0;

  void add(DataRange dataRange) {
    _payloadLength += dataRange.length;
    _dataRanges.add(dataRange);
  }

  void skipByte() {
    _readOneByte();
  }

  void skipBytes(int length) {
    _readFixedLengthDataRange(length);
  }

  int readOneLengthInteger() => _readOneByte();

  int readFixedLengthInteger(int length) =>
      length > 1 ? _readFixedLengthDataRange(length).toInt() : _readOneByte();

  int readLengthEncodedInteger() =>
      readFixedLengthInteger(readOneLengthInteger());

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

  String readRestOfPacketString() => readFixedLengthString(leftLoadingCount);

  String readRestOfPacketUTF8String() =>
      readFixedLengthUTF8String(leftLoadingCount);

  int _readOneByte() {
    var byte = _dataRanges[0].extractOneByte();
    if (_dataRanges[0].isEmpty) {
      _dataRanges.removeAt(0);
    }
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

      return new DataRange(data, 0, data.length);
    } else {
      return range;
    }
  }

  DataRange _readUpToDataRange(int terminator) {
    var range = _dataRanges[0].extractUpToDataRange(terminator);
    if (_dataRanges[0].isEmpty) {
      _dataRanges.removeAt(0);
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var data = new List();

      data.setRange(0, range.length, range.data, range.start);

      var start = range.length;
      do {
        range = _dataRanges[0].extractUpToDataRange(terminator);
        if (_dataRanges[0].isEmpty) {
          _dataRanges.removeAt(0);
        }
        var end = start + range.length;
        data.setRange(start, end, range.data, range.start);
        start = end;
      } while (range.isPending);

      return new DataRange(data, 0, data.length);
    } else {
      return range;
    }
  }
}
