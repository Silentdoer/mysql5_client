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
  List<DataChunk> _chunks;
  int _payloadLength;

  int _chunkIndex;
  int _readCount;

  ReaderBuffer(this._chunks, this._payloadLength) {
    _chunkIndex = 0;
    _readCount = 0;
  }

  ReaderBuffer.reusable() : this._chunks = new List<DataChunk>();

  ReaderBuffer reuse(int reusableChunks, int payloadLength) {
    for (var i = reusableChunks; i < _chunks.length; i++) {
      _chunks[i].free();
    }
    _payloadLength = payloadLength;
    _chunkIndex = 0;
    _readCount = 0;
    return this;
  }

  void free() {
    for (var chunk in _chunks) {
      chunk.free();
    }
    _payloadLength = null;
    _chunkIndex = null;
    _readCount = null;
  }

  DataChunk getReusableDataChunk(int index) {
    if (_chunks.length > index) {
      return _chunks[index];
    } else {
      var chunk = new DataChunk.reusable();
      _chunks.add(chunk);
      return chunk;
    }
  }

  int get payloadLength => _payloadLength;

  bool get isAllRead => _payloadLength == _readCount;

  void skipByte() {
    _readOneByte();
  }

  void skipBytes(int length) {
    readFixedLengthDataRange(length);
  }

  int checkOneLengthInteger() => _chunks[_chunkIndex].checkOneByte();

  int readOneLengthInteger() => _readOneByte();

  int readFixedLengthInteger(int length) =>
      readFixedLengthDataRange(length).toInt();

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

  DataRange readNulTerminatedDataRange() => readUpToDataRange(NULL_TERMINATOR);

  String readNulTerminatedString() =>
      readUpToDataRange(NULL_TERMINATOR).toString();

  String readNulTerminatedUTF8String() =>
      readUpToDataRange(NULL_TERMINATOR).toUTF8String();

  String readFixedLengthString(int length) =>
      readFixedLengthDataRange(length).toString();

  String readFixedLengthUTF8String(int length) =>
      readFixedLengthDataRange(length).toUTF8String();

  DataRange readLengthEncodedDataRange() =>
      readFixedLengthDataRange(readLengthEncodedInteger());

  String readLengthEncodedString() =>
      readFixedLengthString(readLengthEncodedInteger());

  String readLengthEncodedUTF8String() =>
      readFixedLengthUTF8String(readLengthEncodedInteger());

  DataRange readRestOfPacketDataRange() =>
      readFixedLengthDataRange(_payloadLength - _readCount);

  String readRestOfPacketString() =>
      readFixedLengthString(_payloadLength - _readCount);

  String readRestOfPacketUTF8String() =>
      readFixedLengthUTF8String(_payloadLength - _readCount);

  DataRange readFixedLengthDataRange(int length) {
    var chunk = _chunks[_chunkIndex];
    var range = chunk.extractFixedLengthDataRange(length);
    if (chunk.isEmpty) {
      _chunkIndex++;
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var data = new List(length);
      data.setRange(0, range.length, range.data, range.start);
      var start = range.length;
      var leftLength = length - range.length;
      do {
        chunk = _chunks[_chunkIndex];
        range = chunk.extractFixedLengthDataRange(leftLength);
        if (chunk.isEmpty) {
          _chunkIndex++;
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

  DataRange readFixedLengthReusableDataRange(
      DataRange reusableRange, int length) {
    var chunk = _chunks[_chunkIndex];
    var range =
        chunk.extractFixedLengthReusableDataRange(reusableRange, length);
    if (chunk.isEmpty) {
      _chunkIndex++;
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var data = new List(length);
      data.setRange(0, range.length, range.data, range.start);
      var start = range.length;
      var leftLength = length - range.length;
      do {
        chunk = _chunks[_chunkIndex];
        range = chunk.extractFixedLengthReusableDataRange(
            reusableRange, leftLength);
        if (chunk.isEmpty) {
          _chunkIndex++;
        }
        var end = start + range.length;
        data.setRange(start, end, range.data, range.start);
        start = end;
        leftLength -= range.length;
      } while (range.isPending);

      range = reusableRange.reuse(data);
    }

    _readCount += range.length;

    return range;
  }

  DataRange readUpToDataRange(int terminator) {
    var chunk = _chunks[_chunkIndex];
    var range = chunk.extractUpToDataRange(terminator);
    if (chunk.isEmpty) {
      _chunks.removeAt(0);
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var builder = new BytesBuilder();
      builder.add(range.data.sublist(range.start, range.start + range.length));
      do {
        chunk = _chunks[_chunkIndex];
        range = chunk.extractUpToDataRange(terminator);
        if (chunk.isEmpty) {
          _chunkIndex++;
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

  DataRange readUpToReusableDataRange(DataRange reusableRange, int terminator) {
    var chunk = _chunks[_chunkIndex];
    var range = chunk.extractUpToReusableDataRange(reusableRange, terminator);
    if (chunk.isEmpty) {
      _chunks.removeAt(0);
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var builder = new BytesBuilder();
      builder.add(range.data.sublist(range.start, range.start + range.length));
      do {
        chunk = _chunks[_chunkIndex];
        range = chunk.extractUpToReusableDataRange(reusableRange, terminator);
        if (chunk.isEmpty) {
          _chunkIndex++;
        }
        builder
            .add(range.data.sublist(range.start, range.start + range.length));
      } while (range.isPending);

      range = reusableRange.reuse(builder.takeBytes());
    }

    // skip the terminator
    _readCount += range.length + 1;
    return range;
  }

  int _readOneByte() {
    var chunk = _chunks[_chunkIndex];
    var byte = chunk.extractOneByte();
    if (chunk.isEmpty) {
      _chunkIndex++;
    }
    _readCount++;
    return byte;
  }
}
