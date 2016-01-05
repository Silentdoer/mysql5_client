// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_buffer;

import "data_chunk.dart";
import "data_range.dart";
import "data_commons.dart";

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

  DataChunk getReusableChunk(int index) {
    if (_chunks.length > index) {
      return _chunks[index];
    } else {
      var chunk = new DataChunk.reusable();
      _chunks.add(chunk);
      return chunk;
    }
  }

  int get payloadLength => _payloadLength;

  int get available => _payloadLength - _readCount;

  bool get isAllRead => available == 0;

  int checkByte() => _chunks[_chunkIndex].checkOneByte();

  int readByte() {
    var chunk = _chunks[_chunkIndex];
    var byte = chunk.extractOneByte();
    if (chunk.isEmpty) {
      chunk.free();
      _chunkIndex++;
    }
    _readCount++;
    return byte;
  }

  DataRange readFixedLengthDataRange(int length, DataRange reusableRange) {
    if (length > 0) {
      var chunk = _chunks[_chunkIndex];
      var range = chunk.extractFixedLengthDataRange(length, reusableRange);
      _readCount += range.length;
      if (chunk.isEmpty) {
        chunk.free();
        _chunkIndex++;
      }

      if (range.isPending) {
        var leftLength = length - range.length;
        DataRange range2;
        do {
          chunk = _chunks[_chunkIndex];
          range2 = chunk.extractFixedLengthDataRange(
              leftLength, new DataRange.reusable());
          _readCount += range2.length;
          if (chunk.isEmpty) {
            chunk.free();
            _chunkIndex++;
          }
          leftLength -= range2.length;
          range.addExtraRange(range2);
        } while (range2.isPending);
      }

      return range;
    } else {
      return reusableRange.reuseNil();
    }
  }

  DataRange readUpToDataRange(int terminator, DataRange reusableRange) {
    var chunk = _chunks[_chunkIndex];
    var range = chunk.extractUpToDataRange(terminator, reusableRange);
    _readCount += range.length;
    if (chunk.isEmpty) {
      chunk.free();
      _chunkIndex++;
    }

    if (range.isPending) {
      DataRange range2;
      do {
        chunk = _chunks[_chunkIndex];
        range2 =
            chunk.extractUpToDataRange(terminator, new DataRange.reusable());
        _readCount += range2.length;
        if (chunk.isEmpty) {
          chunk.free();
          _chunkIndex++;
        }
        range.addExtraRange(range2);
      } while (range2.isPending);
    }

    // skip the terminator
    _readCount++;
    return range;
  }
}
