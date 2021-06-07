// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

part of mysql_client.data;

class ReaderBuffer {
  List<DataChunk> _chunks;
  int? _dataLength;

  int? _chunkIndex;
  int? _readCount;

  ReaderBuffer(int reusableChunks, int dataLength)
      : this._chunks = List.empty(growable: true) {
    reuse(reusableChunks, dataLength);
  }

  ReaderBuffer.reusable() : this._chunks = List.empty(growable: true);

  ReaderBuffer reuse(int reusableChunks, int dataLength) {
    _dataLength = dataLength;
    _chunkIndex = 0;
    _readCount = 0;
    return this;
  }

  void free() {
    if (_chunkIndex != null) {
      for (int i = _chunkIndex!; i < _chunks.length; i++) {
        _chunks[i].free();
      }
      _dataLength = null;
      _chunkIndex = null;
      _readCount = null;
    }
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

  int? get dataLength => _dataLength;

  int? get readCount => _readCount;

  int get leftCount => _dataLength! - _readCount!;

  bool get isDataLeft => _readCount! < _dataLength!;

  bool get isNotDataLeft => _readCount == _dataLength;

  int checkByte() => _chunks[_chunkIndex!].checkOneByte();

  int readByte() {
    var chunk = _chunks[_chunkIndex!];
    var byte = chunk.extractOneByte();
    if (chunk.isEmpty) {
      chunk.free();
      _chunkIndex = _chunkIndex! + 1;
    }
    _readCount = _readCount! + 1;
    return byte;
  }

  DataRange readFixedLengthDataRange(int length, DataRange reusableRange) {
    if (length > 0) {
      var chunk = _chunks[_chunkIndex!];
      var range = chunk.extractFixedLengthDataRange(length, reusableRange);
      _readCount = _readCount! + range.length!;
      if (chunk.isEmpty) {
        chunk.free();
        _chunkIndex = _chunkIndex! + 1;
      }

      if (range.isPending!) {
        var leftLength = length - range.length!;
        DataRange range2;
        do {
          chunk = _chunks[_chunkIndex!];
          range2 = chunk.extractFixedLengthDataRange(
              leftLength, new DataRange.reusable());
          _readCount = _readCount! + range2.length!;
          if (chunk.isEmpty) {
            chunk.free();
            _chunkIndex = _chunkIndex! + 1;
          }
          leftLength -= range2.length!;
          range.addExtraRange(range2);
        } while (range2.isPending!);
      }

      return range;
    } else {
      return reusableRange.reuseNil();
    }
  }

  DataRange readUpToDataRange(int terminator, DataRange reusableRange) {
    var chunk = _chunks[_chunkIndex!];
    var range = chunk.extractUpToDataRange(terminator, reusableRange);
    _readCount = _readCount! + range.length!;
    if (chunk.isEmpty) {
      chunk.free();
      _chunkIndex = _chunkIndex! + 1;
    }

    if (range.isPending!) {
      DataRange range2;
      do {
        chunk = _chunks[_chunkIndex!];
        range2 =
            chunk.extractUpToDataRange(terminator, new DataRange.reusable());
        _readCount = _readCount! + range2.length!;
        if (chunk.isEmpty) {
          chunk.free();
          _chunkIndex = _chunkIndex! + 1;
        }
        range.addExtraRange(range2);
      } while (range2.isPending!);
    }

    // skip the terminator
    _readCount = _readCount! + 1;
    return range;
  }
}
