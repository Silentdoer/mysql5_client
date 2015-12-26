// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_reader;

import "dart:async";
import "dart:collection";

import "package:mysql_client/src/data_chunk.dart";
import "package:mysql_client/src/reader_buffer.dart";

class DataReader {
  final ReaderBuffer _reusableBuffer = new ReaderBuffer.reusable();

  final Queue<DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  DataReader(this._stream) {
    this._stream.listen(_onData);
  }

  readBuffer(int length) => _readBuffer(0, length, length);

  _readBuffer(int reusableChunksCount, int totalLength, int leftLength) {
    if (_chunks.isEmpty) {
      _dataReadyCompleter = new Completer();

      return _dataReadyCompleter.future
          .then((_) => _dataReadyCompleter = null)
          .then((_) => _readBufferInternal(
              reusableChunksCount, totalLength, leftLength));
    } else {
      return _readBufferInternal(reusableChunksCount, totalLength, leftLength);
    }
  }

  _readBufferInternal(
      int reusableChunksCount, int totalLength, int leftLength) {
    var chunk = _chunks.first;

    var reusableChunk = _reusableBuffer.getReusableChunk(reusableChunksCount);
    var bufferChunk = chunk.extractDataChunk(leftLength, reusableChunk);
    reusableChunksCount++;
    leftLength -= bufferChunk.length;

    if (chunk.isEmpty) {
      _chunks.removeFirst();
    }

    return leftLength > 0
        ? _readBuffer(reusableChunksCount, totalLength, leftLength)
        : _reusableBuffer.reuse(reusableChunksCount, totalLength);
  }

  void _onData(List<int> data) {
    _chunks.add(new DataChunk(data));
    _dataReadyCompleter?.complete();
  }
}
