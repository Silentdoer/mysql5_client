// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_reader;

import "dart:async";
import "dart:collection";

import "package:mysql_client/src/data_chunk.dart";
import "package:mysql_client/src/reader_buffer.dart";

class DataReader {
  final Queue<DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  DataReader(this._stream) {
    this._stream.listen(_onData);
  }

  readBuffer(int length, ReaderBuffer reusableBuffer) =>
      _readBuffer(0, length, length, reusableBuffer);

  _readBuffer(int reusableChunks, int totalLength, int leftLength,
      ReaderBuffer reusableBuffer) {
    if (_chunks.isEmpty) {
      _dataReadyCompleter = new Completer();

      return _dataReadyCompleter.future
          .then((_) => _dataReadyCompleter = null)
          .then((_) => _readBufferInternal(
              reusableChunks, totalLength, leftLength, reusableBuffer));
    } else {
      return _readBufferInternal(
          reusableChunks, totalLength, leftLength, reusableBuffer);
    }
  }

  _readBufferInternal(int reusableChunks, int totalLength, int leftLength,
      ReaderBuffer reusableBuffer) {
    var chunk = _chunks.first;

    var reusableChunk = reusableBuffer.getReusableChunk(reusableChunks);
    var bufferChunk = chunk.extractDataChunk(leftLength, reusableChunk);
    reusableChunks++;
    leftLength -= bufferChunk.length;

    if (chunk.isEmpty) {
      _chunks.removeFirst();
    }

    return leftLength > 0
        ? _readBuffer(reusableChunks, totalLength, leftLength, reusableBuffer)
        : reusableBuffer.reuse(reusableChunks, totalLength);
  }

  void _onData(List<int> data) {
    _chunks.add(new DataChunk(data));
    _dataReadyCompleter?.complete();
  }
}
