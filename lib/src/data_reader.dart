// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_reader;

import "dart:async";
import "dart:collection";

import "data_chunk.dart";
import "reader_buffer.dart";

class DataReader {
  final Queue<DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  DataReader(this._stream) {
    this._stream.listen(_onData);
  }

  readBuffer(int length) => _readBuffer(new List<DataChunk>(), length, length);

  _readBuffer(List<DataChunk> bufferChunks, int totalLength, int leftLength) {
    if (_chunks.isEmpty) {
      _dataReadyCompleter = new Completer();

      return _dataReadyCompleter.future
          .then((_) => _readBufferInternal(bufferChunks, totalLength, leftLength));
    } else {
      return _readBufferInternal(bufferChunks, totalLength, leftLength);
    }
  }

  _readBufferInternal(List<DataChunk> bufferChunks, int totalLength, int leftLength) {
    var chunk = _chunks.first;

    var bufferChunk = chunk.extractDataChunk(leftLength);
    bufferChunks.add(bufferChunk);
    leftLength -= bufferChunk.length;

    if (chunk.isEmpty) {
      _chunks.removeFirst();
    }

    return leftLength > 0
        ? _readBuffer(bufferChunks, totalLength, leftLength)
        : new ReaderBuffer(bufferChunks, totalLength);
  }

  void _onData(List<int> data) {
    _chunks.add(new DataChunk(data));

    if (_dataReadyCompleter != null) {
      var completer = _dataReadyCompleter;
      _dataReadyCompleter = null;
      completer.complete();
    }
  }
}
