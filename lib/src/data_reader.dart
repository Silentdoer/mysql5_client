// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_reader;

import "dart:async";
import "dart:collection";

import "data_chunk.dart";
import "reader_buffer.dart";

class DataReader {
  // TODO capire se esiste una struttura più efficiente (DoubleLinkedList?)
  final Queue<DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  DataReader(this._stream) {
    this._stream.listen(_onData);
  }

  Future<ReaderBuffer> readBuffer(int length) async {
    var value = _readBuffer(new ReaderBuffer(length));
    return value is Future ? value : new Future.value(value);
  }

  _readBuffer(ReaderBuffer buffer) {
    var value = _readChunk((chunk) => buffer.loadChunk(chunk));
    return value is Future
        ? value.then((_) => _readBufferInternal(buffer))
        : _readBufferInternal(buffer);
  }

  _readBufferInternal(ReaderBuffer buffer) {
    return buffer.isAllLoaded ? buffer : _readBuffer(buffer);
  }

  _readChunk(chunkReader(DataChunk chunk)) {
    if (_chunks.isEmpty) {
      _dataReadyCompleter = new Completer();
      return _dataReadyCompleter.future
          .then((_) => _readChunkInternal(chunkReader));
    } else {
      return _readChunkInternal(chunkReader);
    }
  }

  _readChunkInternal(chunkReader(DataChunk chunk)) {
    var chunk = _chunks.first;
    try {
      return chunkReader(chunk);
    } finally {
      if (chunk.isEmpty) {
        _chunks.removeFirst();
      }
    }
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
