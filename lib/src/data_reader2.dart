// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_reader;

import "dart:async";
import "dart:collection";

import "data_chunk2.dart";
import "reader_buffer2.dart";

class NullError extends Error {
  String toString() => "Null value";
}

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

class DataReader {
  // TODO capire se esiste una struttura più efficiente (DoubleLinkedList?)
  final Queue<DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  final ReaderBuffer _dataBuffer;

  DataReader(this._stream) : this._dataBuffer = new ReaderBuffer() {
    this._stream.listen(_onData);
  }

  Future<ReaderBuffer> readBuffer(int length) async {
    _dataBuffer.clean();
    var value = _readBuffer(length);
    return value is Future ? value : new Future.value(value);
  }

  _readBuffer(int leftLength) {
    var value = _readChunk((chunk) {
      _dataBuffer.add(chunk.readRange(leftLength));
    });
    return value is Future
        ? value.then(_readBufferInternal(leftLength))
        : _readBufferInternal(leftLength);
  }

  _readBufferInternal(int leftLength) {
    var range = _dataBuffer.lastRange;
    if (range.isPending) {
      return _readBuffer(leftLength - range.length);
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
}
