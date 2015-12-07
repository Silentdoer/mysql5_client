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

  Future<ReaderBuffer> readBuffer(int length) {
    var value = _readBuffer(new ReaderBuffer(length));
    return value is Future ? value : new Future.value(value);
  }

  _readBuffer(ReaderBuffer buffer) {
    if (_chunks.isEmpty) {
      _dataReadyCompleter = new Completer();

      return _dataReadyCompleter.future
          .then((_) => _readBufferInternal(buffer));
    } else {
      return _readBufferInternal(buffer);
    }
  }

  _readBufferInternal(ReaderBuffer buffer) {
    var chunk = _chunks.first;

    buffer.loadChunk(chunk);

    if (chunk.isEmpty) {
      _chunks.removeFirst();
    }

    return buffer.isAllLoaded ? buffer : _readBuffer(buffer);
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
