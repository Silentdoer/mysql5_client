// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_reader;

import "dart:async";
import "dart:collection";

import "data_chunk.dart";
import "data_range.dart";
import "reader_buffer.dart";

class DataReader {
  final Queue<DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  DataReader(this._stream) {
    this._stream.listen(_onData);
  }

  readBuffer(int length) => _readBuffer(new List<DataRange>(), length, length);

  _readBuffer(List<DataRange> ranges, int totalLength, int leftLength) {
    if (_chunks.isEmpty) {
      _dataReadyCompleter = new Completer();

      return _dataReadyCompleter.future
          .then((_) => _readBufferInternal(ranges, totalLength, leftLength));
    } else {
      return _readBufferInternal(ranges, totalLength, leftLength);
    }
  }

  _readBufferInternal(List<DataRange> ranges, int totalLength, int leftLength) {
    var chunk = _chunks.first;

    var range = chunk.extractDataRange(leftLength);
    ranges.add(range);
    leftLength -= range.length;

    if (chunk.isEmpty) {
      _chunks.removeFirst();
    }

    return leftLength > 0
        ? _readBuffer(ranges, totalLength, leftLength)
        : new ReaderBuffer(ranges, totalLength);
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
