// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

part of mysql_client.data;

class DataReader {

  // TODO questo valore si potrebbe modificare a runtime
  final int? maxChunkSize;

  final ReaderBuffer _reusableBuffer = new ReaderBuffer.reusable();

  final Queue<DataChunk> _chunks = new Queue();

  final RawSocket _socket;

  Completer? _dataRequestCompleter;

  late StreamSubscription<RawSocketEvent> _subscription;

  DataReader(this._socket, {this.maxChunkSize}) {
    _subscription = this._socket.listen(_onData);
    _subscription.pause();
  }

  readBuffer(int length) => _readBuffer(0, length, length);

  _readBuffer(int reusableChunksCount, int totalLength, int leftLength) {
    if (_chunks.isEmpty) {
      _dataRequestCompleter = new Completer();
      _subscription.resume();

      return _dataRequestCompleter!.future.then((readLength) {
        _subscription.pause();
        _dataRequestCompleter = null;

        return _readBufferInternal(
            reusableChunksCount, totalLength, leftLength);
      });
    } else {
      return _readBufferInternal(reusableChunksCount, totalLength, leftLength);
    }
  }

  _readBufferInternal(
      int reusableChunksCount, int totalLength, int leftLength) {
    var chunk = _chunks.first;

    var reusableChunk = _reusableBuffer.getReusableChunk(reusableChunksCount);
    reusableChunksCount++;
    var bufferChunk = chunk.extractDataChunk(leftLength, reusableChunk);
    leftLength -= bufferChunk.length!;

    if (chunk.isEmpty) {
      _chunks.removeFirst();
    }

    return leftLength > 0
        ? _readBuffer(reusableChunksCount, totalLength, leftLength)
        : _reusableBuffer.reuse(reusableChunksCount, totalLength);
  }

  void _onData(RawSocketEvent event) {
    if (event == RawSocketEvent.read && _dataRequestCompleter != null) {
      _chunks.add(new DataChunk(_socket.read(maxChunkSize)!));

      _dataRequestCompleter!.complete();
    }
  }
}
