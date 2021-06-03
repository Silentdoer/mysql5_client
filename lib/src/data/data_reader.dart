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
    // 这里的_socket是一直在使用的，每次executeQuery
    // 用的都是这个_socket来发送数据
    // 所以目前是说这一次发送了数据，但是服务端
    // 没有响应？
    _subscription = this._socket.listen(_onData);
    _subscription.pause();
  }

  readBuffer(int length) => _readBuffer(0, length, length);

  _readBuffer(int reusableChunksCount, int totalLength, int leftLength) {
    //print('333-k-111m');
    if (_chunks.isEmpty) {
      // here
      //print('333-k-nnn');
      // TODO 测试一下await它的future，然后c.isCompleted是不是就是true？
      // 是的话，后面的_onData里不仅仅要判断是否是null，还得判断是否已经完成
      _dataRequestCompleter = new Completer();
      _subscription.resume();
      //print('333-0000');
      return _dataRequestCompleter!.future.then((readLength) {
        // 没有执行这里，说明是Completer的future卡主了
        // 所以是某个地方没有给_dataRequestCompleter写数据了？
        //print('3330-kkk');
        _subscription.pause();
        //print('3331-kkk');
        _dataRequestCompleter = null;

        return _readBufferInternal(
            reusableChunksCount, totalLength, leftLength);
      });
    } else {
      //print('333-k-mmm');
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
    // 这个都没有执行，说明是_onData一直没有被调用
    // 这么一看都好像是无解了。。
    // 因为是dart内部的socket没有被触发获得了数据
    //print('bbbbb-ut');
    if (event == RawSocketEvent.read && _dataRequestCompleter != null) {
      _chunks.add(new DataChunk(_socket.read(maxChunkSize)!));
      //print('ssss-ut');
      _dataRequestCompleter!.complete();
      //print('ssss-uu');
    }
  }
}
