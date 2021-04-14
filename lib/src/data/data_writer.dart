// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

part of mysql_client.data;

class DataWriter {
  final RawSocket _socket;

  DataWriter(this._socket);

  void writeBuffer(List<int> buffer) {
    var writeCount = 0;
    var successCount = _socket.write(buffer);
    if (successCount != buffer.length) {
      print('写入数据：${successCount}  ${buffer.length}  ${this._socket.available()}');
      writeCount++;
      while(true) {
        successCount = _socket.write(buffer);
        writeCount++;
        if (successCount == buffer.length) {
          print('success retry write: ${writeCount} times ${successCount} ${buffer.length}');
          break;
        }
        if (successCount > 0 && successCount != buffer.length) {
          print('WARN: ${successCount} ${buffer.length} ${this._socket.available()}');
        }
      }
    }
  }
}
