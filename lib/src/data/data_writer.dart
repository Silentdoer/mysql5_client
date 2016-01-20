// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

part of mysql_client.data;

class DataWriter {
  final RawSocket _socket;

  DataWriter(this._socket);

  void writeBuffer(List<int> buffer) {
    _socket.write(buffer);
  }
}
