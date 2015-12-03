// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_chunk;

import "data_range2.dart";

class DataChunk {
  final List<int> _data;

  int _index;

  DataChunk(this._data) : this._index = 0;

  bool get isEmpty => _data.length - _index == 0;

  DataRange readRange(int length) {

    // TODO gestire i pending

    var readData = new DataRange(_data, _index, length);
    _index += readData.length;
    return readData;
  }
}
