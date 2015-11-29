// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.reader;

import "dart:async";
import "dart:collection";
import "dart:convert";
import "dart:math";

int DATA_RANGE_COUNT = 0;
int DATA_BUFFER_COUNT = 0;
int DATA_CHUNK_COUNT = 0;
int BUFFER_LIST_COUNT = 0;
int RANGE_LIST_COUNT = 0;
int LIST1_COUNT = 0;

const int _MAX_INT_1 = 251;
final int _MAX_INT_2 = pow(2, 2 * 8);
const int _PREFIX_INT_2 = 0xfc;
final int _MAX_INT_3 = pow(2, 3 * 8);
const int _PREFIX_INT_3 = 0xfd;
final int _MAX_INT_8 = pow(2, 8 * 8);
const int _PREFIX_INT_8 = 0xfe;

const List<int> _EMPTY_DATA = const [];
const List<DataRange> _EMPTY_RANGE_LIST = const [];
final DataBuffer _EMPTY_BUFFER = new DataBuffer();

// TODO creare un pool di DataRange, DataBuffer

class DataBuffer {
  final List<DataRange> _dataRanges = new List<DataRange>();

  DataRange _cachedDataRange;
  List<int> _cachedData;
  int _cachedLength;

  DataBuffer() {
    DATA_BUFFER_COUNT++;
  }

  List<DataRange> get ranges => _dataRanges;

  void add(DataRange dataRange) {
    _dataRanges.add(dataRange);
  }

  int get length {
    if (_cachedLength == null) {
      _cachedLength = 0;
      for (var range in _dataRanges) {
        _cachedLength += range.length;
      }
    }
    return _cachedLength;
  }

  DataRange get singleRange {
    if (_cachedDataRange == null) {
      if (ranges.length > 1) {
        _cachedDataRange = new DataRange(data);
      } else if (ranges.length == 1) {
        _cachedDataRange = _dataRanges[0];
      }
    }
    return _cachedDataRange;
  }

  List<int> get data {
    if (_cachedData == null) {
      // print("Created list [${this.length}]");
      BUFFER_LIST_COUNT++;
      _cachedData = new List(this.length);
      var start = 0;
      this._dataRanges.forEach((range) {
        var end = start + range.length;
        _cachedData.setRange(start, end, range.data, range.start);
        start = end;
      });
    }
    return _cachedData;
  }

  int toInt() {
    var data = this.singleRange.data;
    var i = this.singleRange.start;
    switch (this.singleRange.length) {
      case 1:
        return data[i++];
      case 2:
        return data[i++] | data[i++] << 8;
      case 3:
        return data[i++] | data[i++] << 8 | data[i++] << 16;
      case 4:
        return data[i++] | data[i++] << 8 | data[i++] << 16 | data[i++] << 24;
      case 5:
        return data[i++] |
            data[i++] << 8 |
            data[i++] << 16 |
            data[i++] << 24 |
            data[i++] << 32;
      case 6:
        return data[i++] |
            data[i++] << 8 |
            data[i++] << 16 |
            data[i++] << 24 |
            data[i++] << 32 |
            data[i++] << 40;
      case 7:
        return data[i++] |
            data[i++] << 8 |
            data[i++] << 16 |
            data[i++] << 24 |
            data[i++] << 32 |
            data[i++] << 40 |
            data[i++] << 48;
      case 8:
        return data[i++] |
            data[i++] << 8 |
            data[i++] << 16 |
            data[i++] << 24 |
            data[i++] << 32 |
            data[i++] << 40 |
            data[i++] << 48 |
            data[i++] << 56;
    }

    throw new UnsupportedError("${data.length} length");
  }

  String toString() => new String.fromCharCodes(this.singleRange.data);

  String toUTF8() => UTF8.decode(this.singleRange.data);
}

class DataRange {
  final List<int> _data;
  final int _start;
  int _length;
  bool __isPending;

  DataRange(this._data, [this._start = 0, int length]) {
    if (length == null) {
      length = this._data.length - this._start;
    }
    if (this._start + length <= this._data.length) {
      this.__isPending = false;
      this._length = length;
    } else {
      this.__isPending = true;
      this._length = this._data.length - this._start;
    }

    DATA_RANGE_COUNT++;
  }

  int get start => _start;
  int get end => _start + _length;
  int get length => _length;
  List<int> get data => _data;
  bool get _isPending => __isPending;

  List<int> getData() {
    // print("Created list [$_length]");
    RANGE_LIST_COUNT++;
    var data = new List(_length);
    setData(data, 0);
    return data;
  }

  void setData(List<int> data, int start) {
    data.setRange(start, start + _length, _data, _start);
  }
}

// TODO marker conteggio byte caricati
class DataStreamReader {
  final Queue<_DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  DataStreamReader(this._stream) {
    this._stream.listen(_onData);
  }

  Future<int> readFixedLengthInteger(int length) async {
    var buffer = await this.readFixedLengthBuffer(length);
    return buffer.toInt();
  }

  Future<int> readLengthEncodedInteger() async {
    var firstByte = await this.readByte();
    var bytesLength;
    switch (firstByte) {
      case _PREFIX_INT_2:
        bytesLength = 3;
        break;
      case _PREFIX_INT_3:
        bytesLength = 4;
        break;
      case _PREFIX_INT_8:
        bytesLength = 9;
        break;
      default:
        bytesLength = 1;
    }
    if (bytesLength > 1) {
      var buffer = await this.readFixedLengthBuffer(bytesLength - 1);
      return buffer.toInt();
    } else {
      return firstByte;
    }
  }

  Future<String> readFixedLengthString(int length) async {
    var buffer = await this.readFixedLengthBuffer(length);
    return buffer.toString();
  }

  Future<String> readFixedLengthUTF8String(int length) async {
    var buffer = await this.readFixedLengthBuffer(length);
    return buffer.toUTF8();
  }

  Future<String> readLengthEncodedString() async {
    var length = await this.readLengthEncodedInteger();
    var buffer = await this.readFixedLengthBuffer(length);
    return buffer.toString();
  }

  Future<String> readLengthEncodedUTF8String() async {
    var length = await this.readLengthEncodedInteger();
    var buffer = await this.readFixedLengthBuffer(length);
    return buffer.toUTF8();
  }

  Future<String> readNulTerminatedString() async {
    var buffer = await this.readUpToBuffer(0x00);
    return buffer.toString();
  }

  Future<String> readNulTerminatedUTF8String() async {
    var buffer = await this.readUpToBuffer(0x00);
    return buffer.toUTF8();
  }

  Future skipByte() => _readChunk((chunk) => chunk.skipSingle());

  Future<int> readByte() => _readChunk((chunk) => chunk.readSingle());

  Future skipBytes(int length) async {
    await readFixedLengthBuffer(length);
  }

  Future<List<int>> readBytes(int length) async {
    if (length > 1) {
      var buffer = await readFixedLengthBuffer(length);
      return buffer.data;
    } else if (length == 1) {
      var value = await readByte();
      // print("Created list [1]");
      LIST1_COUNT++;
      return new List.filled(1, value);
    } else {
      return _EMPTY_DATA;
    }
  }

  Future skipBytesUpTo(int terminator) async {
    await readUpToBuffer(terminator);
  }

  Future<List<int>> readBytesUpTo(int terminator) async {
    var buffer = await readUpToBuffer(terminator);
    return buffer.data;
  }

  Future<DataBuffer> readFixedLengthBuffer(int length) async {
    if (length > 0) {
      var buffer = new DataBuffer();
      var resultLength = 0;

      bool isPending = true;
      while (isPending) {
        await _readChunk((chunk) {
          var range = chunk.readFixedRange(length - resultLength);
          buffer.add(range);
          resultLength += range.length;
          isPending = range._isPending;
        });
      }

      return buffer;
    } else {
      return _EMPTY_BUFFER;
    }
  }

  Future<DataBuffer> readUpToBuffer(int terminator) async {
    var buffer = new DataBuffer();

    bool isPending = true;
    while (isPending) {
      await _readChunk((chunk) {
        var range = chunk.readRangeUpTo(terminator);
        buffer.add(range);
        isPending = range._isPending;
      });
    }

    return buffer;
  }

  void _onData(List<int> data) {
    if (data != null && data.isNotEmpty) {
      _chunks.add(new _DataChunk(data));

      if (_chunks.length == 1 && _dataReadyCompleter != null) {
        var completer = _dataReadyCompleter;
        _dataReadyCompleter = null;
        completer.complete();
      }
    }
  }

  Future _readChunk(chunkReader(_DataChunk chunk)) async {
    if (_chunks.isEmpty) {
      _dataReadyCompleter = new Completer();
      await _dataReadyCompleter.future;
    }

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

class _DataChunk {
  final List<int> _data;

  int _index;

  _DataChunk(this._data) : this._index = 0 {
    DATA_CHUNK_COUNT++;
  }

  bool get isEmpty => _data.length - _index == 0;

  void skipSingle() {
    _index++;
  }

  int readSingle() => _data[_index++];

  DataRange readFixedRange(int length) {
    var readData = new DataRange(_data, _index, length);
    _index += readData.length;

    return readData;
  }

  DataRange readRangeUpTo(int terminator) {
    var readData;

    var toIndex = _data.indexOf(terminator, _index);
    if (toIndex != -1) {
      readData = new DataRange(_data, _index, toIndex - _index);
      _index = toIndex + 1;
    } else {
      readData = new DataRange(_data, _index, _data.length + 1);
      _index = readData.length;
    }

    return readData;
  }
}
