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

const int _PREFIX_NULL = 0xfb;
const int _PREFIX_UNDEFINED = 0xff;

const int _MAX_INT_1 = 0xfb;
final int _MAX_INT_2 = pow(2, 2 * 8);
const int _PREFIX_INT_2 = 0xfc;
final int _MAX_INT_3 = pow(2, 3 * 8);
const int _PREFIX_INT_3 = 0xfd;
final int _MAX_INT_8 = pow(2, 8 * 8);
const int _PREFIX_INT_8 = 0xfe;

const List<int> _EMPTY_DATA = const [];
const List<DataRange> _EMPTY_RANGE_LIST = const [];
final DataBuffer _EMPTY_BUFFER = new DataBuffer();

class UndefinedError extends Error {
  String toString() => "Undefined value";
}

class NullError extends Error {
  String toString() => "Null value";
}

class EOFError extends Error {
  String toString() => "EOF value";
}

// TODO creare un pool di DataRange, DataBuffer
// TODO iteratori su reader per il recupero dei dati non tutti in una volta
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

  String toString() => new String.fromCharCodes(this
      .singleRange
      .data
      .sublist(this.singleRange.start, this.singleRange.end));

  String toUTF8() => UTF8.decode(this
      .singleRange
      .data
      .sublist(this.singleRange.start, this.singleRange.end));
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

class DataStreamReader {
  final Queue<_DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  int _expectedPayloadLength;
  int _loadedCount;

  DataStreamReader(this._stream) {
    this._loadedCount = 0;
    this._expectedPayloadLength = -1;
    this._stream.listen(_onData);
  }

  bool get isFirstByte => _loadedCount == 1;

  bool get isAvailable =>
      _expectedPayloadLength == -1 || _loadedCount < _expectedPayloadLength;

  void resetExpectedPayloadLength(int expectedPayloadLength) {
    _loadedCount = 0;
    _expectedPayloadLength = expectedPayloadLength;
  }

  Future<int> readFixedLengthInteger(int length) {
    if (length == 1) {
      return this.readByte();
    } else {
      return this
          .readFixedLengthBuffer(length)
          .then((buffer) => buffer.toInt());
    }
  }

  Future<String> readFixedLengthString(int length) {
    if (length == 1) {
      return this.readByte().then((value) => new String.fromCharCode(value));
    } else {
      return this
          .readFixedLengthBuffer(length)
          .then((buffer) => buffer.toString());
    }
  }

  Future<String> readFixedLengthUTF8String(int length) =>
      this.readFixedLengthBuffer(length).then((buffer) => buffer.toUTF8());

  Future<int> readLengthEncodedInteger() => this.readByte().then((firstByte) {
        var bytesLength;
        switch (firstByte) {
          case _PREFIX_INT_2:
            bytesLength = 3;
            break;
          case _PREFIX_INT_3:
            bytesLength = 4;
            break;
          case _PREFIX_INT_8:
            if (_expectedPayloadLength == -1 ||
                (_expectedPayloadLength - _loadedCount) >= 8) {
              bytesLength = 9;
            } else {
              throw new EOFError();
            }
            break;
          case _PREFIX_NULL:
            throw new NullError();
          case _PREFIX_UNDEFINED:
            throw new UndefinedError();
          default:
            return firstByte;
        }
        return this
            .readFixedLengthBuffer(bytesLength - 1)
            .then((buffer) => buffer.toInt());
      });

  Future<String> readLengthEncodedString() => this
      .readLengthEncodedInteger()
      .then((length) =>
          length != null ? this.readFixedLengthBuffer(length) : null)
      .then((buffer) => buffer != null ? buffer.toString() : null);

  Future<String> readLengthEncodedUTF8String() => this
      .readLengthEncodedInteger()
      .then((length) =>
          length != null ? this.readFixedLengthBuffer(length) : null)
      .then((buffer) => buffer != null ? buffer.toUTF8() : null);

  Future<String> readNulTerminatedString() =>
      this.readUpToBuffer(0x00).then((buffer) => buffer.toString());

  Future<String> readNulTerminatedUTF8String() =>
      this.readUpToBuffer(0x00).then((buffer) => buffer.toUTF8());

  Future skipByte() {
    var value = _readChunk((chunk) => chunk.skipSingle());
    if (value is Future) {
      return value.then((_) {
        _loadedCount++;
      });
    } else {
      _loadedCount++;
      return new Future.value();
    }
  }

  Future<int> readByte() {
    var value = _readChunk((chunk) => chunk.readSingle());
    if (value is Future) {
      return value.then((result) {
        _loadedCount++;
        return result;
      });
    } else {
      _loadedCount++;
      return new Future.value(value);
    }
  }

  Future skipBytes(int length) => readFixedLengthBuffer(length).then((_) {
        _loadedCount += length;
      });

  Future<List<int>> readBytes(int length) {
    if (length > 1) {
      return readFixedLengthBuffer(length).then((buffer) => buffer.data);
    } else if (length == 1) {
      return readByte().then((value) {
        // print("Created list [1]");
        LIST1_COUNT++;
        return new List.filled(1, value);
      });
    } else {
      return new Future.value(_EMPTY_DATA);
    }
  }

  Future skipBytesUpTo(int terminator) => readUpToBuffer(terminator);

  Future<List<int>> readBytesUpTo(int terminator) =>
      readUpToBuffer(terminator).then((buffer) => buffer.data);

  Future<DataBuffer> readFixedLengthBuffer(int length) {
    if (length > 0) {
      var buffer = new DataBuffer();
      var value = _readFixedLengthBuffer(buffer, length);
      if (value is Future) {
        return value.then((_) => buffer);
      } else {
        return new Future.value(buffer);
      }
    } else {
      return new Future.value(_EMPTY_BUFFER);
    }
  }

  Future<DataBuffer> readUpToBuffer(int terminator) {
    var buffer = new DataBuffer();
    var value = _readUpToBuffer(buffer, terminator);
    if (value is Future) {
      return value.then((_) => buffer);
    } else {
      return new Future.value(buffer);
    }
  }

  _readFixedLengthBuffer(DataBuffer buffer, int leftLength) {
    var value = _readChunk((chunk) {
      buffer.add(chunk.readFixedRange(leftLength));
    });

    if (value is Future) {
      return value
          .then((_) => _readFixedLengthBufferInternal(buffer, leftLength));
    } else {
      return _readFixedLengthBufferInternal(buffer, leftLength);
    }
  }

  _readFixedLengthBufferInternal(DataBuffer buffer, int leftLength) {
    var range = buffer.ranges.last;
    _loadedCount += range.length;
    if (range._isPending) {
      return _readFixedLengthBuffer(buffer, leftLength - range.length);
    }
  }

  _readUpToBuffer(DataBuffer buffer, int terminator) {
    var value = _readChunk((chunk) {
      buffer.add(chunk.readRangeUpTo(terminator));
    });

    if (value is Future) {
      return value.then((_) => _readUpToBufferInternal(buffer, terminator));
    } else {
      return _readUpToBufferInternal(buffer, terminator);
    }
  }

  _readUpToBufferInternal(DataBuffer buffer, int terminator) {
    var range = buffer.ranges.last;
    if (range._isPending) {
      _loadedCount += range.length;
      return _readUpToBuffer(buffer, terminator);
    } else {
      // aggiungo il terminatore
      _loadedCount += range.length + 1;
    }
  }

  _readChunk(chunkReader(_DataChunk chunk)) {
    if (_chunks.isEmpty) {
      _dataReadyCompleter = new Completer();
      return _dataReadyCompleter.future
          .then((_) => _readChunkInternal(chunkReader));
    } else {
      return _readChunkInternal(chunkReader);
    }
  }

  _readChunkInternal(chunkReader(_DataChunk chunk)) {
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
    if (data != null && data.isNotEmpty) {
      _chunks.add(new _DataChunk(data));

      if (_dataReadyCompleter != null) {
        var completer = _dataReadyCompleter;
        _dataReadyCompleter = null;
        completer.complete();
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

    var toIndex = _data.indexOf(terminator, _index) + 1;

    if (toIndex > 0) {
      readData = new DataRange(_data, _index, toIndex - _index - 1);
      _index = toIndex;
    } else {
      readData = new DataRange(_data, _index, _data.length - _index + 1);
      _index = _data.length;
    }

    return readData;
  }
}
