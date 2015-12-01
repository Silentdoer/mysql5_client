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

const String _EMPTY_STRING = "";
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

// TODO gestione del length = null (toString da null)
class DataBuffer {
  final List<DataRange> _dataRanges = new List<DataRange>();

  DataRange _cachedDataRange;
  List<int> _cachedData;
  int _cachedLength;

  DataBuffer() {
    DATA_BUFFER_COUNT++;
  }

  void clean() {
    for (var range in _dataRanges) {
      range.deinitialize();
    }
    _dataRanges.clear();

    if (_cachedDataRange != null) {
      if (_cachedDataRange.isInitialized) {
        _cachedDataRange.deinitialize();
      }
      _cachedDataRange = null;
    }
    _cachedData = null;
    _cachedLength = null;
  }

  DataRange get lastRange => _dataRanges.last;

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
      if (_dataRanges.length > 1) {
        _cachedDataRange = new DataRange();
        _cachedDataRange.initialize(data);
      } else if (_dataRanges.length == 1) {
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

  String toString() => this.singleRange != null
      ? new String.fromCharCodes(
          this.singleRange.data, this.singleRange.start, this.singleRange.end)
      : _EMPTY_STRING;

  String toUTF8() => this.singleRange != null
      ? UTF8.decoder.convert(
          this.singleRange.data, this.singleRange.start, this.singleRange.end)
      : _EMPTY_STRING;
}

class DataRange {
  static final List<DataRange> _POOL = new List();

  List<int> _data;
  int _start;
  int _length;
  bool __isPending;

  factory DataRange() {
    return _POOL.isNotEmpty ? _POOL.removeLast() : new DataRange._();
  }

  DataRange._() {
    DATA_RANGE_COUNT++;
  }

  bool get isInitialized => _data != null;

  void initialize(List<int> data, [int start = 0, int length]) {
    this._data = data;
    this._start = start;
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
  }

  void deinitialize() {
    _data = null;
    _start = null;
    _length = null;
    __isPending = null;

    _POOL.add(this);
  }

  int get start => _start;
  int get end => _start + _length;
  int get length => _length;
  List<int> get data => _data;
  bool get _isPending => __isPending;
}

class DataStreamReader {
  final Queue<_DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  int _expectedPayloadLength;
  int _loadedCount;

  final DataBuffer _dataBuffer;

  DataStreamReader(this._stream) : this._dataBuffer = new DataBuffer() {
    this._loadedCount = 0;
    this._stream.listen(_onData);
  }

  bool get isFirstByte => _loadedCount == 1;

  bool get isAvailable => _loadedCount < _expectedPayloadLength;

  void resetExpectedPayloadLength(int expectedPayloadLength) {
    _loadedCount = 0;
    _expectedPayloadLength = expectedPayloadLength;
  }

  Future<int> readFixedLengthInteger(int length) {
    if (length == 1) {
      return this.readByte();
    } else {
      return this
          ._readFixedLengthBuffer(length)
          .then((_) => _dataBuffer.toInt());
    }
  }

  Future<String> readFixedLengthString(int length) {
    if (length == 1) {
      return this.readByte().then((value) => new String.fromCharCode(value));
    } else {
      return this
          ._readFixedLengthBuffer(length)
          .then((_) => _dataBuffer.toString());
    }
  }

  Future<String> readFixedLengthUTF8String(int length) =>
      this._readFixedLengthBuffer(length).then((_) => _dataBuffer.toUTF8());

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
            if (_expectedPayloadLength - _loadedCount >= 8) {
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
            ._readFixedLengthBuffer(bytesLength - 1)
            .then((_) => _dataBuffer.toInt());
      });

  Future<String> readLengthEncodedString() => this
      .readLengthEncodedInteger()
      .then((length) => this._readFixedLengthBuffer(length))
      .then((_) => _dataBuffer.toString());

  Future<String> readLengthEncodedUTF8String() => this
      .readLengthEncodedInteger()
      .then((length) => this._readFixedLengthBuffer(length))
      .then((_) => _dataBuffer.toUTF8());

  Future<String> readNulTerminatedString() =>
      this._readUpToBuffer(0x00).then((_) => _dataBuffer.toString());

  Future<String> readNulTerminatedUTF8String() =>
      this._readUpToBuffer(0x00).then((_) => _dataBuffer.toUTF8());

  Future<String> readRestOfPacketString() => this
      .readFixedLengthString(this._expectedPayloadLength - this._loadedCount);

  Future<String> readRestOfPacketUTF8String() => this.readFixedLengthUTF8String(
      this._expectedPayloadLength - this._loadedCount);

  Future skipByte() {
    var value = _readChunk((chunk) => chunk.skipSingle());
    return _thenFuture(value, (_) {
      _loadedCount++;
    });
  }

  Future<int> readByte() {
    var value = _readChunk((chunk) => chunk.readSingle());
    return _thenFuture(value, (value) {
      _loadedCount++;
      return value;
    });
  }

  Future skipBytes(int length) => _readFixedLengthBuffer(length).then((_) {
        _loadedCount += length;
      });

  Future<List<int>> readBytes(int length) {
    if (length > 1) {
      return _readFixedLengthBuffer(length).then((_) => _dataBuffer.data);
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

  Future skipBytesUpTo(int terminator) => _readUpToBuffer(terminator);

  Future<List<int>> readBytesUpTo(int terminator) =>
      _readUpToBuffer(terminator).then((_) => _dataBuffer.data);

  Future _readFixedLengthBuffer(int length) {
    _dataBuffer.clean();
    if (length > 0) {
      return _thenFuture(__readFixedLengthBuffer(length), (_) => _);
    } else {
      return new Future.value();
    }
  }

  Future _readUpToBuffer(int terminator) {
    _dataBuffer.clean();
    return _thenFuture(__readUpToBuffer(terminator), (_) => _);
  }

  __readFixedLengthBuffer(int leftLength) {
    var value = _readChunk((chunk) {
      _dataBuffer.add(chunk.readFixedRange(leftLength));
    });
    return _then(value, (_) => _readFixedLengthBufferInternal(leftLength));
  }

  _readFixedLengthBufferInternal(int leftLength) {
    var range = _dataBuffer.lastRange;
    _loadedCount += range.length;
    if (range._isPending) {
      return __readFixedLengthBuffer(leftLength - range.length);
    }
  }

  __readUpToBuffer(int terminator) {
    var value = _readChunk((chunk) {
      _dataBuffer.add(chunk.readRangeUpTo(terminator));
    });
    return _then(value, (_) => _readUpToBufferInternal(terminator));
  }

  _readUpToBufferInternal(int terminator) {
    var range = _dataBuffer.lastRange;
    if (range._isPending) {
      _loadedCount += range.length;
      return __readUpToBuffer(terminator);
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

  Future<dynamic> _thenFuture(value, then(value)) {
    if (value is Future) {
      return value.then(then);
    } else {
      return new Future.value(then(value));
    }
  }

  _then(value, then(value)) {
    if (value is Future) {
      return value.then(then);
    } else {
      return then(value);
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
    var readData = new DataRange();
    readData.initialize(_data, _index, length);
    _index += readData.length;
    return readData;
  }

  DataRange readRangeUpTo(int terminator) {
    var readData = new DataRange();

    var toIndex = _data.indexOf(terminator, _index) + 1;

    if (toIndex > 0) {
      readData.initialize(_data, _index, toIndex - _index - 1);
      _index = toIndex;
    } else {
      readData.initialize(_data, _index, _data.length - _index + 1);
      _index = _data.length;
    }

    return readData;
  }
}
