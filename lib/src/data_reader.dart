// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_reader;

import "dart:async";
import "dart:collection";
import "dart:math";

import "data_chunk.dart";
import "data_buffer.dart";
import "data_range.dart";

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

class UndefinedError extends Error {
  String toString() => "Undefined value";
}

class NullError extends Error {
  String toString() => "Null value";
}

class EOFError extends Error {
  String toString() => "EOF value";
}

class DataStreamReader {
  final Queue<DataChunk> _chunks = new Queue();

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

  Future skipByte() {
    var value = _readChunk((chunk) => chunk.skipSingle());
    return _thenFuture(value, (_) {
      _loadedCount++;
    });
  }

  Future skipBytes(int length) => _readFixedLengthBuffer(length).then((_) {
        _loadedCount += length;
      });

  Future<int> readOneLengthInteger() {
    var value = _readChunk((chunk) => chunk.readSingle());
    return _thenFuture(value, (value) {
      _loadedCount++;
      return value;
    });
  }

  Future<int> readFixedLengthInteger(int length) {
    if (length == 1) {
      return this.readOneLengthInteger();
    } else {
      return this
          ._readFixedLengthBuffer(length)
          .then((_) => _dataBuffer.toInt());
    }
  }

  Future<String> readFixedLengthString(int length) {
    if (length == 1) {
      return this
          .readOneLengthInteger()
          .then((value) => new String.fromCharCode(value));
    } else {
      return this
          ._readFixedLengthBuffer(length)
          .then((_) => _dataBuffer.toString());
    }
  }

  Future<String> readFixedLengthUTF8String(int length) =>
      this._readFixedLengthBuffer(length).then((_) => _dataBuffer.toUTF8());

  Future<int> readLengthEncodedInteger() =>
      this.readOneLengthInteger().then((firstByte) {
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
    if (range.isPending) {
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
    if (range.isPending) {
      _loadedCount += range.length;
      return __readUpToBuffer(terminator);
    } else {
      // aggiungo il terminatore
      _loadedCount += range.length + 1;
    }
  }

  _readChunk(chunkReader(DataChunk chunk)) {
    if (_chunks.isEmpty) {
      _dataReadyCompleter = new Completer();
      return _dataReadyCompleter.future
          .then((_) => _readChunkInternal(chunkReader));
    } else {
      return _readChunkInternal(chunkReader);
    }
  }

  _readChunkInternal(chunkReader(DataChunk chunk)) {
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
      _chunks.add(new DataChunk(data));

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