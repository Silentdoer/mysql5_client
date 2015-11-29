// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.base;

import "dart:async";
import "dart:collection";
import "dart:math";
import "dart:convert";
import "dart:typed_data";

import "package:crypto/crypto.dart";

const List<int> _EMPTY_LIST = const [];

const int _MAX_INT_1 = 251;
final int _MAX_INT_2 = pow(2, 2 * 8);
const int _PREFIX_INT_2 = 0xfc;
final int _MAX_INT_3 = pow(2, 3 * 8);
const int _PREFIX_INT_3 = 0xfd;
final int _MAX_INT_8 = pow(2, 8 * 8);
const int _PREFIX_INT_8 = 0xfe;

const int CLIENT_PLUGIN_AUTH = 0x00080000;
const int CLIENT_SECURE_CONNECTION = 0x00008000;
const int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
const int CLIENT_CONNECT_WITH_DB = 0x00000008;
const int CLIENT_CONNECT_ATTRS = 0x00100000;

const int CLIENT_PROTOCOL_41 = 0x00000200;
const int CLIENT_TRANSACTIONS = 0x00002000;
const int CLIENT_SESSION_TRACK = 0x00800000;

const int SERVER_SESSION_STATE_CHANGED = 0x4000;

const int COM_QUERY = 0x03;

String generateAuthResponse(
    String password, String authPluginData, String authPluginName,
    {utf8Encoded: false}) {
  var encodedPassword = encodeString(password, utf8Encoded: utf8Encoded);
  var encodedAuthPluginData =
      encodeString(authPluginData, utf8Encoded: utf8Encoded);

  var response;

  if (authPluginName == "mysql_native_password") {
    // SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
    var passwordSha1 = (new SHA1()..add(encodedPassword)).close();
    var passwordSha1Sha1 = (new SHA1()..add(passwordSha1)).close();
    var hash =
        (new SHA1()..add(encodedAuthPluginData)..add(passwordSha1Sha1)).close();

    var buffer = new StringBuffer();
    var generatedHash = new List<int>(hash.length);
    for (var i = 0; i < generatedHash.length; i++) {
      buffer.writeCharCode(hash[i] ^ passwordSha1[i]);
    }
    response = buffer.toString();
  } else {
    throw new UnsupportedError(authPluginName);
  }

  return response;
}

List<int> encodeString(String value, {utf8Encoded: false}) {
  return !utf8Encoded ? value.codeUnits : UTF8.encode(value);
}

String decodeString(List<int> data, {utf8Encoded: false}) {
  return !utf8Encoded ? new String.fromCharCodes(data) : UTF8.decode(data);
}

List<int> encodeFixedFilledLengthString(int value, int length) {
  return new List.filled(length, value);
}

List<int> encodeNulTerminatedString(String value, {utf8Encoded: false}) {
  var data = new List(value.length + 1);

  data.setAll(0, encodeString(value, utf8Encoded: utf8Encoded));
  data[data.length - 1] = 0x00;

  return data;
}

List<int> encodeFixedLengthString(String value, int length,
    {utf8Encoded: false}) {
  var encodedValue = encodeString(value, utf8Encoded: utf8Encoded);

  if (encodedValue.length != length) {
    throw new ArgumentError("${encodedValue.length} != $length");
  }

  return encodedValue;
}

List<int> encodeLengthEncodedString(String value, {utf8Encoded: false}) {
  var encodedValue = encodeString(value, utf8Encoded: utf8Encoded);

  var data1 = encodeLengthEncodedInteger(encodedValue.length);
  var data2 = encodedValue;

  var data = new List(data1.length + data2.length);
  data.setAll(0, data1);
  data.setAll(data1.length, data2);

  return data;
}

List<int> encodeFixedLengthInteger(int value, int length) {
  var data = new List(length);

  for (var i = 0, rotation = 0, mask = 0xff;
      i < data.length;
      i++, rotation += 8, mask <<= 8) {
    data[i] = (value & mask) >> rotation;
  }

  return data;
}

List<int> encodeLengthEncodedInteger(int value) {
  var data;

  if (value < _MAX_INT_1) {
    data = new List(1);
    data[0] = value;
  } else {
    if (value < _MAX_INT_2) {
      data = new List.filled(3, 0);
      data[0] = _PREFIX_INT_2;
    } else if (value < _MAX_INT_3) {
      data = new List.filled(4, 0);
      data[0] = _PREFIX_INT_3;
    } else if (value < _MAX_INT_8) {
      data = new List.filled(9, 0);
      data[0] = _PREFIX_INT_8;
    } else {
      throw new UnsupportedError("Undefined value");
    }

    for (var i = 1, rotation = 0, mask = 0xff;
        i < data.length;
        i++, rotation += 8, mask <<= 8) {
      data[i] = (value & mask) >> rotation;
    }
  }

  return data;
}

int getDecodingLengthEncodedIntegerBytesLength(int firstByte) {
  switch (firstByte) {
    case _PREFIX_INT_2:
      return 3;
    case _PREFIX_INT_3:
      return 4;
    case _PREFIX_INT_8:
      return 9;
    default:
      return 1;
  }
}

int decodeFixedLengthInteger(List<int> data) {
  switch (data.length) {
    case 1:
      return data[0];
    case 2:
      return data[0] | data[1] << 8;
    case 3:
      return data[0] | data[1] << 8 | data[2] << 16;
    case 4:
      return data[0] | data[1] << 8 | data[2] << 16 | data[3] << 24;
    case 5:
      return data[0] |
          data[1] << 8 |
          data[2] << 16 |
          data[3] << 24 |
          data[4] << 32;
    case 6:
      return data[0] |
          data[1] << 8 |
          data[2] << 16 |
          data[3] << 24 |
          data[4] << 32 |
          data[5] << 40;
    case 7:
      return data[0] |
          data[1] << 8 |
          data[2] << 16 |
          data[3] << 24 |
          data[4] << 32 |
          data[5] << 40 |
          data[6] << 48;
    case 8:
      return data[0] |
          data[1] << 8 |
          data[2] << 16 |
          data[3] << 24 |
          data[4] << 32 |
          data[5] << 40 |
          data[6] << 48 |
          data[7] << 56;
  }

  throw new UnsupportedError("${data.length} length");
}

int decodeLengthEncodedInteger(List<int> secondData) {
  switch (secondData.length) {
    case 2:
      return secondData[0] | secondData[1] << 8;
    case 3:
      return secondData[0] | secondData[1] << 8 | secondData[2] << 16;
    case 8:
      return secondData[0] |
          secondData[1] << 8 |
          secondData[2] << 16 |
          secondData[3] << 24 |
          secondData[4] << 32 |
          secondData[5] << 40 |
          secondData[6] << 48 |
          secondData[7] << 56;
  }

  throw new UnsupportedError("${secondData.length} length");
}

// TODO provare anche a creare un DataRangeComposite

class DataRange {
  final List<int> _data;
  final int _start;
  int _length;
  bool _isPending;

  DataRange(this._data, this._start, int length) {
    if (this._start + length <= this._data.length) {
      this._isPending = false;
      this._length = length;
    } else {
      this._isPending = true;
      this._length = this._data.length - this._start;
    }
  }

  int get length => _length;
  bool get isPending => _isPending;
  int get start => _start;
  List<int> get data => _data;

  List<int> getData() {
    var r = new List(_length);
    setData(r, 0);
    return r;
  }

  void setData(List<int> list, int start) {
    var l = start + _length;
    for (var p1 = start, p2 = _start; p1 < l; p1++, p2++) {
      list[p1] = _data[p2];
    }
  }

  void setDataOld(List<int> list, int start) {
    list.setRange(start, start + _length, _data, _start);
  }
}

class DataChunk {
  int _index;

  List<int> _data;

  DataChunk(this._data) : this._index = 0;

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

class DataReader {
  final Queue<DataChunk> _chunks = new Queue();

  final Stream<List<int>> _stream;

  Completer _dataReadyCompleter;

  DataReader(this._stream) {
    this._stream.listen(_onData);
  }

  void _onData(List<int> data) {
    if (data != null && data.isNotEmpty) {
      _chunks.add(new DataChunk(data));

      if (_chunks.length == 1 && _dataReadyCompleter != null) {
        var completer = _dataReadyCompleter;
        _dataReadyCompleter = null;
        completer.complete();
      }
    }
  }

  Future _readChunk(chunkReader(DataChunk chunk)) async {
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

  Future skipByte() => _readChunk((chunk) => chunk.skipSingle());

  Future<int> readByte() => _readChunk((chunk) => chunk.readSingle());

  Future skipBytes(int length) async {
    if (length > 0) {
      var resultLength = 0;

      bool isPending = true;
      while (isPending) {
        await _readChunk((chunk) {
          var range = chunk.readFixedRange(length - resultLength);
          resultLength += range.length;
          isPending = range.isPending;
        });
      }
    } else if (length == 1) {
      await skipByte();
    }
  }

  Future<dynamic> processBytes(
      int length, rangeProcess(DataRange range)) async {
    if (length > 1) {
      var resultLength = 0;

      bool isPending = true;
      while (isPending) {
        var value;

        await _readChunk((chunk) {
          var range = chunk.readFixedRange(length - resultLength);
          value = rangeProcess(range);
          resultLength += range.length;
          isPending = range.isPending;
        });

        if (isPending) {
          throw new UnsupportedError("");
        }

        return value;
      }
    }

    throw new UnsupportedError("");
  }

  Future<List<int>> readBytes(int length) async {
    if (length > 1) {
      var result = new List(length);
      var resultLength = 0;

      bool isPending = true;
      while (isPending) {
        await _readChunk((chunk) {
          var range = chunk.readFixedRange(length - resultLength);
          range.setData(result, resultLength);
          resultLength += range.length;
          isPending = range.isPending;
        });
      }

      return result;
    } else if (length == 1) {
      var value = await readByte();
      return [value];
    } else {
      return _EMPTY_LIST;
    }
  }

  Future skipBytesUpTo(int terminator) async {
    bool isPending = true;
    while (isPending) {
      await _readChunk((chunk) {
        var range = chunk.readRangeUpTo(terminator);
        isPending = range.isPending;
      });
    }
  }

  Future<List<int>> readBytesUpTo(int terminator) async {
    var result = new List();

    bool isPending = true;
    while (isPending) {
      await _readChunk((chunk) {
        var range = chunk.readRangeUpTo(terminator);
        result.addAll(range.getData());
        isPending = range.isPending;
      });
    }

    return result;
  }
}
