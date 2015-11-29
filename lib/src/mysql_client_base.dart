// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.base;

import "dart:math";
import "dart:convert";

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

int decodeFixedLengthInteger1(data) => data;

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