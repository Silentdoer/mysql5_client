// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.base;

import "dart:convert";
import "package:crypto/crypto.dart";

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
