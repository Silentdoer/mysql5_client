// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";
import "dart:math";
import "dart:io";

import "package:stack_trace/stack_trace.dart";
import 'package:mysql_client/mysql_client.dart';

// sudo ngrep -x -q -d lo0 '' 'port 3306'

int serverCapabilityFlags;
String authPluginName;
String authPluginData;

int clientCapabilityFlags;
var userName = "root";
var password = "mysql";
var database = "test";
var characterSet = 0x21; // corrisponde a utf8_general_ci
var maxPacketSize = pow(2, 24) - 1;
var clientConnectAttributes = {
  "_os": "debian6.0",
  "_client_name": "libmysql",
  "_pid": "22344",
  "_client_version": "5.6.6-m9",
  "_platform": "x86_64",
  "foo": "bar"
};

Future main() async {
  await run();
}

Future run() async {
  clientCapabilityFlags =
      decodeFixedLengthInteger([0x0d, 0xa2, 0x00, 0x00]); // TODO sistemare

  var socket = await Socket.connect("localhost", 3306);
  socket.setOption(SocketOption.TCP_NODELAY, true);

  var reader = new DataStreamReader(socket);

  await readInitialHandshakePacket(reader);

  await writeHandshakeResponsePacket(socket);

  await readCommandResponsePacket(reader);

  var sw = new Stopwatch()..start();

  for (var i = 0; i < 10; i++) {
    await writeCommandQueryPacket(socket);
    await readCommandQueryResponsePacket(reader);
  }

  print("testMySql: ${sw.elapsedMilliseconds} ms");

  await socket.close();

  socket.destroy();
}

Future readInitialHandshakePacket(DataStreamReader reader) async {
  var payloadLength = await reader.readFixedLengthInteger(3);
  print("payloadLength: $payloadLength");

  var sequenceId = await reader.readOneLengthInteger();
  print("sequenceId: $sequenceId");

  reader.resetExpectedPayloadLength(payloadLength);

  // 1              [0a] protocol version
  var protocolVersion = await reader.readOneLengthInteger();
  print("protocolVersion: $protocolVersion");

  // string[NUL]    server version
  var serverVersion = await reader.readNulTerminatedString();
  print("serverVersion: $serverVersion");

  // 4              connection id
  var connectionId = await reader.readFixedLengthInteger(4);
  print("connectionId: $connectionId");

  // string[8]      auth-plugin-data-part-1
  var authPluginDataPart1 = await reader.readFixedLengthString(8);
  print("authPluginDataPart1: $authPluginDataPart1");

  // 1              [00] filler
  await reader.skipByte();
  print("filler1: SKIPPED");

  // 2              capability flags (lower 2 bytes)
  var capabilityFlags1 = await reader.readFixedLengthInteger(2);
  print("capabilityFlags1: $capabilityFlags1");

  // if more data in the packet:
  if (reader.isAvailable) {
    // 1              character set
    var characterSet = await reader.readOneLengthInteger();
    print("characterSet: $characterSet");

    // 2              status flags
    var statusFlags = await reader.readFixedLengthInteger(2);
    print("statusFlags: $statusFlags");

    // 2              capability flags (upper 2 bytes)
    var capabilityFlags2 = await reader.readFixedLengthInteger(2);
    print("capabilityFlags2: $capabilityFlags2");

    serverCapabilityFlags = capabilityFlags1 | (capabilityFlags2 << 16);
    print("serverCapabilityFlags: $serverCapabilityFlags");

    var authPluginDataLength = 0;
    // if capabilities & CLIENT_PLUGIN_AUTH {
    if (serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
      // 1              length of auth-plugin-data
      authPluginDataLength = await reader.readOneLengthInteger();
      print("authPluginDataLength: $authPluginDataLength");
    } else {
      // 1              [00]
      await reader.skipByte();
      print("filler2: SKIPPED");
    }

    // string[10]     reserved (all [00])
    await reader.skipBytes(10);
    print("reserved1: SKIPPED");

    var authPluginDataPart2 = "";
    // if capabilities & CLIENT_SECURE_CONNECTION {
    if (serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
      // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
      var len = max(authPluginDataLength - 8, 13);
      authPluginDataPart2 = await reader.readFixedLengthString(len);
      print("authPluginDataPart2: $authPluginDataPart2");
    }

    authPluginData = "$authPluginDataPart1$authPluginDataPart2"
        .substring(0, authPluginDataLength - 1);
    print("authPluginData: $authPluginData [${authPluginData.length}]");

    // if capabilities & CLIENT_PLUGIN_AUTH {
    if (serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
      // string[NUL]    auth-plugin name
      authPluginName = await reader.readNulTerminatedString();
      print("authPluginName: $authPluginName");
    }
  }
}

Future writeHandshakeResponsePacket(Socket socket) async {
  var sequenceId =
      0x01; // penso dipenda dalla sequenza a cui era arrivato il server

  var data = [];
  // 4              capability flags, CLIENT_PROTOCOL_41 always set
  data.addAll(encodeFixedLengthInteger(clientCapabilityFlags, 4));
  // 4              max-packet size
  data.addAll(encodeFixedLengthInteger(maxPacketSize, 4));
  // 1              character set
  data.addAll(encodeFixedLengthInteger(characterSet, 1));
  // string[23]     reserved (all [0])
  data.addAll(encodeFixedFilledLengthString(0x00, 23));
  // string[NUL]    username
  data.addAll(encodeNulTerminatedString(userName, utf8Encoded: true));

  // if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
  if (serverCapabilityFlags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0) {
    // lenenc-int     length of auth-response
    // string[n]      auth-response
    data.addAll(encodeLengthEncodedString(generateAuthResponse(
        password, authPluginData, authPluginName,
        utf8Encoded: true)));
    // else if capabilities & CLIENT_SECURE_CONNECTION {
  } else if (serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
    // 1              length of auth-response
    // string[n]      auth-response
    // TODO to implement
    throw new UnsupportedError("TODO to implement");
    // else {
  } else {
    // string[NUL]    auth-response
    // TODO to implement
    throw new UnsupportedError("TODO to implement");
  }
  // if capabilities & CLIENT_CONNECT_WITH_DB {
  if (serverCapabilityFlags & CLIENT_CONNECT_WITH_DB != 0) {
    // string[NUL]    database
    data.addAll(encodeNulTerminatedString(database, utf8Encoded: true));
  }
  // if capabilities & CLIENT_PLUGIN_AUTH {
  if (serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
    // string[NUL]    auth plugin name
    data.addAll(encodeNulTerminatedString(authPluginName));
  }
  // if capabilities & CLIENT_CONNECT_ATTRS {
  if (serverCapabilityFlags & CLIENT_CONNECT_ATTRS != 0) {
    // lenenc-int     length of all key-values
    // lenenc-str     key
    // lenenc-str     value
    // if-more data in 'length of all key-values', more keys and value pairs
    var data2 = [];
    clientConnectAttributes.forEach((key, value) {
      data2.addAll(encodeLengthEncodedString(key));
      data2.addAll(encodeLengthEncodedString(value));
    });
    data.addAll(encodeLengthEncodedInteger(data2.length));
    data.addAll(data2);
  }

  var packetHeaderData = new List(4);
  packetHeaderData.setAll(0, encodeFixedLengthInteger(data.length, 3));
  packetHeaderData.setAll(3, encodeFixedLengthInteger(sequenceId, 1));

  socket.add(packetHeaderData);
  socket.add(data);
}

Future readCommandResponsePacket(DataStreamReader reader) async {
  var payloadLength = await reader.readFixedLengthInteger(3);
  print("payloadLength: $payloadLength");

  var sequenceId = await reader.readOneLengthInteger();
  print("sequenceId: $sequenceId");

  reader.resetExpectedPayloadLength(payloadLength);

  // int<1>	header	[00] or [fe] the OK packet header
  var header = await reader.readOneLengthInteger();
  print("header: $header");

  // TODO distinguere il pacchetto OK, ERROR

  // int<lenenc>	affected_rows	affected rows
  var affectedRows = await reader.readLengthEncodedInteger();
  print("affectedRows: $affectedRows");

  // int<lenenc>	last_insert_id	last insert-id
  var lastInsertId = await reader.readLengthEncodedInteger();
  print("lastInsertId: $lastInsertId");

  var statusFlags;
  // if capabilities & CLIENT_PROTOCOL_41 {
  if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
    // int<2>	status_flags	Status Flags
    statusFlags = await reader.readFixedLengthInteger(2);
    print("statusFlags: $statusFlags");
    // int<2>	warnings	number of warnings
    var warnings = await reader.readFixedLengthInteger(2);
    print("warnings: $warnings");
    // } elseif capabilities & CLIENT_TRANSACTIONS {
  } else if (serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
    // int<2>	status_flags	Status Flags
    statusFlags = await reader.readFixedLengthInteger(2);
    print("statusFlags: $statusFlags");
  } else {
    statusFlags = 0;
    print("statusFlags: $statusFlags");
  }

  // if capabilities & CLIENT_SESSION_TRACK {
  if (serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
    // string<lenenc>	info	human readable status information
    if (reader.isAvailable) {
      var info = await reader.readLengthEncodedString();
      print("info: $info");
    }

    // if status_flags & SERVER_SESSION_STATE_CHANGED {
    if (statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
      // string<lenenc>	session_state_changes	session state info
      if (reader.isAvailable) {
        var sessionStateChanges = await reader.readLengthEncodedString();
        print("sessionStateChanges: $sessionStateChanges");
      }
    }
    // } else {
  } else {
    // string<EOF>	info	human readable status information
    var info = await reader.readRestOfPacketString();
    print("info: $info");
  }
}

Future writeCommandQueryPacket(Socket socket) async {
  var sequenceId = 0x00;

  var data = [];
  // 1              [03] COM_QUERY
  data.addAll(encodeFixedLengthInteger(COM_QUERY, 1));
  // string[EOF]    the query the server shall execute
  data.addAll(encodeString("SELECT * FROM people", utf8Encoded: true));

  var packetHeaderData = new List(4);
  packetHeaderData.setAll(0, encodeFixedLengthInteger(data.length, 3));
  packetHeaderData.setAll(3, encodeFixedLengthInteger(sequenceId, 1));

  socket.add(packetHeaderData);
  socket.add(data);
}

Future readCommandQueryResponsePacket(DataStreamReader reader) async {
  var sw = new Stopwatch()..start();

  await readResultSetColumnCountResponsePacket(reader);
  print("readResultSetColumnCountResponsePacket: ${sw.elapsedMilliseconds} ms");

  sw.reset();
  var columnCount = 3;
  for (var i = 0; i < columnCount; i++) {
    await readResultSetColumnDefinitionResponsePacket(reader);
  }
  await readEOFResponsePacket(reader);
  print(
      "readResultSetColumnDefinitionResponsePacket: ${sw.elapsedMilliseconds} ms");

  sw.reset();
  try {
    while (true) {
      await readResultSetRowResponsePacket(reader);
    }
  } on EOFError {
    if (reader.isFirstByte) {
      // EOF packet
      // if capabilities & CLIENT_PROTOCOL_41 {
      if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
        // int<2>	warnings	number of warnings
        var warnings = await reader.readFixedLengthInteger(2);

        // int<2>	status_flags	Status Flags
        var statusFlags = await reader.readFixedLengthInteger(2);
      }
    } else {
      rethrow;
    }
  } on UndefinedError {
    if (reader.isFirstByte) {
      // TODO Error packet

      throw new UnsupportedError("IMPLEMENT STARTED ERROR PACKET");
    } else {
      rethrow;
    }
  }
  print(
      "readResultSetColumnDefinitionResponsePacket: ${sw.elapsedMilliseconds} ms");
}

Future readResultSetColumnCountResponsePacket(DataStreamReader reader) async {
  var payloadLength = await reader.readFixedLengthInteger(3);

  var sequenceId = await reader.readOneLengthInteger();

  reader.resetExpectedPayloadLength(payloadLength);

  // A packet containing a Protocol::LengthEncodedInteger column_count
  var columnCount = await reader.readOneLengthInteger();
}

Future readResultSetColumnDefinitionResponsePacket(
    DataStreamReader reader) async {
  var payloadLength = await reader.readFixedLengthInteger(3);

  var sequenceId = await reader.readOneLengthInteger();

  reader.resetExpectedPayloadLength(payloadLength);

  // lenenc_str     catalog
  var catalog = await reader.readLengthEncodedString();

  // lenenc_str     schema
  var schema = await reader.readLengthEncodedString();

  // lenenc_str     table
  var table = await reader.readLengthEncodedString();

  // lenenc_str     org_table
  var orgTable = await reader.readLengthEncodedString();

  // lenenc_str     name
  var name = await reader.readLengthEncodedString();

  // lenenc_str     org_name
  var orgName = await reader.readLengthEncodedString();

  // lenenc_int     length of fixed-length fields [0c]
  var fieldsLength = await reader.readLengthEncodedInteger();

  // 2              character set
  var characterSet = await reader.readFixedLengthInteger(2);

  // 4              column length
  var columnLength = await reader.readFixedLengthInteger(4);

  // 1              type
  var type = await reader.readOneLengthInteger();

  // 2              flags
  var flags = await reader.readFixedLengthInteger(2);

  // 1              decimals
  var decimals = await reader.readOneLengthInteger();

  // 2              filler [00] [00]
  await reader.skipBytes(2);
}

Future readResultSetRowResponsePacket(DataStreamReader reader) async {
  var payloadLength = await reader.readFixedLengthInteger(3);

  var sequenceId = await reader.readFixedLengthInteger(1);

  reader.resetExpectedPayloadLength(payloadLength);

  while (reader.isAvailable) {
    var value;
    try {
      value = await reader.readLengthEncodedString();
    } on NullError {
      value = null;
    }
  }
}

Future readEOFResponsePacket(DataStreamReader reader) async {
  var payloadLength = await reader.readFixedLengthInteger(3);

  var sequenceId = await reader.readOneLengthInteger();

  reader.resetExpectedPayloadLength(payloadLength);

  // int<1>	header	[00] or [fe] the OK packet header
  var header = await reader.readOneLengthInteger();
  if (header != 0xfe) {
    throw new StateError("$header != 0xfe");
  }

  // if capabilities & CLIENT_PROTOCOL_41 {
  if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
    // int<2>	warnings	number of warnings
    var warnings = await reader.readFixedLengthInteger(2);
    // int<2>	status_flags	Status Flags
    var statusFlags = await reader.readFixedLengthInteger(2);
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
