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
    await writeCommandQueryPacket2(socket);
    await readCommandQueryResponsePacket2(reader);
  }

  print("testMySql: ${sw.elapsedMilliseconds} ms");

  print("DATA_RANGE_COUNT: $DATA_RANGE_COUNT");
  print("DATA_BUFFER_COUNT: $DATA_BUFFER_COUNT");
  print("DATA_CHUNK_COUNT: $DATA_CHUNK_COUNT");
  print("BUFFER_LIST_COUNT: $BUFFER_LIST_COUNT");
  print("RANGE_LIST_COUNT: $RANGE_LIST_COUNT");
  print("LIST1_COUNT: $LIST1_COUNT");

  await socket.close();

  socket.destroy();
}

Future readInitialHandshakePacket(DataStreamReader reader) async {
  var loaded = 0;

  var payloadLength = decodeFixedLengthInteger(await reader.readBytes(3));
  print("payloadLength: $payloadLength");

  var sequenceId = decodeFixedLengthInteger1(await reader.readByte());
  print("sequenceId: $sequenceId");

  // 1              [0a] protocol version
  var protocolVersion = decodeFixedLengthInteger1(await reader.readByte());
  print("$loaded: protocolVersion: $protocolVersion");
  loaded += 1;

  // string[NUL]    server version
  var serverVersion = decodeString(await reader.readBytesUpTo(0x00));
  print("$loaded: serverVersion: $serverVersion");
  loaded += serverVersion.length + 1;

  // 4              connection id
  var connectionId = decodeFixedLengthInteger(await reader.readBytes(4));
  print("$loaded: connectionId: $connectionId");
  loaded += 4;

  // string[8]      auth-plugin-data-part-1
  var authPluginDataPart1 = decodeString(await reader.readBytes(8));
  print("$loaded: authPluginDataPart1: $authPluginDataPart1");
  loaded += 8;

  // 1              [00] filler
  await reader.skipByte();
  print("$loaded: filler1: SKIPPED");
  loaded += 1;

  // 2              capability flags (lower 2 bytes)
  var capabilityFlags1 = decodeFixedLengthInteger(await reader.readBytes(2));
  print("$loaded: capabilityFlags1: $capabilityFlags1");
  loaded += 2;

  // if more data in the packet:
  if (loaded < payloadLength) {
    // 1              character set
    var characterSet = decodeFixedLengthInteger1(await reader.readByte());
    print("$loaded: characterSet: $characterSet");
    loaded += 1;

    // 2              status flags
    var statusFlags = decodeFixedLengthInteger(await reader.readBytes(2));
    print("$loaded: statusFlags: $statusFlags");
    loaded += 2;

    // 2              capability flags (upper 2 bytes)
    var capabilityFlags2 = decodeFixedLengthInteger(await reader.readBytes(2));
    print("$loaded: capabilityFlags2: $capabilityFlags2");
    loaded += 2;

    serverCapabilityFlags = capabilityFlags1 | (capabilityFlags2 << 16);
    print("serverCapabilityFlags: $serverCapabilityFlags");

    var authPluginDataLength = 0;
    // if capabilities & CLIENT_PLUGIN_AUTH {
    if (serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
      // 1              length of auth-plugin-data
      authPluginDataLength =
          decodeFixedLengthInteger1(await reader.readByte());
      print("$loaded: authPluginDataLength: $authPluginDataLength");
      loaded += 1;
    } else {
      // 1              [00]
      await reader.skipByte();
      print("$loaded: filler2: SKIPPED");
      loaded += 1;
    }

    // string[10]     reserved (all [00])
    await reader.skipBytes(10);
    print("$loaded: reserved1: SKIPPED");
    loaded += 10;

    var authPluginDataPart2 = "";
    // if capabilities & CLIENT_SECURE_CONNECTION {
    if (serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
      // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
      var len = max(authPluginDataLength - 8, 13);
      authPluginDataPart2 = decodeString(await reader.readBytes(len));
      print("$loaded: authPluginDataPart2: $authPluginDataPart2");
      loaded += len;
    }

    authPluginData = "$authPluginDataPart1$authPluginDataPart2"
        .substring(0, authPluginDataLength - 1);
    print("authPluginData: $authPluginData [${authPluginData.length}]");

    // if capabilities & CLIENT_PLUGIN_AUTH {
    if (serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
      // string[NUL]    auth-plugin name
      authPluginName = decodeString(await reader.readBytesUpTo(0x00));
      print("$loaded: authPluginName: $authPluginName");
      loaded += authPluginName.length + 1;
    }
  }

  print("Loaded: $loaded");
  if (loaded != payloadLength) {
    throw new StateError("$loaded != $payloadLength");
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
  var loaded = 0;

  var payloadLength = decodeFixedLengthInteger(await reader.readBytes(3));
  print("payloadLength: $payloadLength");

  var sequenceId = decodeFixedLengthInteger1(await reader.readByte());
  print("sequenceId: $sequenceId");

  // int<1>	header	[00] or [fe] the OK packet header
  var header = decodeFixedLengthInteger1(await reader.readByte());
  print("$loaded: header: $header");
  loaded += 1;

  // TODO distinguere il pacchetto OK, ERROR

  // int<lenenc>	affected_rows	affected rows
  var affectedRowsFirstByte = await reader.readByte();
  var affectedRowsBytesLength =
      getDecodingLengthEncodedIntegerBytesLength(affectedRowsFirstByte);
  var affectedRows = affectedRowsBytesLength > 1
      ? decodeLengthEncodedInteger(
          await reader.readBytes(affectedRowsBytesLength - 1))
      : affectedRowsFirstByte;
  print("$loaded: affectedRows: $affectedRows");
  loaded += affectedRowsBytesLength;

  // int<lenenc>	last_insert_id	last insert-id
  var lastInsertIdFirstByte = await reader.readByte();
  var lastInsertIdBytesLength =
      getDecodingLengthEncodedIntegerBytesLength(lastInsertIdFirstByte);
  var lastInsertId = lastInsertIdBytesLength > 1
      ? decodeLengthEncodedInteger(
          await reader.readBytes(lastInsertIdBytesLength - 1))
      : lastInsertIdFirstByte;
  print("$loaded: lastInsertId: $lastInsertId");
  loaded += lastInsertIdBytesLength;

  var statusFlags;
  // if capabilities & CLIENT_PROTOCOL_41 {
  if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
    // int<2>	status_flags	Status Flags
    statusFlags = decodeFixedLengthInteger(await reader.readBytes(2));
    print("$loaded: statusFlags: $statusFlags");
    loaded += 2;
    // int<2>	warnings	number of warnings
    var warnings = decodeFixedLengthInteger(await reader.readBytes(2));
    print("$loaded: warnings: $warnings");
    loaded += 2;
    // } elseif capabilities & CLIENT_TRANSACTIONS {
  } else if (serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
    // int<2>	status_flags	Status Flags
    statusFlags = decodeFixedLengthInteger(await reader.readBytes(2));
    print("$loaded: statusFlags: $statusFlags");
    loaded += 2;
  } else {
    statusFlags = 0;
    print("$loaded: statusFlags: $statusFlags");
  }

  // if capabilities & CLIENT_SESSION_TRACK {
  if (serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
    // string<lenenc>	info	human readable status information
    if (loaded < payloadLength) {
      var infoFirstByte = await reader.readByte();
      var infoBytesLength =
          getDecodingLengthEncodedIntegerBytesLength(infoFirstByte);
      var infoLength = infoBytesLength > 1
          ? decodeLengthEncodedInteger(
              await reader.readBytes(infoBytesLength - 1))
          : infoFirstByte;
      var info = decodeString(await reader.readBytes(infoLength));
      print("$loaded: info: $info");
      loaded += infoBytesLength + infoLength;
    }

    // if status_flags & SERVER_SESSION_STATE_CHANGED {
    if (statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
      // string<lenenc>	session_state_changes	session state info
      if (loaded < payloadLength) {
        var sessionStateChangesFirstByte = await reader.readByte();
        var sessionStateChangesBytesLength =
            getDecodingLengthEncodedIntegerBytesLength(
                sessionStateChangesFirstByte);
        var sessionStateChangesLength = sessionStateChangesBytesLength > 1
            ? decodeLengthEncodedInteger(
                await reader.readBytes(sessionStateChangesBytesLength - 1))
            : sessionStateChangesFirstByte;
        var sessionStateChanges =
            decodeString(await reader.readBytes(sessionStateChangesLength));
        print("$loaded: sessionStateChanges: $sessionStateChanges");
        loaded += sessionStateChangesBytesLength + sessionStateChangesLength;
      }
    }
    // } else {
  } else {
    // string<EOF>	info	human readable status information
    var info = decodeString(await reader.readBytes(payloadLength - loaded));
    print("$loaded: info: $info");
    loaded += info.length;
  }

  print("Loaded: $loaded");
  if (loaded != payloadLength) {
    throw new StateError("$loaded != $payloadLength");
  }
}

Future writeCommandQueryPacket1(Socket socket) async {
  var sequenceId = 0x00;

  var data = [];
  // 1              [03] COM_QUERY
  data.addAll(encodeFixedLengthInteger(COM_QUERY, 1));
  // string[EOF]    the query the server shall execute
  data.addAll(encodeString("select count(*) from people", utf8Encoded: true));

  var packetHeaderData = new List(4);
  packetHeaderData.setAll(0, encodeFixedLengthInteger(data.length, 3));
  packetHeaderData.setAll(3, encodeFixedLengthInteger(sequenceId, 1));

  socket.add(packetHeaderData);
  socket.add(data);
}

Future writeCommandQueryPacket2(Socket socket) async {
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

Future readCommandQueryResponsePacket1(DataStreamReader reader) async {
  await readResultSetColumnCountResponsePacket(reader);

  await readResultSetColumnDefinitionResponsePacket(reader);
  await readEOFResponsePacket(reader);

  await readResultSetRowResponsePacket(reader);
  await readEOFResponsePacket(reader);
}

Future readCommandQueryResponsePacket2(DataStreamReader reader) async {
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
  var eof = false;
  while (!eof) {
    eof = await readResultSetRowResponsePacket(reader);
  }
  print(
      "readResultSetColumnDefinitionResponsePacket: ${sw.elapsedMilliseconds} ms");
}

Future readResultSetColumnCountResponsePacket(DataStreamReader reader) async {
  var loaded = 0;

  var payloadLength = decodeFixedLengthInteger(await reader.readBytes(3));
  var sequenceId = decodeFixedLengthInteger1(await reader.readByte());

  // A packet containing a Protocol::LengthEncodedInteger column_count
  var columnCount = decodeFixedLengthInteger1(await reader.readByte());
  print("$loaded: columnCount: $columnCount");
  loaded += 1;

  if (loaded != payloadLength) {
    throw new StateError("$loaded != $payloadLength");
  }
}

Future readResultSetColumnDefinitionResponsePacket(
    DataStreamReader reader) async {
  var loaded = 0;

  var payloadLength = decodeFixedLengthInteger(await reader.readBytes(3));
  var sequenceId = decodeFixedLengthInteger1(await reader.readByte());

  // lenenc_str     catalog
  var catalogFirstByte = await reader.readByte();
  var catalogBytesLength =
      getDecodingLengthEncodedIntegerBytesLength(catalogFirstByte);
  var catalogLength = catalogBytesLength > 1
      ? decodeLengthEncodedInteger(
          await reader.readBytes(catalogBytesLength - 1))
      : catalogFirstByte;
  var catalog = decodeString(await reader.readBytes(catalogLength));
  loaded += catalogBytesLength + catalogLength;

  // lenenc_str     schema
  var schemaFirstByte = await reader.readByte();
  var schemaBytesLength =
      getDecodingLengthEncodedIntegerBytesLength(schemaFirstByte);
  var schemaLength = schemaBytesLength > 1
      ? decodeLengthEncodedInteger(
          await reader.readBytes(schemaBytesLength - 1))
      : schemaFirstByte;
  var schema = decodeString(await reader.readBytes(schemaLength));
  loaded += schemaBytesLength + schemaLength;

  // lenenc_str     table
  var tableFirstByte = await reader.readByte();
  var tableBytesLength =
      getDecodingLengthEncodedIntegerBytesLength(tableFirstByte);
  var tableLength = tableBytesLength > 1
      ? decodeLengthEncodedInteger(await reader.readBytes(tableBytesLength - 1))
      : tableFirstByte;
  var table = decodeString(await reader.readBytes(tableLength));
  loaded += tableBytesLength + tableLength;

  // lenenc_str     org_table
  var orgTableFirstByte = await reader.readByte();
  var orgTableBytesLength =
      getDecodingLengthEncodedIntegerBytesLength(orgTableFirstByte);
  var orgTableLength = orgTableBytesLength > 1
      ? decodeLengthEncodedInteger(
          await reader.readBytes(orgTableBytesLength - 1))
      : orgTableFirstByte;
  var orgTable = decodeString(await reader.readBytes(orgTableLength));
  loaded += orgTableBytesLength + orgTableLength;

  // lenenc_str     name
  var nameFirstByte = await reader.readByte();
  var nameBytesLength =
      getDecodingLengthEncodedIntegerBytesLength(nameFirstByte);
  var nameLength = nameBytesLength > 1
      ? decodeLengthEncodedInteger(await reader.readBytes(nameBytesLength - 1))
      : nameFirstByte;
  var name = decodeString(await reader.readBytes(nameLength));
  loaded += nameBytesLength + nameLength;

  // lenenc_str     org_name
  var orgNameFirstByte = await reader.readByte();
  var orgNameBytesLength =
      getDecodingLengthEncodedIntegerBytesLength(orgNameFirstByte);
  var orgNameLength = orgNameBytesLength > 1
      ? decodeLengthEncodedInteger(
          await reader.readBytes(orgNameBytesLength - 1))
      : orgNameFirstByte;
  var orgName = decodeString(await reader.readBytes(orgNameLength));
  loaded += orgNameBytesLength + orgNameLength;

  // lenenc_int     length of fixed-length fields [0c]
  var fieldsLengthFirstByte = await reader.readByte();
  var fieldsLengthBytesLength =
      getDecodingLengthEncodedIntegerBytesLength(fieldsLengthFirstByte);
  var fieldsLength = fieldsLengthBytesLength > 1
      ? decodeLengthEncodedInteger(
          await reader.readBytes(fieldsLengthBytesLength - 1))
      : fieldsLengthFirstByte;
  loaded += fieldsLengthBytesLength;

  // 2              character set
  var characterSet = decodeFixedLengthInteger(await reader.readBytes(2));
  loaded += 2;

  // 4              column length
  var columnLength = decodeFixedLengthInteger(await reader.readBytes(4));
  loaded += 4;

  // 1              type
  var type = decodeFixedLengthInteger1(await reader.readByte());
  loaded += 1;

  // 2              flags
  var flags = decodeFixedLengthInteger(await reader.readBytes(2));
  loaded += 2;

  // 1              decimals
  var decimals = decodeFixedLengthInteger1(await reader.readByte());
  loaded += 1;

  // 2              filler [00] [00]
  await reader.skipBytes(2);
  loaded += 2;

  if (loaded != payloadLength) {
    throw new StateError("$loaded != $payloadLength");
  }
}

Future<bool> readResultSetRowResponsePacket(DataStreamReader reader) async {
  var eof = false;
  var loaded = 0;

  var payloadLength = decodeFixedLengthInteger(await reader.readBytes(3));
  // var payloadLength = await reader.readFixedLengthInteger(3);

  var sequenceId = await reader.readByte();
  // TODO versione di un byte più veloce
  // var sequenceId = await reader.readFixedLengthInteger(1);

  reader.resetLoadedCount();

  while (loaded < payloadLength) {
    var columnFirstByte = await reader.readByte();
    if (columnFirstByte != 0xfb) {
      if (columnFirstByte == 0xfe && loaded == 0 && payloadLength < 8) {
        eof = true;
        loaded += 1;

        // if capabilities & CLIENT_PROTOCOL_41 {
        if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
          // int<2>	warnings	number of warnings
          var warnings = decodeFixedLengthInteger(await reader.readBytes(2));
          loaded += 2;
          // int<2>	status_flags	Status Flags
          var statusFlags = decodeFixedLengthInteger(await reader.readBytes(2));
          loaded += 2;
        }
      } else {
        var columnBytesLength =
            getDecodingLengthEncodedIntegerBytesLength(columnFirstByte);
        var columnLength = columnBytesLength > 1
            ? decodeLengthEncodedInteger(
                await reader.readBytes(columnBytesLength - 1))
            : columnFirstByte;

        var column = decodeString(await reader.readBytes(columnLength));

        loaded += columnBytesLength + columnLength;
      }
    } else {
      // NULL is sent as 0xfb
    }
  }

  if (loaded != payloadLength) {
    throw new StateError("$loaded != $payloadLength");
  }

  return eof;
}

Future<bool> readResultSetRowResponsePacket2(DataStreamReader reader) async {
  var eof = false;
  var loaded = 0;

  var buffer = await reader.readFixedLengthBuffer(3);

  var payloadLength = buffer.singleRange.data[buffer.singleRange.start];

  await reader.skipByte();

  while (loaded < payloadLength) {
    var columnFirstByte = await reader.readByte();
    if (columnFirstByte != 0xfb) {
      if (columnFirstByte == 0xfe && loaded == 0 && payloadLength < 8) {
        eof = true;
        loaded += 1;

        // if capabilities & CLIENT_PROTOCOL_41 {
        if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
          await reader.skipBytes(4);
          loaded += 4;
        }
      } else {
        await reader.skipBytes(payloadLength - loaded - 1);
        loaded = payloadLength;
      }
    } else {
      // NULL is sent as 0xfb
    }
  }

  if (loaded != payloadLength) {
    throw new StateError("$loaded != $payloadLength");
  }

  return eof;
}

Future readResponsePacket(DataStreamReader reader) async {
  var loaded = 0;

  var payloadLength = decodeFixedLengthInteger(await reader.readBytes(3));
  print("payloadLength: $payloadLength");

  var sequenceId = decodeFixedLengthInteger1(await reader.readByte());
  print("sequenceId: $sequenceId");

  // TODO solo per test iniziale
  if (loaded < payloadLength) {
    var data = await reader.readBytes(payloadLength - loaded);
    print("$loaded: last data: $data");
    loaded += data.length;
  }

  print("Loaded: $loaded");
  if (loaded != payloadLength) {
    throw new StateError("$loaded != $payloadLength");
  }
}

Future readEOFResponsePacket(DataStreamReader reader) async {
  var loaded = 0;

  var payloadLength = decodeFixedLengthInteger(await reader.readBytes(3));
  var sequenceId = decodeFixedLengthInteger1(await reader.readByte());

  // int<1>	header	[00] or [fe] the OK packet header
  var header = decodeFixedLengthInteger1(await reader.readByte());
  loaded += 1;

  if (header != 0xfe) {
    throw new StateError("$header != 0xfe");
  }

  // if capabilities & CLIENT_PROTOCOL_41 {
  if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
    // int<2>	warnings	number of warnings
    var warnings = decodeFixedLengthInteger(await reader.readBytes(2));
    loaded += 2;
    // int<2>	status_flags	Status Flags
    var statusFlags = decodeFixedLengthInteger(await reader.readBytes(2));
    loaded += 2;
  }

  if (loaded != payloadLength) {
    throw new StateError("$loaded != $payloadLength");
  }
}
