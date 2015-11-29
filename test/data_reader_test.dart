// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";
import "dart:math";
import "dart:typed_data";

import 'package:mysql_client/mysql_client.dart';

// sudo ngrep -x -q -d lo0 '' 'port 3306'


const int _MAX_INT_1 = 251;
final int _MAX_INT_2 = pow(2, 2 * 8);
const int _PREFIX_INT_2 = 0xfc;
final int _MAX_INT_3 = pow(2, 3 * 8);
const int _PREFIX_INT_3 = 0xfd;
final int _MAX_INT_8 = pow(2, 8 * 8);
const int _PREFIX_INT_8 = 0xfe;

Future main() async {

  var controller = new StreamController<List<int>>();
  controller
      .add([]); // TODO gestire anche il caso di una lista vuota (non si sa mai)
  controller.add([1]);
  controller.add([1, 2, 3]);
  controller.add([4, 5, 6]);

  var reader = new DataReader(controller.stream);
  print(await reader.readByte());
  print(await reader.readBytes(4));
  print(await reader.readByte());
  print(await reader.readByte());

/*
  print(await reader.readBytesUpTo(6));
  print(await reader.readByte());
*/
  print(pow(2, 16));
  print(pow(2, 24));
  print(pow(2, 64));

  testLengthEncodedInteger(10000);

  testLengthEncodedInteger(0);
  testLengthEncodedInteger(1);
  testLengthEncodedInteger(_MAX_INT_1 - 1);
  testLengthEncodedInteger(_MAX_INT_1);
  testLengthEncodedInteger(255);
  testLengthEncodedInteger(256);
  testLengthEncodedInteger(_MAX_INT_2 - 1);
  testLengthEncodedInteger(_MAX_INT_2);
  testLengthEncodedInteger(_MAX_INT_3 - 1);
  testLengthEncodedInteger(_MAX_INT_3);
  testLengthEncodedInteger(_MAX_INT_8 - 1);

  testLengthEncodedString("");
  testLengthEncodedString("asd");
  testLengthEncodedString("asdlasdl");

  testFixedLengthInteger(0, 1);
  testFixedLengthInteger(255, 1);
  testFixedLengthInteger(256, 2);
  testFixedLengthInteger(pow(2, 8 * 8) - 1, 8);

  var packets = [];
  packets.add([0x36, 0x00, 0x00, 0x00, 0x0a, 0x35, 0x2e, 0x35, 0x2e, 0x32, 0x2d, 0x6d, 0x32, 0x00, 0x0b, 0x00]);
  packets.add([0x00, 0x00, 0x64, 0x76, 0x48, 0x40, 0x49, 0x2d, 0x43, 0x4a, 0x00, 0xff, 0xf7, 0x08, 0x02, 0x00]);
  packets.add([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x34, 0x64]);
  packets.add([0x7c, 0x63, 0x5a, 0x77, 0x6b, 0x34, 0x5e, 0x5d, 0x3a, 0x00]);

  var stream2 = new Stream.fromIterable(packets);

  var reader2 = new DataReader(stream2);
  var payloadLength = decodeFixedLengthInteger(await reader2.readBytes(3));
  print("payloadLength: $payloadLength");
  var sequenceId = await reader2.readByte();
  print("sequenceId: $sequenceId");

  var loaded = 0;
  var protocolVersion = await reader2.readByte();
  print("$loaded: protocolVersion: $protocolVersion");
  loaded += 1;
  var serverVersion = decodeString(await reader2.readBytesUpTo(0x00));
  print("$loaded: serverVersion: $serverVersion");
  loaded += serverVersion.length + 1;
  var connectionId = decodeFixedLengthInteger(await reader2.readBytes(4));
  print("$loaded: connectionId: $connectionId");
  loaded += 4;
  var authPluginDataPart1 = decodeString(await reader2.readBytes(8));
  print("$loaded: authPluginDataPart1: $authPluginDataPart1");
  loaded += 8;
  await reader2.skipByte();
  print("$loaded: filler: SKIPPED");
  loaded += 1;
  var capabilityFlags1 = decodeFixedLengthInteger(await reader2.readBytes(2));
  print("$loaded: capabilityFlags1: $capabilityFlags1");
  loaded += 2;

  // if more data in the packet:
  if (true) {
    var characterSet = await reader2.readByte();
    print("$loaded: characterSet: $characterSet");
    loaded += 1;
    var statusFlags = decodeFixedLengthInteger(await reader2.readBytes(2));
    print("$loaded: statusFlags: $statusFlags");
    loaded += 2;
    var capabilityFlags2 = decodeFixedLengthInteger(await reader2.readBytes(2));
    print("$loaded: capabilityFlags2: $capabilityFlags2");
    loaded += 2;

    var capabilityFlags = capabilityFlags1 | (capabilityFlags2 << 16);
    print("capabilityFlags: $capabilityFlags");

    var authPluginDataLength = 0;
    // if capabilities & CLIENT_PLUGIN_AUTH {
    if (capabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
      authPluginDataLength = await reader2.readByte();
      print("$loaded: authPluginDataLength: $authPluginDataLength");
      loaded += 1;
    } else {
      await reader2.readByte();
      print("$loaded: filler: SKIPPED");
      loaded += 1;
    }

    await reader2.skipBytes(10);
    print("$loaded: reserved: SKIPPED");
    loaded += 10;

    // if capabilities & CLIENT_SECURE_CONNECTION {
    if (capabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
      var len = max(authPluginDataLength - 8, 13);
      var authPluginDataPart2 = decodeString(await reader2.readBytes(len));
      print("$loaded: authPluginDataPart2: $authPluginDataPart2");
      loaded += len;
    }

    // if capabilities & CLIENT_PLUGIN_AUTH {
    if (capabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
      var authPluginName = decodeString(await reader2.readBytesUpTo(0x00));
      print("$loaded: authPluginName: $authPluginName");
      loaded += authPluginName.length + 1;
    }

    print("Loaded: $loaded");
  }

  var sw = new Stopwatch();
  sw.start();
  for (var i = 0; i < 3000000; i++) {
    decodeFixedLengthInteger([0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01]);
  }
  print("Elapsed in ${sw.elapsedMilliseconds} ms");

/*
  var list = [254, 0, 0, 0, 1, 0, 0, 0, 0];
  var sw = new Stopwatch();
  sw.start();
  for (var i = 0; i < 1000000; i++) {
    decodeLengthEncodedInteger(list);
  }
  print("Elapsed in ${sw.elapsedMilliseconds} ms");
  sw.reset();
  for (var i = 0; i < 1000000; i++) {
    decodeFixedLengthInteger2(list);
  }
  print("Elapsed in ${sw.elapsedMilliseconds} ms");
  sw.reset();
  for (var i = 0; i < 1000000; i++) {
    encodeLengthEncodedInteger(18446744073709551615);
  }
  print("Elapsed in ${sw.elapsedMilliseconds} ms");
  sw.reset();
  for (var i = 0; i < 1000000; i++) {
    encodeFixedLengthInteger2(18446744073709551615);
  }
  print("Elapsed in ${sw.elapsedMilliseconds} ms");
*/
}


void testFixedLengthInteger(int value, int length) {
  print("Encode $value");
  var encoded = encodeFixedLengthInteger(value, length);
  print("Encoded $encoded");
  var decoded = decodeFixedLengthInteger(encoded);
  print("Decoded $decoded");
  if (value != decoded) {
    throw new StateError("$value != $decoded");
  }
}

void testLengthEncodedInteger(int value) {
  print("Encode $value");
  var encoded = encodeLengthEncodedInteger(value);
  print("Encoded $encoded");

  var encodedFirstByte = encoded[0];
  var encodedBytesLength = getDecodingLengthEncodedIntegerBytesLength(encodedFirstByte);
  if (encodedBytesLength != encoded.length) {
    throw new StateError("$encodedBytesLength != ${encoded.length}");
  }

  var decoded = encodedBytesLength > 1 ? decodeLengthEncodedInteger(encoded.sublist(1, 1 + encodedBytesLength - 1)) : encodedFirstByte;
  print("Decoded $decoded");
  if (value != decoded) {
    throw new StateError("$value != $decoded");
  }
}

void testLengthEncodedString(String value) {
  print("Encode $value");
  var encoded = encodeLengthEncodedString(value);
  print("Encoded $encoded");

  var encodedFirstByte = encoded[0];
  var encodedBytesLength = getDecodingLengthEncodedIntegerBytesLength(encodedFirstByte);
  var decodedLength = encodedBytesLength > 1 ? decodeLengthEncodedInteger(encoded.sublist(1, 1 + encodedBytesLength - 1)) : encodedFirstByte;
  var decoded = decodeString(encoded.sublist(1, 1 + encodedBytesLength - 1 + decodedLength));

  print("Decoded $decoded");
  if (value != decoded) {
    throw new StateError("$value != $decoded");
  }
}


