// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';
import 'package:mysql_client/src/reader_buffer.dart';
import 'package:mysql_client/src/data_chunk.dart';

// sudo ngrep -x -q -d lo0 '' 'port 3306'

class Packet {
  final int sequenceId;
  final ReaderBuffer payload;
  Packet(this.sequenceId, this.payload);
}

Future main() async {
  await test1();

  // await test2();

  // await test3();

  await test6();
}

Future test1() async {
  var header = [46, 0, 0, 3];
  var payload = [
    3,
    100,
    101,
    102,
    4,
    116,
    101,
    115,
    116,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    4,
    110,
    97,
    109,
    101,
    4,
    110,
    97,
    109,
    101,
    12,
    33,
    0,
    253,
    2,
    0,
    0,
    253,
    0,
    0,
    0,
    0,
    0
  ];
  var sw = new Stopwatch();
  sw.start();
  for (var i = 0; i < 1000000; i++) {
    await read1(header, payload);
  }
  print(sw.elapsedMilliseconds);
}

Future test2() async {
  var header = [46, 0, 0, 3];
  var payload = [
    3,
    100,
    101,
    102,
    4,
    116,
    101,
    115,
    116,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    4,
    110,
    97,
    109,
    101,
    4,
    110,
    97,
    109,
    101,
    12,
    33,
    0,
    253,
    2,
    0,
    0,
    253,
    0,
    0,
    0,
    0,
    0
  ];
  var sw = new Stopwatch();
  sw.start();
  for (var i = 0; i < 1000000; i++) {
    await read2(header, payload);
  }
  print(sw.elapsedMilliseconds);
}

Future test3() async {
  var header = [46, 0, 0, 3];
  var payload = [
    3,
    100,
    101,
    102,
    4,
    116,
    101,
    115,
    116,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    4,
    110,
    97,
    109,
    101,
    4,
    110,
    97,
    109,
    101,
    12,
    33,
    0,
    253,
    2,
    0,
    0,
    253,
    0,
    0,
    0,
    0,
    0
  ];
  var sw = new Stopwatch();
  sw.start();
  for (var i = 0; i < 1000000; i++) {
    await read3(header, payload);
  }
  print(sw.elapsedMilliseconds);
}

Future test4() async {
  var header = [46, 0, 0, 3];
  var payload = [
    3,
    100,
    101,
    102,
    4,
    116,
    101,
    115,
    116,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    4,
    110,
    97,
    109,
    101,
    4,
    110,
    97,
    109,
    101,
    12,
    33,
    0,
    253,
    2,
    0,
    0,
    253,
    0,
    0,
    0,
    0,
    0
  ];
  var sw = new Stopwatch();
  sw.start();
  for (var i = 0; i < 1000000; i++) {
    read4(header, payload);
  }
  print(sw.elapsedMilliseconds);
}

Future test5() async {
  var header = [46, 0, 0, 3];
  var payload = [
    3,
    100,
    101,
    102,
    4,
    116,
    101,
    115,
    116,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    4,
    110,
    97,
    109,
    101,
    4,
    110,
    97,
    109,
    101,
    12,
    33,
    0,
    253,
    2,
    0,
    0,
    253,
    0,
    0,
    0,
    0,
    0
  ];
  var sw = new Stopwatch();
  sw.start();
  for (var i = 0; i < 1000000; i++) {
    await read5(header, payload);
  }
  print(sw.elapsedMilliseconds);
}

Future test6() async {
  var header = [46, 0, 0, 3];
  var payload = [
    3,
    100,
    101,
    102,
    4,
    116,
    101,
    115,
    116,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    6,
    112,
    101,
    111,
    112,
    108,
    101,
    4,
    110,
    97,
    109,
    101,
    4,
    110,
    97,
    109,
    101,
    12,
    33,
    0,
    253,
    2,
    0,
    0,
    253,
    0,
    0,
    0,
    0,
    0
  ];
  var sw = new Stopwatch();
  sw.start();
  for (var i = 0; i < 1000000; i++) {
    var value = readPacket(header, payload);
    if (value is Future) {
      await value.then((packet) => read6(packet));
    } else {
      read6(value);
    }
  }
  print(sw.elapsedMilliseconds);
}

Future read1(List<int> header, List<int> payload) async {
  var _buffer = await createBuffer(header);

  var payloadLength = _buffer.readFixedLengthInteger(3);
  var sequenceId = _buffer.readOneLengthInteger();

  _buffer = await createBuffer(payload);

  // lenenc_str     catalog
  var catalog = _buffer.readLengthEncodedString();
  // lenenc_str     schema
  var schema = _buffer.readLengthEncodedString();
  // lenenc_str     table
  var table = _buffer.readLengthEncodedString();
  // lenenc_str     org_table
  var orgTable = _buffer.readLengthEncodedString();
  // lenenc_str     name
  var name = _buffer.readLengthEncodedString();
  // lenenc_str     org_name
  var orgName = _buffer.readLengthEncodedString();
  // lenenc_int     length of fixed-length fields [0c]
  var fieldsLength = _buffer.readLengthEncodedInteger();
  // 2              character set
  var characterSet = _buffer.readFixedLengthInteger(2);
  // 4              column length
  var columnLength = _buffer.readFixedLengthInteger(4);
  // 1              type
  var type = _buffer.readOneLengthInteger();
  // 2              flags
  var flags = _buffer.readFixedLengthInteger(2);
  // 1              decimals
  var decimals = _buffer.readOneLengthInteger();
  // 2              filler [00] [00]
  _buffer.skipBytes(2);
}

Future read2(List<int> header, List<int> payload) async {
  var _buffer = await createBuffer(header);

  var payloadLength = _buffer.readFixedLengthInteger(3);
  var sequenceId = _buffer.readOneLengthInteger();

  _buffer = await createBuffer(payload);

  _buffer.checkByte();

  _buffer.skipBytes(payloadLength);
}

Future read3(List<int> header, List<int> payload) async {
  var _buffer = await createBuffer(header);

  var payloadLength = _buffer.readFixedLengthInteger(3);
  var sequenceId = _buffer.readOneLengthInteger();

  _buffer = await createBuffer(payload);

  // lenenc_str     catalog
  var catalogLength = _buffer.readLengthEncodedInteger();
  _buffer.skipBytes(catalogLength);

  // lenenc_str     schema
  var schemaLength = _buffer.readLengthEncodedInteger();
  _buffer.skipBytes(schemaLength);

  // lenenc_str     table
  var tableLength = _buffer.readLengthEncodedInteger();
  _buffer.skipBytes(tableLength);

  // lenenc_str     org_table
  var orgTableLength = _buffer.readLengthEncodedInteger();
  _buffer.skipBytes(orgTableLength);

  // lenenc_str     name
  var nameLength = _buffer.readLengthEncodedInteger();
  _buffer.skipBytes(nameLength);

  // lenenc_str     org_name
  var orgNameLength = _buffer.readLengthEncodedInteger();
  _buffer.skipBytes(orgNameLength);

  // lenenc_int     length of fixed-length fields [0c]
  var fieldsLength = _buffer.readLengthEncodedInteger();

  // 2              character set
  _buffer.skipBytes(2);
  // 4              column length
  _buffer.skipBytes(4);
  // 1              type
  _buffer.skipBytes(1);
  // 2              flags
  _buffer.skipBytes(2);
  // 1              decimals
  _buffer.skipBytes(1);
  // 2              filler [00] [00]
  _buffer.skipBytes(2);
}

void read4(List<int> header, List<int> payload) {
  var _buffer = createBuffer2(header);

  var payloadLength = _buffer.readFixedLengthInteger(3);
  var sequenceId = _buffer.readOneLengthInteger();

  _buffer = createBuffer2(payload);

  // lenenc_str     catalog
  var catalog = _buffer.readLengthEncodedString();
  // lenenc_str     schema
  var schema = _buffer.readLengthEncodedString();
  // lenenc_str     table
  var table = _buffer.readLengthEncodedString();
  // lenenc_str     org_table
  var orgTable = _buffer.readLengthEncodedString();
  // lenenc_str     name
  var name = _buffer.readLengthEncodedString();
  // lenenc_str     org_name
  var orgName = _buffer.readLengthEncodedString();
  // lenenc_int     length of fixed-length fields [0c]
  var fieldsLength = _buffer.readLengthEncodedInteger();
  // 2              character set
  var characterSet = _buffer.readFixedLengthInteger(2);
  // 4              column length
  var columnLength = _buffer.readFixedLengthInteger(4);
  // 1              type
  var type = _buffer.readOneLengthInteger();
  // 2              flags
  var flags = _buffer.readFixedLengthInteger(2);
  // 1              decimals
  var decimals = _buffer.readOneLengthInteger();
  // 2              filler [00] [00]
  _buffer.skipBytes(2);
}

read5(List<int> header, List<int> payload) {
  var value = createBuffer2(header);
  if (value is Future) {
    return value.then((header) {
      read51(header, payload);
    });
  } else {
    read51(value, payload);
  }
}

read51(ReaderBuffer _buffer, List<int> payload) {
  var payloadLength = _buffer.readFixedLengthInteger(3);
  var sequenceId = _buffer.readOneLengthInteger();

  var value = createBuffer2(payload);
  if (value is Future) {
    return value.then((payload) {
      read52(sequenceId, payload);
    });
  } else {
    read52(sequenceId, value);
  }
}

void read52(int sequenceId, ReaderBuffer _buffer) {
  // lenenc_str     catalog
  var catalog = _buffer.readLengthEncodedString();
  // lenenc_str     schema
  var schema = _buffer.readLengthEncodedString();
  // lenenc_str     table
  var table = _buffer.readLengthEncodedString();
  // lenenc_str     org_table
  var orgTable = _buffer.readLengthEncodedString();
  // lenenc_str     name
  var name = _buffer.readLengthEncodedString();
  // lenenc_str     org_name
  var orgName = _buffer.readLengthEncodedString();
  // lenenc_int     length of fixed-length fields [0c]
  var fieldsLength = _buffer.readLengthEncodedInteger();
  // 2              character set
  var characterSet = _buffer.readFixedLengthInteger(2);
  // 4              column length
  var columnLength = _buffer.readFixedLengthInteger(4);
  // 1              type
  var type = _buffer.readOneLengthInteger();
  // 2              flags
  var flags = _buffer.readFixedLengthInteger(2);
  // 1              decimals
  var decimals = _buffer.readOneLengthInteger();
  // 2              filler [00] [00]
  _buffer.skipBytes(2);
}

void read6(Packet packet) {
  var _buffer = packet.payload;

  // lenenc_str     catalog
  var catalog = _buffer.readLengthEncodedString();
  // lenenc_str     schema
  var schema = _buffer.readLengthEncodedString();
  // lenenc_str     table
  var table = _buffer.readLengthEncodedString();
  // lenenc_str     org_table
  var orgTable = _buffer.readLengthEncodedString();
  // lenenc_str     name
  var name = _buffer.readLengthEncodedString();
  // lenenc_str     org_name
  var orgName = _buffer.readLengthEncodedString();
  // lenenc_int     length of fixed-length fields [0c]
  var fieldsLength = _buffer.readLengthEncodedInteger();
  // 2              character set
  var characterSet = _buffer.readFixedLengthInteger(2);
  // 4              column length
  var columnLength = _buffer.readFixedLengthInteger(4);
  // 1              type
  var type = _buffer.readOneLengthInteger();
  // 2              flags
  var flags = _buffer.readFixedLengthInteger(2);
  // 1              decimals
  var decimals = _buffer.readOneLengthInteger();
  // 2              filler [00] [00]
  _buffer.skipBytes(2);
}

readPacket(List<int> header, List<int> payload) {
  var value = createBuffer2(header);
  if (value is Future) {
    return value.then((header) {
      return readPacket2(header, payload);
    });
  } else {
    return readPacket2(value, payload);
  }
}

readPacket2(ReaderBuffer _buffer, List<int> payload) {
  var payloadLength = _buffer.readFixedLengthInteger(3);
  var sequenceId = _buffer.readOneLengthInteger();

  var value = createBuffer2(payload);
  if (value is Future) {
    return value.then((payload) => new Packet(sequenceId, payload));
  } else {
    return new Packet(sequenceId, value);
  }
}

Future<ReaderBuffer> createBuffer(List<int> data) async {
  var buffer = new ReaderBuffer(data.length);
  buffer.loadChunk(new DataChunk(data));
  return buffer;
}

ReaderBuffer createBuffer2(List<int> data) {
  var buffer = new ReaderBuffer(data.length);
  buffer.loadChunk(new DataChunk(data));
  return buffer;
}
