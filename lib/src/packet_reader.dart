library mysql_client.packet_reader;

import "dart:async";
import 'dart:math';

import "reader_buffer.dart";
import 'data_commons.dart';
import 'data_reader.dart';

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

class ResponseError extends Error {
  final ErrorPacket packet;

  ResponseError(this.packet);
}

abstract class Packet {
  int payloadLength;
  int sequenceId;
}

abstract class GenericResponsePacket extends Packet {
  int header;
  String info;
  String sessionStateChanges;
}

abstract class SuccessResponsePacket extends GenericResponsePacket {
  int affectedRows;
  int lastInsertId;
  int statusFlags;
  int warnings;
}

class OkPacket extends SuccessResponsePacket {}

class EOFPacket extends SuccessResponsePacket {}

class ErrorPacket extends GenericResponsePacket {
  int errorCode;
  String sqlStateMarker;
  String sqlState;
  String errorMessage;
}

class InitialHandshakePacket extends Packet {
  int protocolVersion;
  String serverVersion;
  int connectionId;
  String authPluginDataPart1;
  int capabilityFlags1;
  int characterSet;
  int statusFlags;
  int capabilityFlags2;
  int serverCapabilityFlags;
  int authPluginDataLength;
  String authPluginDataPart2;
  String authPluginData;
  String authPluginName;
}

class ResultSetColumnCountResponsePacket extends Packet {
  int columnCount;
}

class ResultSetColumnDefinitionResponsePacket extends Packet {
  String catalog;
  String schema;
  String table;
  String orgTable;
  String name;
  String orgName;
  int fieldsLength;
  int characterSet;
  int columnLength;
  int type;
  int flags;
  int decimals;
}

class ResultSetRowResponsePacket extends Packet {
  List<String> values;
}

class PacketBuffer {
  final int sequenceId;

  final ReaderBuffer payload;

  PacketBuffer(this.sequenceId, this.payload);

  int get header => payload.checkOneLengthInteger();

  int get payloadLength => this.payload.payloadLength;
}

class PacketReader {
  final DataReader _reader;

  int serverCapabilityFlags;
  final int clientCapabilityFlags;

  PacketReader(this._reader, {this.clientCapabilityFlags: 0});

  Future<Packet> readInitialHandshakeResponse() =>
      _readSyncPacketFromBuffer(_readInitialHandshakeResponseInternal);

  Future<Packet> readCommandResponse() =>
      _readSyncPacketFromBuffer(_readCommandResponseInternal);

  Future<Packet> readCommandQueryResponse() =>
      _readSyncPacketFromBuffer(_readCommandQueryResponseInternal);

  Future<Packet> readResultSetColumnDefinitionResponse() =>
      _readSyncPacketFromBuffer(_readResultSetColumnDefinitionResponseInternal);

  Future<Packet> readResultSetRowResponse() =>
      _readSyncPacketFromBuffer(_readResultSetRowResponseInternal);

  Future<List<Packet>> readResultSetRowResponses([int length]) async =>
      _readResultSetRowResponses(new List<Packet>(), length);

  _readResultSetRowResponse() =>
      _readPacketFromBuffer(_readResultSetRowResponseInternal);

  _readResultSetRowResponses(List<Packet> packets, [int length]) {
    var value = _readResultSetRowResponse();
    if (value is Future) {
      return value.then((packet) =>
          _readResultSetRowResponsesInternal(packets, packet, length));
    } else {
      return _readResultSetRowResponsesInternal(packets, value, length);
    }
  }

  _readResultSetRowResponsesInternal(List<Packet> packets, Packet packet,
      [int length]) {
    if (packet is ResultSetRowResponsePacket) {
      packets.add(packet);

      if (length == null || packets.length < length) {
        return _readResultSetRowResponses(packets, length);
      } else {
        return packets;
      }
    } else {
      return packets;
    }
  }

  bool _isOkPacket(PacketBuffer buffer) =>
      buffer.header == 0 && buffer.payloadLength >= 7;

  bool _isEOFPacket(PacketBuffer buffer) =>
      buffer.header == 0xfe && buffer.payloadLength < 9;

  bool _isErrorPacket(PacketBuffer buffer) => buffer.header == 0xff;

  bool _isLocalInFilePacket(PacketBuffer buffer) => buffer.header == 0xfb;

  Packet _readCommandResponseInternal(PacketBuffer buffer) {
    if (_isOkPacket(buffer)) {
      return _readOkPacket(buffer);
    } else if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else {
      throw new UnsupportedError("header: ${buffer.header}");
    }
  }

  Packet _readInitialHandshakeResponseInternal(PacketBuffer buffer) {
    if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else {
      return _readInitialHandshakePacket(buffer);
    }
  }

  Packet _readCommandQueryResponseInternal(PacketBuffer buffer) {
    if (_isOkPacket(buffer)) {
      return _readOkPacket(buffer);
    } else if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else if (_isLocalInFilePacket(buffer)) {
      throw new UnsupportedError("Protocol::LOCAL_INFILE_Data");
    } else {
      return _readResultSetColumnCountResponsePacket(buffer);
    }
  }

  Packet _readResultSetColumnDefinitionResponseInternal(PacketBuffer buffer) {
    if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else if (_isEOFPacket(buffer)) {
      return _readEOFPacket(buffer);
    } else {
      return _readResultSetColumnDefinitionResponsePacket(buffer);
    }
  }

  Packet _readResultSetRowResponseInternal(PacketBuffer buffer) {
    if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else if (_isEOFPacket(buffer)) {
      return _readEOFPacket(buffer);
    } else {
      return _readResultSetRowResponsePacket(buffer);
    }
  }

  InitialHandshakePacket _readInitialHandshakePacket(PacketBuffer buffer) {
    var packet = new InitialHandshakePacket();

    // 1              [0a] protocol version
    packet.protocolVersion = buffer.payload.readOneLengthInteger();
    // string[NUL]    server version
    packet.serverVersion = buffer.payload.readNulTerminatedString();
    // 4              connection id
    packet.connectionId = buffer.payload.readFixedLengthInteger(4);
    // string[8]      auth-plugin-data-part-1
    packet.authPluginDataPart1 = buffer.payload.readFixedLengthString(8);
    // 1              [00] filler
    buffer.payload.skipByte();
    // 2              capability flags (lower 2 bytes)
    packet.capabilityFlags1 = buffer.payload.readFixedLengthInteger(2);
    // if more data in the packet:
    if (!buffer.payload.isAllRead) {
      // 1              character set
      packet.characterSet = buffer.payload.readOneLengthInteger();
      // 2              status flags
      packet.statusFlags = buffer.payload.readFixedLengthInteger(2);
      // 2              capability flags (upper 2 bytes)
      packet.capabilityFlags2 = buffer.payload.readFixedLengthInteger(2);
      packet.serverCapabilityFlags =
          packet.capabilityFlags1 | (packet.capabilityFlags2 << 16);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // 1              length of auth-plugin-data
        packet.authPluginDataLength = buffer.payload.readOneLengthInteger();
      } else {
        // 1              [00]
        buffer.payload.skipByte();
        packet.authPluginDataLength = 0;
      }
      // string[10]     reserved (all [00])
      buffer.payload.skipBytes(10);
      // if capabilities & CLIENT_SECURE_CONNECTION {
      if (packet.serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
        // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
        var len = max(packet.authPluginDataLength - 8, 13);
        packet.authPluginDataPart2 = buffer.payload.readFixedLengthString(len);
      } else {
        packet.authPluginDataPart2 = "";
      }
      packet.authPluginData =
          "${packet.authPluginDataPart1}${packet.authPluginDataPart2}"
              .substring(0, packet.authPluginDataLength - 1);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // string[NUL]    auth-plugin name
        packet.authPluginName = buffer.payload.readNulTerminatedString();
      }
    }

    return packet;
  }

  ResultSetColumnCountResponsePacket _readResultSetColumnCountResponsePacket(
      PacketBuffer buffer) {
    var packet = new ResultSetColumnCountResponsePacket();

    packet.columnCount = buffer.payload.readOneLengthInteger();

    return packet;
  }

  ResultSetColumnDefinitionResponsePacket _readResultSetColumnDefinitionResponsePacket(
      PacketBuffer buffer) {
    var packet = new ResultSetColumnDefinitionResponsePacket();

    // lenenc_str     catalog
    packet.catalog = buffer.payload.readLengthEncodedString();
    // lenenc_str     schema
    packet.schema = buffer.payload.readLengthEncodedString();
    // lenenc_str     table
    packet.table = buffer.payload.readLengthEncodedString();
    // lenenc_str     org_table
    packet.orgTable = buffer.payload.readLengthEncodedString();
    // lenenc_str     name
    packet.name = buffer.payload.readLengthEncodedString();
    // lenenc_str     org_name
    packet.orgName = buffer.payload.readLengthEncodedString();
    // lenenc_int     length of fixed-length fields [0c]
    packet.fieldsLength = buffer.payload.readLengthEncodedInteger();
    // 2              character set
    packet.characterSet = buffer.payload.readFixedLengthInteger(2);
    // 4              column length
    packet.columnLength = buffer.payload.readFixedLengthInteger(4);
    // 1              type
    packet.type = buffer.payload.readOneLengthInteger();
    // 2              flags
    packet.flags = buffer.payload.readFixedLengthInteger(2);
    // 1              decimals
    packet.decimals = buffer.payload.readOneLengthInteger();
    // 2              filler [00] [00]
    buffer.payload.skipBytes(2);

    return packet;
  }

  ResultSetRowResponsePacket _readResultSetRowResponsePacket(
      PacketBuffer buffer) {
    var packet = new ResultSetRowResponsePacket();

    packet.values = [];

    while (!buffer.payload.isAllRead) {
      var value;
      if (buffer.payload.checkOneLengthInteger() != PREFIX_NULL) {
        value = buffer.payload.readLengthEncodedString();
        // var fieldLength = buffer.payload.readLengthEncodedInteger();
        // buffer.payload.skipBytes(fieldLength);
      } else {
        buffer.payload.skipByte();
        value = null;
      }
      packet.values.add(value);
    }

    return packet;
  }

  OkPacket _readOkPacket(PacketBuffer buffer) {
    var packet = new OkPacket();

    packet.sequenceId = buffer.sequenceId;
    packet.payloadLength = buffer.payloadLength;

    _completeSuccessResponsePacket(packet, buffer);

    return packet;
  }

  EOFPacket _readEOFPacket(PacketBuffer buffer) {
    var packet = new EOFPacket();

    packet.sequenceId = buffer.sequenceId;
    packet.payloadLength = buffer.payloadLength;

    // check CLIENT_DEPRECATE_EOF flag
    bool isEOFDeprecated = false;

    if (isEOFDeprecated) {
      _completeSuccessResponsePacket(packet, buffer);
    } else {
      // EOF packet
      // if capabilities & CLIENT_PROTOCOL_41 {
      if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
        // int<2>	warnings	number of warnings
        packet.warnings = buffer.payload.readFixedLengthInteger(2);
        // int<2>	status_flags	Status Flags
        packet.statusFlags = buffer.payload.readFixedLengthInteger(2);
      }
    }

    return packet;
  }

  ErrorPacket _readErrorPacket(PacketBuffer buffer) {
    var packet = new ErrorPacket();

    packet.sequenceId = buffer.sequenceId;
    packet.payloadLength = buffer.payloadLength;

    // int<2>	error_code	error-code
    packet.errorCode = buffer.payload.readFixedLengthInteger(2);
    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // string[1]	sql_state_marker	# marker of the SQL State
      packet.sqlStateMarker = buffer.payload.readFixedLengthString(1);
      // string[5]	sql_state	SQL State
      packet.sqlState = buffer.payload.readFixedLengthString(5);
    }
    // string<EOF>	error_message	human readable error message
    packet.errorMessage = buffer.payload.readRestOfPacketString();

    return packet;
  }

  void _completeSuccessResponsePacket(
      SuccessResponsePacket packet, PacketBuffer buffer) {
    // int<1>	header	[00] or [fe] the OK packet header
    packet.header = buffer.payload.readOneLengthInteger();
    // int<lenenc>	affected_rows	affected rows
    packet.affectedRows = buffer.payload.readLengthEncodedInteger();
    // int<lenenc>	last_insert_id	last insert-id
    packet.lastInsertId = buffer.payload.readLengthEncodedInteger();

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	status_flags	Status Flags
      packet.statusFlags = buffer.payload.readFixedLengthInteger(2);
      // int<2>	warnings	number of warnings
      packet.warnings = buffer.payload.readFixedLengthInteger(2);
      // } elseif capabilities & CLIENT_TRANSACTIONS {
    } else if (serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
      // int<2>	status_flags	Status Flags
      packet.statusFlags = buffer.payload.readFixedLengthInteger(2);
    } else {
      packet.statusFlags = 0;
    }

    // if capabilities & CLIENT_SESSION_TRACK {
    if (serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
      // string<lenenc>	info	human readable status information
      if (!buffer.payload.isAllRead) {
        packet.info = buffer.payload.readLengthEncodedString();
      }

      // if status_flags & SERVER_SESSION_STATE_CHANGED {
      if (packet.statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
        // string<lenenc>	session_state_changes	session state info
        if (!buffer.payload.isAllRead) {
          packet.sessionStateChanges = buffer.payload.readLengthEncodedString();
        }
      }
      // } else {
    } else {
      // string<EOF>	info	human readable status information
      packet.info = buffer.payload.readRestOfPacketString();
    }
  }

  Future<Packet> _readSyncPacketFromBuffer(Packet reader(PacketBuffer buffer)) {
    var value = _readPacketBuffer();
    if (value is Future) {
      return value.then((buffer) => reader(buffer));
    } else {
      return new Future.value(reader(value));
    }
  }

  _readPacketFromBuffer(Packet reader(PacketBuffer buffer)) {
    var value = _readPacketBuffer();
    if (value is Future) {
      return value.then((buffer) => reader(buffer));
    } else {
      return reader(value);
    }
  }

  _readPacketBuffer() {
    var value = _reader.readBuffer(4);
    if (value is Future) {
      return value.then((header) => _readPacketBufferInternal(header));
    } else {
      return _readPacketBufferInternal(value);
    }
  }

  _readPacketBufferInternal(ReaderBuffer header) {
    var payloadLength = header.readFixedLengthInteger(3);
    var sequenceId = header.readOneLengthInteger();

    var value = _reader.readBuffer(payloadLength);

    if (value is Future) {
      return value.then((payload) => new PacketBuffer(sequenceId, payload));
    } else {
      return new PacketBuffer(sequenceId, value);
    }
  }
}
