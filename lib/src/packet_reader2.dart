library mysql_client.packet_reader;

import "dart:async";
import 'dart:math';

import "data_commons.dart";
import "reader_buffer.dart";
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
  final int payloadLength;
  final int sequenceId;

  Packet(this.payloadLength, this.sequenceId);
}

abstract class GenericResponsePacket extends Packet {
  int header;
  String info;
  String sessionStateChanges;

  GenericResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

abstract class SuccessResponsePacket extends GenericResponsePacket {
  int affectedRows;
  int lastInsertId;
  int statusFlags;
  int warnings;

  SuccessResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class OkPacket extends SuccessResponsePacket {
  OkPacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class EOFPacket extends SuccessResponsePacket {
  EOFPacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class ErrorPacket extends GenericResponsePacket {
  int errorCode;
  String sqlStateMarker;
  String sqlState;
  String errorMessage;

  ErrorPacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
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

  InitialHandshakePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class ResultSetColumnCountResponsePacket extends Packet {
  int columnCount;

  ResultSetColumnCountResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class ResultSetColumnDefinitionResponsePacket extends Packet {
  ResultSetColumnDefinitionResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class ResultSetRowResponsePacket extends Packet {
  ResultSetRowResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class PacketBuffer {
  final int sequenceId;

  final ReaderBuffer payload;

  PacketBuffer(this.sequenceId, ReaderBuffer payload) : this.payload = payload;

  int get header => payload.first;

  int get payloadLength => this.payload.payloadLength;
}

class PacketReader {
  final DataReader _reader;

  int serverCapabilityFlags;
  final int clientCapabilityFlags;

  PacketReader(this._reader, {this.clientCapabilityFlags: 0});

  Future<Packet> readInitialHandshakeResponse() =>
      _readSyncPacket(_readInitialHandshakeResponse);

  Future<Packet> readCommandResponse() => _readSyncPacket(_readCommandResponse);

  Future<Packet> readCommandQueryResponse() =>
      _readSyncPacket(_readCommandQueryResponse);

  Future<Packet> readResultSetColumnDefinitionResponse() =>
      _readSyncPacket(_readResultSetColumnDefinitionResponse);

  Future<Packet> readResultSetRowResponse() =>
      _readSyncPacket(_readResultSetRowResponse);

  Packet _readInitialHandshakeResponse(PacketBuffer buffer) {
    if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else {
      return _readInitialHandshakePacket(buffer);
    }
  }

  Packet _readCommandResponse(PacketBuffer buffer) {
    if (_isOkPacket(buffer)) {
      return _readOkPacket(buffer);
    } else if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else {
      throw new UnsupportedError("header: ${buffer.header}");
    }
  }

  Packet _readCommandQueryResponse(PacketBuffer buffer) {
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

  Packet _readResultSetColumnDefinitionResponse(PacketBuffer buffer) {
    if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else if (_isEOFPacket(buffer)) {
      return _readEOFPacket(buffer);
    } else {
      return _readResultSetColumnDefinitionResponsePacket(buffer);
    }
  }

  Packet _readResultSetRowResponse(PacketBuffer buffer) {
    if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else if (_isEOFPacket(buffer)) {
      return _readEOFPacket(buffer);
    } else {
      return _readResultSetRowResponsePacket(buffer);
    }
  }

  InitialHandshakePacket _readInitialHandshakePacket(PacketBuffer buffer) {
    var packet =
        new InitialHandshakePacket(buffer.payloadLength, buffer.sequenceId);

    var _buffer = buffer.payload;

    // 1              [0a] protocol version
    packet.protocolVersion = _buffer.readOneLengthInteger();
    // string[NUL]    server version
    packet.serverVersion = _buffer.readNulTerminatedString();
    // 4              connection id
    packet.connectionId = _buffer.readFixedLengthInteger(4);
    // string[8]      auth-plugin-data-part-1
    packet.authPluginDataPart1 = _buffer.readFixedLengthString(8);
    // 1              [00] filler
    _buffer.skipByte();
    // 2              capability flags (lower 2 bytes)
    packet.capabilityFlags1 = _buffer.readFixedLengthInteger(2);
    // if more data in the packet:
    if (!_buffer.isAllRead) {
      // 1              character set
      packet.characterSet = _buffer.readOneLengthInteger();
      // 2              status flags
      packet.statusFlags = _buffer.readFixedLengthInteger(2);
      // 2              capability flags (upper 2 bytes)
      packet.capabilityFlags2 = _buffer.readFixedLengthInteger(2);
      packet.serverCapabilityFlags =
          packet.capabilityFlags1 | (packet.capabilityFlags2 << 16);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // 1              length of auth-plugin-data
        packet.authPluginDataLength = _buffer.readOneLengthInteger();
      } else {
        // 1              [00]
        _buffer.skipByte();
        packet.authPluginDataLength = 0;
      }
      // string[10]     reserved (all [00])
      _buffer.skipBytes(10);
      // if capabilities & CLIENT_SECURE_CONNECTION {
      if (packet.serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
        // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
        var len = max(packet.authPluginDataLength - 8, 13);
        packet.authPluginDataPart2 = _buffer.readFixedLengthString(len);
      } else {
        packet.authPluginDataPart2 = "";
      }
      packet.authPluginData =
          "${packet.authPluginDataPart1}${packet.authPluginDataPart2}"
              .substring(0, packet.authPluginDataLength - 1);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // string[NUL]    auth-plugin name
        packet.authPluginName = _buffer.readNulTerminatedString();
      }
    }
    return packet;
  }

  ResultSetColumnCountResponsePacket _readResultSetColumnCountResponsePacket(
      PacketBuffer buffer) {
    var packet = new ResultSetColumnCountResponsePacket(
        buffer.payloadLength, buffer.sequenceId);

    packet.columnCount = buffer.payload.readOneLengthInteger();

    return packet;
  }

  ResultSetColumnDefinitionResponsePacket _readResultSetColumnDefinitionResponsePacket(
      PacketBuffer buffer) {
    var packet = new ResultSetColumnDefinitionResponsePacket(
        buffer.payloadLength, buffer.sequenceId);

    buffer.payload.skipBytes(buffer.payloadLength);

    return packet;
  }

  ResultSetRowResponsePacket _readResultSetRowResponsePacket(
      PacketBuffer buffer) {
    var packet =
        new ResultSetRowResponsePacket(buffer.payloadLength, buffer.sequenceId);

    while (!buffer.payload.isAllRead) {
      var value;
      try {
        value = buffer.payload.readLengthEncodedString();
      } on NullError {
        value = null;
      }
    }

    return packet;
  }

  bool _isOkPacket(PacketBuffer buffer) {
    return buffer.header == 0 && buffer.payloadLength >= 7;
  }

  bool _isEOFPacket(PacketBuffer buffer) {
    return buffer.header == 0xfe && buffer.payloadLength < 9;
  }

  bool _isErrorPacket(PacketBuffer buffer) {
    return buffer.header == 0xff;
  }

  bool _isLocalInFilePacket(PacketBuffer buffer) {
    return buffer.header == 0xfb;
  }

  OkPacket _readOkPacket(PacketBuffer buffer) {
    var packet = new OkPacket(buffer.payloadLength, buffer.sequenceId);

    _completeSuccessResponsePacket(packet, buffer);

    return packet;
  }

  ErrorPacket _readErrorPacket(PacketBuffer buffer) {
    var packet = new ErrorPacket(buffer.payloadLength, buffer.sequenceId);

    buffer.payload.skipBytes(buffer.payloadLength);

    return packet;
  }

  EOFPacket _readEOFPacket(PacketBuffer buffer) {
    var packet = new EOFPacket(buffer.payloadLength, buffer.sequenceId);

    // check CLIENT_DEPRECATE_EOF flag
    bool isEOFDeprecated = false;

    if (isEOFDeprecated) {
      _completeSuccessResponsePacket(packet, buffer);
    } else {
      buffer.payload.skipBytes(packet.payloadLength);
    }

    return packet;
  }

  void _completeSuccessResponsePacket(
      SuccessResponsePacket packet, PacketBuffer buffer) {
    buffer.payload.skipBytes(buffer.payloadLength);
  }

  Future<Packet> _readSyncPacket(Packet reader(PacketBuffer buffer)) {
    var value = _readPacketBuffer();
    if (value is Future) {
      return value.then((buffer) => reader(buffer));
    } else {
      return new Future.value(reader(value));
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
