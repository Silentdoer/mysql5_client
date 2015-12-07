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

class ResultSetPacket extends Packet {
  ResultSetPacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);
}

class PacketReader {
  final DataReader _reader;

  int serverCapabilityFlags;
  final int clientCapabilityFlags;

  PacketReader(this._reader, {this.clientCapabilityFlags: 0});

  Future<GenericResponsePacket> readCommandResponsePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);
    try {
      var header = buffer.checkByte();
      if (_isOkHeader(header, payloadLength)) {
        return _completeOkPacket(
            new OkPacket(payloadLength, sequenceId), buffer);
      } else if (_isErrorHeader(header, payloadLength)) {
        throw new ResponseError(_completeErrorPacket(
            new ErrorPacket(payloadLength, sequenceId), buffer));
      } else {
        throw new UnsupportedError(
            "header: $header, payloadLength: $payloadLength");
      }
    } finally {
      buffer.deinitialize();
    }
  }

  Future<InitialHandshakePacket> readInitialHandshakePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);
    try {
      var header = buffer.checkByte();
      if (_isErrorHeader(header, payloadLength)) {
        throw new ResponseError(_completeErrorPacket(
            new ErrorPacket(payloadLength, sequenceId), buffer));
      } else {
        return _completeInitialHandshakePacket(
            new InitialHandshakePacket(payloadLength, sequenceId), buffer);
      }
    } finally {
      buffer.deinitialize();
    }
  }

  Future readCommandQueryResponsePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);
    try {
      var header = buffer.checkByte();
      if (_isOkHeader(header, payloadLength)) {
        return _completeOkPacket(
            new OkPacket(payloadLength, sequenceId), buffer);
      } else if (_isErrorHeader(header, payloadLength)) {
        throw new ResponseError(_completeErrorPacket(
            new ErrorPacket(payloadLength, sequenceId), buffer));
      } else if (_isLocalInFileHeader(header, payloadLength)) {
        throw new UnsupportedError("Protocol::LOCAL_INFILE_Data");
      } else {
        return await _completeResultSetPacket(
            payloadLength, sequenceId, buffer);
      }
    } finally {
      buffer.deinitialize();
    }

    // TODO migliorare la gestione del risultato
  }

  Future<ResultSetPacket> _completeResultSetPacket(
      int payloadLength, int sequenceId, ReaderBuffer buffer) async {

    _readResultSetColumnCountResponsePacket(payloadLength, sequenceId, buffer);

    var columnCount = 3;
    for (var i = 0; i < columnCount; i++) {
      await _readResultSetColumnDefinitionResponsePacket();
    }
    await _readEOFResponsePacket();

    try {
      while (true) {
        await _readResultSetRowResponsePacket();
      }
    } on EOFError catch (e) {
      if (e.buffer.isFirstByte) {
        // EOF packet
        // if capabilities & CLIENT_PROTOCOL_41 {
        if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
          // int<2>	warnings	number of warnings
          var warnings = e.buffer.readFixedLengthInteger(2);
          // int<2>	status_flags	Status Flags
          var statusFlags = e.buffer.readFixedLengthInteger(2);
        }
        e.buffer.deinitialize();
      } else {
        e.buffer.deinitialize();

        rethrow;
      }
    } on UndefinedError catch (e) {
      if (e.buffer.isFirstByte) {
        // TODO Error packet

        e.buffer.deinitialize();

        throw new UnsupportedError("IMPLEMENT STARTED ERROR PACKET");
      } else {
        e.buffer.deinitialize();

        rethrow;
      }
    }
  }

  void _readResultSetColumnCountResponsePacket(
      int payloadLength, int sequenceId, ReaderBuffer buffer) {
    // A packet containing a Protocol::LengthEncodedInteger column_count
    var columnCount = buffer.readOneLengthInteger();
  }

  Future _readResultSetColumnDefinitionResponsePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);

    // lenenc_str     catalog
    var catalog = buffer.readLengthEncodedString();
    // lenenc_str     schema
    var schema = buffer.readLengthEncodedString();
    // lenenc_str     table
    var table = buffer.readLengthEncodedString();
    // lenenc_str     org_table
    var orgTable = buffer.readLengthEncodedString();
    // lenenc_str     name
    var name = buffer.readLengthEncodedString();
    // lenenc_str     org_name
    var orgName = buffer.readLengthEncodedString();
    // lenenc_int     length of fixed-length fields [0c]
    var fieldsLength = buffer.readLengthEncodedInteger();
    // 2              character set
    var characterSet = buffer.readFixedLengthInteger(2);
    // 4              column length
    var columnLength = buffer.readFixedLengthInteger(4);
    // 1              type
    var type = buffer.readOneLengthInteger();
    // 2              flags
    var flags = buffer.readFixedLengthInteger(2);
    // 1              decimals
    var decimals = buffer.readOneLengthInteger();
    // 2              filler [00] [00]
    buffer.skipBytes(2);

    buffer.deinitialize();
  }

  Future _readResultSetRowResponsePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);

    while (!buffer.isAllRead) {
      var value;
      try {
        value = buffer.readLengthEncodedString();
      } on NullError {
        value = null;
      }

      print(value);
    }

    buffer.deinitialize();
  }

  Future _readEOFResponsePacket() async {
    var headerBuffer = await _reader.readBuffer(4);
    var payloadLength = headerBuffer.readFixedLengthInteger(3);
    var sequenceId = headerBuffer.readOneLengthInteger();
    headerBuffer.deinitialize();

    var buffer = await _reader.readBuffer(payloadLength);

    // int<1>	header	[00] or [fe] the OK packet header
    var header = buffer.readOneLengthInteger();
    if (header != 0xfe) {
      throw new StateError("$header != 0xfe");
    }

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	warnings	number of warnings
      var warnings = buffer.readFixedLengthInteger(2);
      // int<2>	status_flags	Status Flags
      var statusFlags = buffer.readFixedLengthInteger(2);
    }

    buffer.deinitialize();
  }

  bool _isOkHeader(int header, int payloadLength) {
    return header == 0 && payloadLength >= 7;
  }

  bool _isEOFHeader(int header, int payloadLength) {
    return header == 0xfe && payloadLength < 9;
  }

  bool _isErrorHeader(int header, int payloadLength) {
    return header == 0xff;
  }

  bool _isLocalInFileHeader(int header, int payloadLength) {
    return header == 0xfb;
  }

  OkPacket _completeOkPacket(OkPacket packet, ReaderBuffer buffer) {
    return _completeSuccessResponsePacket(packet, buffer);
  }

  EOFPacket _completeEOFPacket(EOFPacket packet, ReaderBuffer buffer) {
    // check CLIENT_DEPRECATE_EOF flag
    bool isEOFDeprecated = false;

    if (isEOFDeprecated) {
      return _completeSuccessResponsePacket(packet, buffer);
    } else {
      // if capabilities & CLIENT_PROTOCOL_41 {
      if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
        // int<2>	warnings	number of warnings
        packet.warnings = buffer.readFixedLengthInteger(2);
        // int<2>	status_flags	Status Flags
        packet.statusFlags = buffer.readFixedLengthInteger(2);
      }
    }

    return packet;
  }

  ErrorPacket _completeErrorPacket(ErrorPacket packet, ReaderBuffer buffer) {
    // int<2>	error_code	error-code
    packet.errorCode = buffer.readFixedLengthInteger(2);
    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // string[1]	sql_state_marker	# marker of the SQL State
      packet.sqlStateMarker = buffer.readFixedLengthString(1);
      // string[5]	sql_state	SQL State
      packet.sqlState = buffer.readFixedLengthString(5);
    }
    // string<EOF>	error_message	human readable error message
    packet.errorMessage = buffer.readRestOfPacketString();

    return packet;
  }

  SuccessResponsePacket _completeSuccessResponsePacket(
      SuccessResponsePacket packet, ReaderBuffer buffer) {
    // int<1>	header	[00] or [fe] the OK packet header
    packet.header = buffer.readOneLengthInteger();
    // int<lenenc>	affected_rows	affected rows
    packet.affectedRows = buffer.readLengthEncodedInteger();
    // int<lenenc>	last_insert_id	last insert-id
    packet.lastInsertId = buffer.readLengthEncodedInteger();

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	status_flags	Status Flags
      packet.statusFlags = buffer.readFixedLengthInteger(2);
      // int<2>	warnings	number of warnings
      packet.warnings = buffer.readFixedLengthInteger(2);
      // } elseif capabilities & CLIENT_TRANSACTIONS {
    } else if (serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
      // int<2>	status_flags	Status Flags
      packet.statusFlags = buffer.readFixedLengthInteger(2);
    } else {
      packet.statusFlags = 0;
    }

    // if capabilities & CLIENT_SESSION_TRACK {
    if (serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
      // string<lenenc>	info	human readable status information
      if (!buffer.isAllRead) {
        packet.info = buffer.readLengthEncodedString();
      }

      // if status_flags & SERVER_SESSION_STATE_CHANGED {
      if (packet.statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
        // string<lenenc>	session_state_changes	session state info
        if (!buffer.isAllRead) {
          packet.sessionStateChanges = buffer.readLengthEncodedString();
        }
      }
      // } else {
    } else {
      // string<EOF>	info	human readable status information
      packet.info = buffer.readRestOfPacketString();
    }

    return packet;
  }

  InitialHandshakePacket _completeInitialHandshakePacket(
      InitialHandshakePacket packet, ReaderBuffer buffer) {
    // 1              [0a] protocol version
    packet.protocolVersion = buffer.readOneLengthInteger();
    // string[NUL]    server version
    packet.serverVersion = buffer.readNulTerminatedString();
    // 4              connection id
    packet.connectionId = buffer.readFixedLengthInteger(4);
    // string[8]      auth-plugin-data-part-1
    packet.authPluginDataPart1 = buffer.readFixedLengthString(8);
    // 1              [00] filler
    buffer.skipByte();
    // 2              capability flags (lower 2 bytes)
    packet.capabilityFlags1 = buffer.readFixedLengthInteger(2);
    // if more data in the packet:
    if (!buffer.isAllRead) {
      // 1              character set
      packet.characterSet = buffer.readOneLengthInteger();
      // 2              status flags
      packet.statusFlags = buffer.readFixedLengthInteger(2);
      // 2              capability flags (upper 2 bytes)
      packet.capabilityFlags2 = buffer.readFixedLengthInteger(2);
      packet.serverCapabilityFlags =
          packet.capabilityFlags1 | (packet.capabilityFlags2 << 16);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // 1              length of auth-plugin-data
        packet.authPluginDataLength = buffer.readOneLengthInteger();
      } else {
        // 1              [00]
        buffer.skipByte();
        packet.authPluginDataLength = 0;
      }
      // string[10]     reserved (all [00])
      buffer.skipBytes(10);
      // if capabilities & CLIENT_SECURE_CONNECTION {
      if (packet.serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
        // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
        var len = max(packet.authPluginDataLength - 8, 13);
        packet.authPluginDataPart2 = buffer.readFixedLengthString(len);
      } else {
        packet.authPluginDataPart2 = "";
      }
      packet.authPluginData =
          "${packet.authPluginDataPart1}${packet.authPluginDataPart2}"
              .substring(0, packet.authPluginDataLength - 1);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet.serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // string[NUL]    auth-plugin name
        packet.authPluginName = buffer.readNulTerminatedString();
      }
    }

    return packet;
  }
}
