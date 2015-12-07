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

  ReaderBuffer _buffer;

  int serverCapabilityFlags;
  final int clientCapabilityFlags;

  PacketReader(this._reader, {this.clientCapabilityFlags: 0});

  Future<GenericResponsePacket> readCommandResponsePacket() async {
    _buffer = await _reader.readBuffer(4);
    var payloadLength = _buffer.readFixedLengthInteger(3);
    var sequenceId = _buffer.readOneLengthInteger();

    _buffer = await _reader.readBuffer(payloadLength);
    var header = _buffer.checkByte();
    if (_isOkHeader(header, payloadLength)) {
      return _completeOkPacket(new OkPacket(payloadLength, sequenceId));
    } else if (_isErrorHeader(header, payloadLength)) {
      throw new ResponseError(_completeErrorPacket(
          new ErrorPacket(payloadLength, sequenceId)));
    } else {
      throw new UnsupportedError(
          "header: $header, payloadLength: $payloadLength");
    }
  }

  Future<InitialHandshakePacket> readInitialHandshakePacket() async {
    _buffer = await _reader.readBuffer(4);
    var payloadLength = _buffer.readFixedLengthInteger(3);
    var sequenceId = _buffer.readOneLengthInteger();

    _buffer = await _reader.readBuffer(payloadLength);
    var header = _buffer.checkByte();
    if (_isErrorHeader(header, payloadLength)) {
      throw new ResponseError(_completeErrorPacket(
          new ErrorPacket(payloadLength, sequenceId)));
    } else {
      return _completeInitialHandshakePacket(
          new InitialHandshakePacket(payloadLength, sequenceId));
    }
  }

  Future readCommandQueryResponsePacket() async {
    _buffer = await _reader.readBuffer(4);
    var payloadLength = _buffer.readFixedLengthInteger(3);
    var sequenceId = _buffer.readOneLengthInteger();

    _buffer = await _reader.readBuffer(payloadLength);
    var header = _buffer.checkByte();
    if (_isOkHeader(header, payloadLength)) {
      return _completeOkPacket(new OkPacket(payloadLength, sequenceId));
    } else if (_isErrorHeader(header, payloadLength)) {
      throw new ResponseError(_completeErrorPacket(
          new ErrorPacket(payloadLength, sequenceId)));
    } else if (_isLocalInFileHeader(header, payloadLength)) {
      throw new UnsupportedError("Protocol::LOCAL_INFILE_Data");
    } else {
      return await _completeResultSetPacket(payloadLength, sequenceId);
    }
  }

  Future<ResultSetPacket> _completeResultSetPacket(
      int payloadLength, int sequenceId) async {

    _readResultSetColumnCountResponsePacket(payloadLength, sequenceId);

    var columnCount = 3;
    for (var i = 0; i < columnCount; i++) {
      await _readResultSetColumnDefinitionResponsePacket();
    }
    await _readEOFResponsePacket();

    try {
      while (true) {
        await _readResultSetRowResponsePacket();
      }
    } on EOFError {
      if (_buffer.isFirstByte) {
        // EOF packet
        // if capabilities & CLIENT_PROTOCOL_41 {
        if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
          // int<2>	warnings	number of warnings
          var warnings = _buffer.readFixedLengthInteger(2);
          // int<2>	status_flags	Status Flags
          var statusFlags = _buffer.readFixedLengthInteger(2);
        }
      } else {
        rethrow;
      }
    } on UndefinedError {
      if (_buffer.isFirstByte) {
        // TODO Error packet
        throw new UnsupportedError("IMPLEMENT STARTED ERROR PACKET");
      } else {
        rethrow;
      }
    }
  }

  void _readResultSetColumnCountResponsePacket(
      int payloadLength, int sequenceId) {
    // A packet containing a Protocol::LengthEncodedInteger column_count
    var columnCount = _buffer.readOneLengthInteger();
  }

  Future _readResultSetColumnDefinitionResponsePacket() async {
    _buffer = await _reader.readBuffer(4);
    var payloadLength = _buffer.readFixedLengthInteger(3);
    var sequenceId = _buffer.readOneLengthInteger();

    _buffer = await _reader.readBuffer(payloadLength);

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

  Future _readResultSetRowResponsePacket() async {
    _buffer = await _reader.readBuffer(4);
    var payloadLength = _buffer.readFixedLengthInteger(3);
    var sequenceId = _buffer.readOneLengthInteger();

    _buffer = await _reader.readBuffer(payloadLength);

    while (!_buffer.isAllRead) {
      var value;
      try {
        value = _buffer.readLengthEncodedString();
      } on NullError {
        value = null;
      }

      // print(value);
    }
  }

  Future _readEOFResponsePacket() async {
    _buffer = await _reader.readBuffer(4);
    var payloadLength = _buffer.readFixedLengthInteger(3);
    var sequenceId = _buffer.readOneLengthInteger();

    _buffer = await _reader.readBuffer(payloadLength);

    // int<1>	header	[00] or [fe] the OK packet header
    var header = _buffer.readOneLengthInteger();
    if (header != 0xfe) {
      throw new StateError("$header != 0xfe");
    }

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	warnings	number of warnings
      var warnings = _buffer.readFixedLengthInteger(2);
      // int<2>	status_flags	Status Flags
      var statusFlags = _buffer.readFixedLengthInteger(2);
    }
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

  OkPacket _completeOkPacket(OkPacket packet) {
    return _completeSuccessResponsePacket(packet);
  }

  EOFPacket _completeEOFPacket(EOFPacket packet) {
    // check CLIENT_DEPRECATE_EOF flag
    bool isEOFDeprecated = false;

    if (isEOFDeprecated) {
      return _completeSuccessResponsePacket(packet);
    } else {
      // if capabilities & CLIENT_PROTOCOL_41 {
      if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
        // int<2>	warnings	number of warnings
        packet.warnings = _buffer.readFixedLengthInteger(2);
        // int<2>	status_flags	Status Flags
        packet.statusFlags = _buffer.readFixedLengthInteger(2);
      }
    }

    return packet;
  }

  ErrorPacket _completeErrorPacket(ErrorPacket packet) {
    // int<2>	error_code	error-code
    packet.errorCode = _buffer.readFixedLengthInteger(2);
    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // string[1]	sql_state_marker	# marker of the SQL State
      packet.sqlStateMarker = _buffer.readFixedLengthString(1);
      // string[5]	sql_state	SQL State
      packet.sqlState = _buffer.readFixedLengthString(5);
    }
    // string<EOF>	error_message	human readable error message
    packet.errorMessage = _buffer.readRestOfPacketString();

    return packet;
  }

  SuccessResponsePacket _completeSuccessResponsePacket(
      SuccessResponsePacket packet) {
    // int<1>	header	[00] or [fe] the OK packet header
    packet.header = _buffer.readOneLengthInteger();
    // int<lenenc>	affected_rows	affected rows
    packet.affectedRows = _buffer.readLengthEncodedInteger();
    // int<lenenc>	last_insert_id	last insert-id
    packet.lastInsertId = _buffer.readLengthEncodedInteger();

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	status_flags	Status Flags
      packet.statusFlags = _buffer.readFixedLengthInteger(2);
      // int<2>	warnings	number of warnings
      packet.warnings = _buffer.readFixedLengthInteger(2);
      // } elseif capabilities & CLIENT_TRANSACTIONS {
    } else if (serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
      // int<2>	status_flags	Status Flags
      packet.statusFlags = _buffer.readFixedLengthInteger(2);
    } else {
      packet.statusFlags = 0;
    }

    // if capabilities & CLIENT_SESSION_TRACK {
    if (serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
      // string<lenenc>	info	human readable status information
      if (!_buffer.isAllRead) {
        packet.info = _buffer.readLengthEncodedString();
      }

      // if status_flags & SERVER_SESSION_STATE_CHANGED {
      if (packet.statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
        // string<lenenc>	session_state_changes	session state info
        if (!_buffer.isAllRead) {
          packet.sessionStateChanges = _buffer.readLengthEncodedString();
        }
      }
      // } else {
    } else {
      // string<EOF>	info	human readable status information
      packet.info = _buffer.readRestOfPacketString();
    }

    return packet;
  }

  InitialHandshakePacket _completeInitialHandshakePacket(
      InitialHandshakePacket packet) {
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
}
