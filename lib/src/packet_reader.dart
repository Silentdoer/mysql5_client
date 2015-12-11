library mysql_client.packet_reader;

import "dart:async";
import 'dart:math';

import "reader_buffer.dart";
import 'data_commons.dart';
import 'data_reader.dart';
import 'data_range.dart';

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
  final List<DataRange> _dataRanges;

  ResultSetColumnDefinitionResponsePacket.reusable()
      : _dataRanges = new List<DataRange>.filled(13, new DataRange.reusable());

  ResultSetColumnDefinitionResponsePacket reuse(PacketBuffer buffer) {
    var i = 0;

    // lenenc_str     catalog
    buffer.payload.readFixedLengthDataRange(
        buffer.payload.readLengthEncodedDataRange(_dataRanges[i++]).toInt(),
        _dataRanges[i]);
    // lenenc_str     schema
    buffer.payload.readFixedLengthDataRange(
        buffer.payload.readLengthEncodedDataRange(_dataRanges[i++]).toInt(),
        _dataRanges[i]);
    // lenenc_str     table
    buffer.payload.readFixedLengthDataRange(
        buffer.payload.readLengthEncodedDataRange(_dataRanges[i++]).toInt(),
        _dataRanges[i]);
    // lenenc_str     org_table
    buffer.payload.readFixedLengthDataRange(
        buffer.payload.readLengthEncodedDataRange(_dataRanges[i++]).toInt(),
        _dataRanges[i]);
    // lenenc_str     name
    buffer.payload.readFixedLengthDataRange(
        buffer.payload.readLengthEncodedDataRange(_dataRanges[i++]).toInt(),
        _dataRanges[i]);
    // lenenc_str     org_name
    buffer.payload.readFixedLengthDataRange(
        buffer.payload.readLengthEncodedDataRange(_dataRanges[i++]).toInt(),
        _dataRanges[i]);
    // lenenc_int     length of fixed-length fields [0c]
    buffer.payload.readLengthEncodedDataRange(_dataRanges[i++]);
    // 2              character set
    buffer.payload.readFixedLengthDataRange(2, _dataRanges[i++]);
    // 4              column length
    buffer.payload.readFixedLengthDataRange(4, _dataRanges[i++]);
    // 1              type
    buffer.payload.readFixedLengthDataRange(1, _dataRanges[i++]);
    // 2              flags
    buffer.payload.readFixedLengthDataRange(2, _dataRanges[i++]);
    // 1              decimals
    buffer.payload.readFixedLengthDataRange(1, _dataRanges[i++]);
    // 2              filler [00] [00]
    buffer.payload.readFixedLengthDataRange(2, _dataRanges[i++]);

    return this;
  }

  void free() {
    for (var range in _dataRanges) {
      range?.free();
    }
  }

  String get catalog => _getString(0);
  String get schema => _getString(1);
  String get table => _getString(2);
  String get orgTable => _getString(3);
  String get name => _getString(4);
  String get orgName => _getString(5);
  int get fieldsLength => _getInt(6);
  int get characterSet => _getInt(7);
  int get columnLength => _getInt(8);
  int get type => _getInt(9);
  int get flags => _getInt(10);
  int get decimals => _getInt(11);

  int _getInt(int index) => _dataRanges[index].toInt();

  String _getString(int index) => _dataRanges[index].toString();
}

class ResultSetRowResponsePacket extends Packet {
  final List<DataRange> _dataRanges;

  ResultSetRowResponsePacket.reusable(int columnCount)
      : _dataRanges =
            new List<DataRange>.filled(columnCount, new DataRange.reusable());

  ResultSetRowResponsePacket reuse(PacketBuffer buffer) {
    var i = 0;
    while (!buffer.payload.isAllRead) {
      if (buffer.payload.checkOneLengthInteger() != PREFIX_NULL) {
        var fieldLength = buffer.payload.readLengthEncodedInteger();
        buffer.payload.readFixedLengthDataRange(fieldLength, _dataRanges[i++]);
      } else {
        buffer.payload.skipByte();
        _dataRanges[i++].reuseNil();
      }
    }
    return this;
  }

  void free() {
    for (var range in _dataRanges) {
      range?.free();
    }
  }

  String getString(int index) => _dataRanges[index].toString();

  String getUTF8String(int index) => _dataRanges[index].toUTF8String();
}

class PacketBuffer {
  int _sequenceId;

  ReaderBuffer _payload;

  PacketBuffer(this._sequenceId, this._payload);

  PacketBuffer.reusable() : this._payload = new ReaderBuffer.reusable();

  PacketBuffer reuse(int sequenceId, ReaderBuffer payload) {
    _sequenceId = sequenceId;
    _payload = payload;

    return this;
  }

  void free() {
    _payload.free();
    _sequenceId = null;
  }

  int get sequenceId => _sequenceId;

  ReaderBuffer get payload => _payload;

  int get header => _payload.checkOneLengthInteger();

  int get payloadLength => _payload.payloadLength;
}

class PacketReader {
  final DataReader _reader;

  final ReaderBuffer _reusableHeaderReaderBuffer = new ReaderBuffer.reusable();
  final DataRange _reusablePayloadLengthDataRange = new DataRange.reusable();

  int serverCapabilityFlags;
  final int clientCapabilityFlags;

  PacketReader(this._reader, {this.clientCapabilityFlags: 0});

  Future<Packet> readInitialHandshakeResponse() => _readPacketFromBufferAsync(
      _readInitialHandshakeResponseInternal, new PacketBuffer.reusable());

  Future<Packet> readCommandResponse() => _readPacketFromBufferAsync(
      _readCommandResponseInternal, new PacketBuffer.reusable());

  Future<Packet> readCommandQueryResponse() => _readPacketFromBufferAsync(
      _readCommandQueryResponseInternal, new PacketBuffer.reusable());

  readResultSetColumnDefinitionResponse(
      ResultSetColumnDefinitionResponsePacket reusablePacket,
      PacketBuffer reusablePacketBuffer) {
    var value = _readPacketBuffer(reusablePacketBuffer);
    if (value is Future) {
      return value.then((buffer) =>
          _readResultSetColumnDefinitionResponseInternal(
              buffer, reusablePacket));
    } else {
      return _readResultSetColumnDefinitionResponseInternal(
          value, reusablePacket);
    }
  }

  readResultSetRowResponse(ResultSetRowResponsePacket reusablePacket,
      PacketBuffer reusablePacketBuffer) {
    var value = _readPacketBuffer(reusablePacketBuffer);
    if (value is Future) {
      return value.then((buffer) =>
          _readResultSetRowResponseInternal(buffer, reusablePacket));
    } else {
      return _readResultSetRowResponseInternal(value, reusablePacket);
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

  Packet _readResultSetColumnDefinitionResponseInternal(PacketBuffer buffer,
      ResultSetColumnDefinitionResponsePacket reusablePacket) {
    if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else if (_isEOFPacket(buffer)) {
      return _readEOFPacket(buffer);
    } else {
      return _readResultSetColumnDefinitionResponsePacket(
          buffer, reusablePacket);
    }
  }

  Packet _readResultSetRowResponseInternal(
      PacketBuffer buffer, ResultSetRowResponsePacket reusablePacket) {
    if (_isErrorPacket(buffer)) {
      return _readErrorPacket(buffer);
    } else if (_isEOFPacket(buffer)) {
      return _readEOFPacket(buffer);
    } else {
      return _readResultSetRowResponsePacket(buffer, reusablePacket);
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
          PacketBuffer buffer,
          ResultSetColumnDefinitionResponsePacket reusablePacket) =>
      reusablePacket.reuse(buffer);

  ResultSetRowResponsePacket _readResultSetRowResponsePacket(
          PacketBuffer buffer, ResultSetRowResponsePacket reusablePacket) =>
      reusablePacket.reuse(buffer);

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

  Future<Packet> _readPacketFromBufferAsync(
      Packet reader(PacketBuffer buffer), PacketBuffer reusablePacketBuffer) {
    var value = _readPacketFromBuffer(reader, reusablePacketBuffer);
    return value is Future ? value : new Future.value(value);
  }

  _readPacketFromBuffer(
      Packet reader(PacketBuffer buffer), PacketBuffer reusablePacketBuffer) {
    var value = _readPacketBuffer(reusablePacketBuffer);
    if (value is Future) {
      return value.then((buffer) => reader(buffer));
    } else {
      return reader(value);
    }
  }

  _readPacketBuffer(PacketBuffer reusablePacketBuffer) {
    var value = _reader.readBuffer(4, _reusableHeaderReaderBuffer);
    if (value is Future) {
      return value.then((headerReaderBuffer) =>
          _readPacketBufferInternal(reusablePacketBuffer));
    } else {
      return _readPacketBufferInternal(reusablePacketBuffer);
    }
  }

  _readPacketBufferInternal(PacketBuffer reusablePacketBuffer) {
    var payloadLength = _reusableHeaderReaderBuffer
        .readFixedLengthDataRange(3, _reusablePayloadLengthDataRange)
        .toInt();
    var sequenceId = _reusableHeaderReaderBuffer.readOneLengthInteger();
    _reusableHeaderReaderBuffer.free();
    _reusablePayloadLengthDataRange.free();

    var value = _reader.readBuffer(payloadLength, reusablePacketBuffer.payload);
    if (value is Future) {
      return value.then((payloadReaderBuffer) =>
          reusablePacketBuffer.reuse(sequenceId, payloadReaderBuffer));
    } else {
      return reusablePacketBuffer.reuse(sequenceId, value);
    }
  }
}
