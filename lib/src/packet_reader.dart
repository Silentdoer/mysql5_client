library mysql_client.packet_reader;

import "dart:async";
import 'dart:math';

import 'package:mysql_client/src/data_commons.dart';
import 'package:mysql_client/src/data_range.dart';
import "package:mysql_client/src/reader_buffer.dart";
import 'package:mysql_client/src/data_reader.dart';
import "package:mysql_client/src/packet_buffer.dart";

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

class PacketReader {
  final DataReader _reader;

  final PacketBuffer _reusablePacketBuffer = new PacketBuffer.reusable();
  final ReaderBuffer _reusableHeaderReaderBuffer = new ReaderBuffer.reusable();
  final DataRange _reusableDataRange = new DataRange.reusable();

  int serverCapabilityFlags;
  final int clientCapabilityFlags;

  PacketReader(this._reader, {this.clientCapabilityFlags: 0});

  Future<Packet> readInitialHandshakeResponse() =>
      _readPacketFromBufferAsync(_readInitialHandshakeResponseInternal);

  Future<Packet> readCommandResponse() =>
      _readPacketFromBufferAsync(_readCommandResponseInternal);

  Future<Packet> readCommandQueryResponse() =>
      _readPacketFromBufferAsync(_readCommandQueryResponseInternal);

  readResultSetColumnDefinitionResponse(
      ResultSetColumnDefinitionResponsePacket reusablePacket) {
    var value = _readPacketBuffer();
    if (value is Future) {
      return value.then((_) =>
          _readResultSetColumnDefinitionResponseInternal(reusablePacket));
    } else {
      return _readResultSetColumnDefinitionResponseInternal(reusablePacket);
    }
  }

  readResultSetRowResponse(ResultSetRowResponsePacket reusablePacket) {
    var value = _readPacketBuffer();
    if (value is Future) {
      return value
          .then((_) => _readResultSetRowResponseInternal(reusablePacket));
    } else {
      return _readResultSetRowResponseInternal(reusablePacket);
    }
  }

  bool _isOkPacket() => _reusablePacketBuffer.header == 0 &&
      _reusablePacketBuffer.payloadLength >= 7;

  bool _isEOFPacket() => _reusablePacketBuffer.header == 0xfe &&
      _reusablePacketBuffer.payloadLength < 9;

  bool _isErrorPacket() => _reusablePacketBuffer.header == 0xff;

  bool _isLocalInFilePacket() => _reusablePacketBuffer.header == 0xfb;

  Packet _readCommandResponseInternal() {
    if (_isOkPacket()) {
      return _readOkPacket();
    } else if (_isErrorPacket()) {
      return _readErrorPacket();
    } else {
      throw new UnsupportedError("header: ${_reusablePacketBuffer.header}");
    }
  }

  Packet _readInitialHandshakeResponseInternal() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else {
      return _readInitialHandshakePacket();
    }
  }

  Packet _readCommandQueryResponseInternal() {
    if (_isOkPacket()) {
      return _readOkPacket();
    } else if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isLocalInFilePacket()) {
      throw new UnsupportedError("Protocol::LOCAL_INFILE_Data");
    } else {
      return _readResultSetColumnCountResponsePacket();
    }
  }

  Packet _readResultSetColumnDefinitionResponseInternal(
      ResultSetColumnDefinitionResponsePacket reusablePacket) {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _readResultSetColumnDefinitionResponsePacket(reusablePacket);
    }
  }

  Packet _readResultSetRowResponseInternal(
      ResultSetRowResponsePacket reusablePacket) {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _readResultSetRowResponsePacket(reusablePacket);
    }
  }

  OkPacket _readOkPacket() {
    var packet = new OkPacket();

    packet.sequenceId = _reusablePacketBuffer.sequenceId;
    packet.payloadLength = _reusablePacketBuffer.payloadLength;

    _completeSuccessResponsePacket(packet);

    return packet;
  }

  EOFPacket _readEOFPacket() {
    var packet = new EOFPacket();

    packet.sequenceId = _reusablePacketBuffer.sequenceId;
    packet.payloadLength = _reusablePacketBuffer.payloadLength;

    // check CLIENT_DEPRECATE_EOF flag
    bool isEOFDeprecated = false;

    if (isEOFDeprecated) {
      _completeSuccessResponsePacket(packet);
    } else {
      // EOF packet
      // if capabilities & CLIENT_PROTOCOL_41 {
      if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
        // int<2>	warnings	number of warnings
        packet.warnings = _reusablePacketBuffer.payload
            .readFixedLengthDataRange(2, _reusableDataRange)
            .toInt();
        // int<2>	status_flags	Status Flags
        packet.statusFlags = _reusablePacketBuffer.payload
            .readFixedLengthDataRange(2, _reusableDataRange)
            .toInt();
      }
    }

    return packet;
  }

  ErrorPacket _readErrorPacket() {
    var packet = new ErrorPacket();

    packet.sequenceId = _reusablePacketBuffer.sequenceId;
    packet.payloadLength = _reusablePacketBuffer.payloadLength;

    // int<2>	error_code	error-code
    packet.errorCode = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _reusableDataRange)
        .toInt();
    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // string[1]	sql_state_marker	# marker of the SQL State
      packet.sqlStateMarker = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(1, _reusableDataRange)
          .toString();
      // string[5]	sql_state	SQL State
      packet.sqlState = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(5, _reusableDataRange)
          .toString();
    }
    // string<EOF>	error_message	human readable error message
    packet.errorMessage = _reusablePacketBuffer.payload
        .readNulTerminatedDataRange(_reusableDataRange)
        .toString();

    return packet;
  }

  void _completeSuccessResponsePacket(SuccessResponsePacket packet) {
    // int<1>	header	[00] or [fe] the OK packet header
    packet.header = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _reusableDataRange)
        .toInt();
    // int<lenenc>	affected_rows	affected rows
    packet.affectedRows = _reusablePacketBuffer.payload
        .readLengthEncodedDataRange(_reusableDataRange)
        .toInt();
    // int<lenenc>	last_insert_id	last insert-id
    packet.lastInsertId = _reusablePacketBuffer.payload
        .readLengthEncodedDataRange(_reusableDataRange)
        .toInt();

    // if capabilities & CLIENT_PROTOCOL_41 {
    if (serverCapabilityFlags & CLIENT_PROTOCOL_41 != 0) {
      // int<2>	status_flags	Status Flags
      packet.statusFlags = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _reusableDataRange)
          .toInt();
      // int<2>	warnings	number of warnings
      packet.warnings = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _reusableDataRange)
          .toInt();
      // } elseif capabilities & CLIENT_TRANSACTIONS {
    } else if (serverCapabilityFlags & CLIENT_TRANSACTIONS != 0) {
      // int<2>	status_flags	Status Flags
      packet.statusFlags = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _reusableDataRange)
          .toInt();
    } else {
      packet.statusFlags = 0;
    }

    // if capabilities & CLIENT_SESSION_TRACK {
    if (serverCapabilityFlags & CLIENT_SESSION_TRACK != 0) {
      // string<lenenc>	info	human readable status information
      if (!_reusablePacketBuffer.payload.isAllRead) {
        packet.info = _reusablePacketBuffer.payload
            .readFixedLengthDataRange(
                _reusablePacketBuffer.payload
                    .readLengthEncodedDataRange(_reusableDataRange)
                    .toInt(),
                _reusableDataRange)
            .toString();
      }

      // if status_flags & SERVER_SESSION_STATE_CHANGED {
      if (packet.statusFlags & SERVER_SESSION_STATE_CHANGED != 0) {
        // string<lenenc>	session_state_changes	session state info
        if (!_reusablePacketBuffer.payload.isAllRead) {
          packet.sessionStateChanges = _reusablePacketBuffer.payload
              .readFixedLengthDataRange(
                  _reusablePacketBuffer.payload
                      .readLengthEncodedDataRange(_reusableDataRange)
                      .toInt(),
                  _reusableDataRange)
              .toString();
        }
      }
      // } else {
    } else {
      // string<EOF>	info	human readable status information
      packet.info = _reusablePacketBuffer.payload
          .readNulTerminatedDataRange(_reusableDataRange)
          .toString();
    }
  }

  InitialHandshakePacket _readInitialHandshakePacket() {
    var packet = new InitialHandshakePacket();

    // 1              [0a] protocol version
    packet._protocolVersion = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _reusableDataRange)
        .toInt();
    // string[NUL]    server version
    packet._serverVersion = _reusablePacketBuffer.payload
        .readNulTerminatedDataRange(_reusableDataRange)
        .toString();
    // 4              connection id
    packet._connectionId = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(4, _reusableDataRange)
        .toInt();
    // string[8]      auth-plugin-data-part-1
    packet._authPluginDataPart1 = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(8, _reusableDataRange)
        .toString();
    // 1              [00] filler
    _reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _reusableDataRange);
    // 2              capability flags (lower 2 bytes)
    packet._capabilityFlags1 = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _reusableDataRange)
        .toInt();
    // if more data in the packet:
    if (!_reusablePacketBuffer.payload.isAllRead) {
      // 1              character set
      packet._characterSet = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(1, _reusableDataRange)
          .toInt();
      // 2              status flags
      packet._statusFlags = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _reusableDataRange)
          .toInt();
      // 2              capability flags (upper 2 bytes)
      packet._capabilityFlags2 = _reusablePacketBuffer.payload
          .readFixedLengthDataRange(2, _reusableDataRange)
          .toInt();
      packet._serverCapabilityFlags =
          packet.capabilityFlags1 | (packet.capabilityFlags2 << 16);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet._serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // 1              length of auth-plugin-data
        packet._authPluginDataLength = _reusablePacketBuffer.payload
            .readFixedLengthDataRange(1, _reusableDataRange)
            .toInt();
      } else {
        // 1              [00]
        _reusablePacketBuffer.payload
            .readFixedLengthDataRange(1, _reusableDataRange);
        packet._authPluginDataLength = 0;
      }
      // string[10]     reserved (all [00])
      _reusablePacketBuffer.payload
          .readFixedLengthDataRange(10, _reusableDataRange);
      // if capabilities & CLIENT_SECURE_CONNECTION {
      if (packet._serverCapabilityFlags & CLIENT_SECURE_CONNECTION != 0) {
        // string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
        var len = max(packet._authPluginDataLength - 8, 13);
        packet._authPluginDataPart2 = _reusablePacketBuffer.payload
            .readFixedLengthDataRange(len, _reusableDataRange)
            .toString();
      } else {
        packet.authPluginDataPart2 = "";
      }
      packet._authPluginData =
          "${packet._authPluginDataPart1}${packet._authPluginDataPart2}"
              .substring(0, packet._authPluginDataLength - 1);
      // if capabilities & CLIENT_PLUGIN_AUTH {
      if (packet._serverCapabilityFlags & CLIENT_PLUGIN_AUTH != 0) {
        // string[NUL]    auth-plugin name
        packet._authPluginName = _reusablePacketBuffer.payload
            .readNulTerminatedDataRange(_reusableDataRange)
            .toString();
      }
    }

    return packet;
  }

  ResultSetColumnCountResponsePacket _readResultSetColumnCountResponsePacket() {
    var packet = new ResultSetColumnCountResponsePacket();

    // A packet containing a Protocol::LengthEncodedInteger column_count
    packet._columnCount = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _reusableDataRange)
        .toInt();

    return packet;
  }

  ResultSetColumnDefinitionResponsePacket _readResultSetColumnDefinitionResponsePacket(
      ResultSetColumnDefinitionResponsePacket reusablePacket) {
    var dataRange;
    var i = 0;
    // lenenc_str     catalog
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     schema
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     table
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     org_table
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     name
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_str     org_name
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(
        _reusablePacketBuffer.payload
            .readLengthEncodedDataRange(dataRange)
            .toInt(),
        dataRange);
    // lenenc_int     length of fixed-length fields [0c]
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readLengthEncodedDataRange(dataRange);
    // 2              character set
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(2, dataRange);
    // 4              column length
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(4, dataRange);
    // 1              type
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(1, dataRange);
    // 2              flags
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(2, dataRange);
    // 1              decimals
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(1, dataRange);
    // 2              filler [00] [00]
    dataRange = reusablePacket.getReusableRange(i++);
    _reusablePacketBuffer.payload.readFixedLengthDataRange(2, dataRange);

    return reusablePacket.reuse();
  }

  ResultSetRowResponsePacket _readResultSetRowResponsePacket(
      ResultSetRowResponsePacket reusablePacket) {
    var i = 0;
    while (!_reusablePacketBuffer.payload.isAllRead) {
      var reusableRange = reusablePacket.getReusableRange(i++);
      if (_reusablePacketBuffer.payload.checkOneLengthInteger() !=
          PREFIX_NULL) {
        _reusablePacketBuffer.payload.readFixedLengthDataRange(
            _reusablePacketBuffer.payload
                .readLengthEncodedDataRange(reusableRange)
                .toInt(),
            reusableRange);
      } else {
        _reusablePacketBuffer.payload.skipByte();
        reusableRange.reuseNil();
      }
    }

    return reusablePacket.reuse();
  }

  Future<Packet> _readPacketFromBufferAsync(Packet reader()) {
    var value = _readPacketFromBuffer(reader);
    return value is Future ? value : new Future.value(value);
  }

  _readPacketFromBuffer(Packet reader()) {
    var value = _readPacketBuffer();
    if (value is Future) {
      return value.then((_) => reader());
    } else {
      return reader();
    }
  }

  _readPacketBuffer() {
    var value = _reader.readBuffer(4, _reusableHeaderReaderBuffer);
    if (value is Future) {
      return value.then((headerReaderBuffer) => _readPacketBufferInternal());
    } else {
      return _readPacketBufferInternal();
    }
  }

  _readPacketBufferInternal() {
    var payloadLength = _reusableHeaderReaderBuffer
        .readFixedLengthDataRange(3, _reusableDataRange)
        .toInt();
    var sequenceId = _reusableHeaderReaderBuffer.readOneLengthInteger();

    var value =
        _reader.readBuffer(payloadLength, _reusablePacketBuffer.payload);
    if (value is Future) {
      return value.then((payloadReaderBuffer) =>
          _reusablePacketBuffer.reuse(sequenceId, payloadReaderBuffer));
    } else {
      return _reusablePacketBuffer.reuse(sequenceId, value);
    }
  }
}

abstract class Packet {
  int payloadLength;
  int sequenceId;
}

class ReusablePacket extends Packet {
  final List<DataRange> _dataRanges;

  ReusablePacket.reusable(int rangeCount)
      : _dataRanges =
            new List<DataRange>.filled(rangeCount, new DataRange.reusable());

  DataRange getReusableRange(int i) => _dataRanges[i];

  void free() {
    for (var range in _dataRanges) {
      range?.free();
    }
  }

  int _getInt(int index) => _dataRanges[index].toInt();

  String _getString(int index) => _dataRanges[index].toString();

  String _getUTF8String(int index) => _dataRanges[index].toUTF8String();
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
  int _protocolVersion;
  String _serverVersion;
  int _connectionId;
  String _authPluginDataPart1;
  int _capabilityFlags1;
  int _characterSet;
  int _statusFlags;
  int _capabilityFlags2;
  int _serverCapabilityFlags;
  int _authPluginDataLength;
  String _authPluginDataPart2;
  String _authPluginData;
  String _authPluginName;

  int get protocolVersion => _protocolVersion;
  String get serverVersion => _serverVersion;
  int get connectionId => _connectionId;
  String get authPluginDataPart1 => _authPluginDataPart1;
  int get capabilityFlags1 => _capabilityFlags1;
  int get characterSet => _characterSet;
  int get statusFlags => _statusFlags;
  int get capabilityFlags2 => _capabilityFlags2;
  int get serverCapabilityFlags => _serverCapabilityFlags;
  int get authPluginDataLength => _authPluginDataLength;
  String get authPluginDataPart2 => _authPluginDataPart2;
  String get authPluginData => _authPluginData;
  String get authPluginName => _authPluginName;
}

class ResultSetColumnCountResponsePacket extends Packet {
  int _columnCount;

  int get columnCount => _columnCount;
}

class ResultSetColumnDefinitionResponsePacket extends ReusablePacket {
  ResultSetColumnDefinitionResponsePacket.reusable() : super.reusable(13);

  ResultSetColumnDefinitionResponsePacket reuse() => this;

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
}

class ResultSetRowResponsePacket extends ReusablePacket {
  ResultSetRowResponsePacket.reusable(int columnCount)
      : super.reusable(columnCount);

  ResultSetRowResponsePacket reuse() => this;

  String getString(int index) => _getString(index);

  String getUTF8String(int index) => _getUTF8String(index);
}
