library mysql_client.protocol;

import 'dart:async';

import 'package:mysql_client/src/data_writer.dart';
import 'package:mysql_client/src/packet_reader.dart';

class QueryError extends Error {}

abstract class Protocol {
  final DataWriter _writer;

  final PacketReader _reader;

  Protocol(this._writer, this._reader);

  Future<Packet> _readCommandResponse() => _reader.readCommandResponse();

  Future<Packet> _readEOFResponse() => _reader.readEOFResponse();
}

class QueryCommandTextProtocol extends Protocol {
  QueryCommandTextProtocol(DataWriter writer, PacketReader reader)
      : super(writer, reader);

  Future<QueryResult> executeQuery(String query) async {
    await _writeCommandQueryPacket(query);

    var response = await _readCommandQueryResponse();

    if (response is OkPacket) {
      return new QueryResult.ok(response.affectedRows);
    }

    if (response is! ResultSetColumnCountResponsePacket) {
      throw new QueryError();
    }

    var result = new QueryResult.resultSet(response.columnCount, this);

    return result;
  }

  Future<Packet> _readCommandQueryResponse() =>
      _reader.readCommandQueryResponse();

  _readResultSetColumnDefinitionResponse(
          ResultSetColumnDefinitionResponsePacket reusablePacket) =>
      _reader.readResultSetColumnDefinitionResponse(reusablePacket);

  _readResultSetRowResponse(ResultSetRowResponsePacket reusablePacket) =>
      _reader.readResultSetRowResponse(reusablePacket);

  Future _writeCommandQueryPacket(String query) async {
    WriterBuffer buffer = _writer.createBuffer();

    var sequenceId = 0x00;

    // 1              [03] COM_QUERY
    buffer.writeFixedLengthInteger(COM_QUERY, 1);
    // string[EOF]    the query the server shall execute
    buffer.writeFixedLengthUTF8String(query);

    var headerBuffer = _writer.createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _writer.writeBuffer(headerBuffer);
    _writer.writeBuffer(buffer);
  }

  void close() {}
}

class QueryResult {
  final int affectedRows;

  final int columnCount;

  final QueryCommandTextProtocol _protocol;

  QueryColumnSetReader _columnSetReader;

  QueryRowSetReader _rowSetReader;

  QueryResult.resultSet(this.columnCount, this._protocol)
      : this.affectedRows = 0;

  QueryResult.ok(this.affectedRows)
      : this.columnCount = 0,
        this._protocol = null;

  QueryColumnSetReader get columnSetReader {
    // TODO check dello stato

    _columnSetReader = new QueryColumnSetReader(columnCount, _protocol);

    return _columnSetReader;
  }

  QueryRowSetReader get rowSetReader {
    // TODO check dello stato

    _rowSetReader = new QueryRowSetReader(columnCount, _protocol);

    return _rowSetReader;
  }

  void close() {
    if (_columnSetReader != null) {
      _columnSetReader.close();
    }
    if (_rowSetReader != null) {
      _rowSetReader.close();
    }
  }
}

abstract class SetReader {
  Future<bool> next();

  internalNext();

  void close();
}

class QueryColumnSetReader extends SetReader {
  final int _columnCount;

  final QueryCommandTextProtocol _protocol;

  final ResultSetColumnDefinitionResponsePacket _reusableColumnPacket;

  QueryColumnSetReader(this._columnCount, this._protocol)
      : this._reusableColumnPacket =
            new ResultSetColumnDefinitionResponsePacket.reusable();

  Future<bool> next() {
    var value = internalNext();
    return value is Future ? value : new Future.value(value);
  }

  internalNext() {
    // TODO check dello stato

    var response =
        _protocol._readResultSetColumnDefinitionResponse(_reusableColumnPacket);

    return response is Future
        ? response.then(
            (response) => response is ResultSetColumnDefinitionResponsePacket)
        : response is ResultSetColumnDefinitionResponsePacket;
  }

  String get name => _reusableColumnPacket.orgName;

  void close() {
    // TODO check dello stato

    _reusableColumnPacket.free();
  }
}

class QueryRowSetReader extends SetReader {
  final int _columnCount;

  final QueryCommandTextProtocol _protocol;

  final ResultSetRowResponsePacket _reusableRowPacket;

  QueryRowSetReader(int columnCount, this._protocol)
      : this._columnCount = columnCount,
        this._reusableRowPacket =
            new ResultSetRowResponsePacket.reusable(columnCount);

  Future<bool> next() {
    var value = internalNext();
    return value is Future ? value : new Future.value(value);
  }

  internalNext() {
    // TODO check dello stato

    var response = _protocol._readResultSetRowResponse(_reusableRowPacket);

    return response is Future
        ? response.then((response) => response is ResultSetRowResponsePacket)
        : response is ResultSetRowResponsePacket;
  }

  String getString(int index) => _reusableRowPacket.getString(index);

  String getUTF8String(int index) => _reusableRowPacket.getUTF8String(index);

  void close() {
    // TODO check dello stato

    _reusableRowPacket.free();
  }
}
