library mysql_client.protocol;

import 'dart:async';

import 'package:mysql_client/src/data_writer.dart';
import 'package:mysql_client/src/packet_reader.dart';

class QueryError extends Error {}

abstract class Protocol {
  final DataWriter _writer;

  final PacketReader _reader;

  Protocol(this._writer, this._reader);
}

class QueryCommandTextProtocol extends Protocol {
  QueryCommandTextProtocol(DataWriter writer, PacketReader reader)
      : super(writer, reader);

  Future<QueryResult> executeQuery(String query) async {
    await _writeCommandQueryPacket(query);

    var response = await _reader.readCommandQueryResponse();

    if (response is OkPacket) {
      print("ok");

      print("affectedRows: ${response.affectedRows}");
      print("info: ${response.info}");
      print("lastInsertId: ${response.lastInsertId}");
      print("sessionStateChanges: ${response.sessionStateChanges}");
      print("warnings: ${response.warnings}");

      return new QueryResult.ok();
    }

    if (response is! ResultSetColumnCountResponsePacket) {
      throw new QueryError();
    }

    var result = new QueryResult.resultSet(response.columnCount, _reader);

    return result;
  }

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
  final int columnCount;

  final PacketReader _reader;

  QueryColumnSetReader _columnSetReader;

  QueryRowSetReader _rowSetReader;

  QueryResult.resultSet(this.columnCount, this._reader);

  QueryResult.ok()
      : this.columnCount = 0,
        this._reader = null;

  QueryColumnSetReader get columnSetReader {
    // TODO check dello stato

    _columnSetReader = new QueryColumnSetReader(columnCount, _reader);

    return _columnSetReader;
  }

  QueryRowSetReader get rowSetReader {
    // TODO check dello stato

    _rowSetReader = new QueryRowSetReader(columnCount, _reader);

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

  final PacketReader _reader;

  final ResultSetColumnDefinitionResponsePacket _reusableColumnPacket;

  QueryColumnSetReader(this._columnCount, this._reader)
      : this._reusableColumnPacket =
            new ResultSetColumnDefinitionResponsePacket.reusable();

  Future<bool> next() {
    var value = internalNext();
    return value is Future ? value : new Future.value(value);
  }

  internalNext() {
    // TODO check dello stato

    var response =
        _reader.readResultSetColumnDefinitionResponse(_reusableColumnPacket);

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

  final PacketReader _reader;

  final ResultSetRowResponsePacket _reusableRowPacket;

  QueryRowSetReader(int columnCount, this._reader)
      : this._columnCount = columnCount,
        this._reusableRowPacket =
            new ResultSetRowResponsePacket.reusable(columnCount);

  Future<bool> next() {
    var value = internalNext();
    return value is Future ? value : new Future.value(value);
  }

  internalNext() {
    // TODO check dello stato

    var response = _reader.readResultSetRowResponse(_reusableRowPacket);

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
