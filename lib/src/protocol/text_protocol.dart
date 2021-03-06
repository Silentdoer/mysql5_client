part of mysql_client.protocol;

const int COM_QUERY = 0x03;

class QueryCommandTextProtocol extends ProtocolDelegate {
  final ResultSetColumnDefinitionPacket _reusableColumnPacket;

  ResultSetRowPacket? _reusableRowPacket;

  QueryCommandTextProtocol(Protocol protocol)
      : _reusableColumnPacket =
            new ResultSetColumnDefinitionPacket.reusable(protocol),
        super(protocol);

  ResultSetColumnDefinitionPacket get reusableColumnPacket =>
      _reusableColumnPacket;

  ResultSetRowPacket? get reusableRowPacket => _reusableRowPacket;

  void free() {
    super.free();

    _reusableColumnPacket._free();
    _reusableRowPacket?._free();
  }

  void writeCommandQueryPacket(String query) {
    // 所以是不是也可能是这里有bug，
    // 即某次发送query有问题导致服务端不响应？
    _resetSequenceId();

    _createWriterBuffer();

    // 1              [03] COM_QUERY
    _writeFixedLengthInteger(COM_QUERY, 1);
    // string[EOF]    the query the server shall execute
    _writeFixedLengthUTF8String(query);

    _writePacket();
  }

  Future<Packet> readCommandQueryResponse() {
    //print('333-1');
    var value = _readPacketBuffer();
    // 注意，这个future应该是一个异步执行的
    // 所以它已经在等待执行了
    // 不过根据之前测试的，如果它没有用到isolate
    // 那么它其实是在等待main线程来执行，所以其实
    // 会在await的时候才排上队。。
    //print((value is Future).toString() + '  ' + value.runtimeType.toString() + '333-2' + (value is Future<Packet>).toString());

    // value 是 Future一般，但是却不是Future<Packet>而是Future<dynamic>
    // 这里是先333-n后333-m，
    // 似乎要这么理解，then只是往第一个future里添加了一个转换
    // 方法，但是其实await还是执行的第一个future；
    // 因此这里其实可以理解是第一个future就已经卡主了
    // 因此then里面的一直没有执行；
    // 即_readPacketBuffer()就卡主了？
    var value2 = value is Future
        ? value.then((_) {
            // 注意，没有输出333-m，说明value其实没有执行完毕
            // 或者说都还没排上队
            //print('333-m');
            return _readCommandQueryResponsePacket();
          })
        : _readCommandQueryResponsePacket();
    // 这个是Future<Packet>因为上面的then进行了转换
    //print('333-3 value2 is:${value2.runtimeType}');
    // 似乎是：_readCommandQueryResponsePacket卡主了
    // then只会返回另一种Future，而要获得Future的执行后的值必须await
    // 如Future<A>可以通过then转换为Future<B>，但是获取B类型的返回值必须await
    // 所以外部对result进行await其实是开始去执行_readCommandQueryResponsePacket里
    // 的请求具体值的代码；
    var result =
        value2 is Future<Packet> ? value2 : new Future.value(value2 as Packet);
    //print('333-3');
    return result;
  }

  skipResultSetColumnDefinitionResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _skipResultSetColumnDefinitionResponsePacket())
        : _skipResultSetColumnDefinitionResponsePacket();
  }

  readResultSetColumnDefinitionResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetColumnDefinitionResponsePacket())
        : _readResultSetColumnDefinitionResponsePacket();
  }

  skipResultSetRowResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _skipResultSetRowResponsePacket())
        : _skipResultSetRowResponsePacket();
  }

  readResultSetRowResponse() {
    var value = _readPacketBuffer();
    return value is Future
        ? value.then((_) => _readResultSetRowResponsePacket())
        : _readResultSetRowResponsePacket();
  }

  Packet _readCommandQueryResponsePacket() {
    //print('333-5');
    if (_isOkPacket()) {
      //print('333-6');
      return _readOkPacket();
    } else if (_isErrorPacket()) {
      //print('333-7');
      return _readErrorPacket();
    } else if (_isLocalInFilePacket()) {
      //print('333-8');
      throw new UnsupportedError("Protocol::LOCAL_INFILE_Data");
    } else {
      //print('333-9');
      return _readResultSetColumnCountPacket();
    }
  }

  Packet _skipResultSetColumnDefinitionResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _skipResultSetColumnDefinitionPacket();
    }
  }

  Packet _readResultSetColumnDefinitionResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _readResultSetColumnDefinitionPacket();
    }
  }

  Packet _skipResultSetRowResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _skipResultSetRowPacket();
    }
  }

  Packet _readResultSetRowResponsePacket() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else if (_isEOFPacket()) {
      return _readEOFPacket();
    } else {
      return _readResultSetRowPacket();
    }
  }

  ResultSetColumnCountPacket _readResultSetColumnCountPacket() {
    var packet = new ResultSetColumnCountPacket(_payloadLength, _sequenceId);

    // A packet containing a Protocol::LengthEncodedInteger column_count
    packet._columnCount = _readLengthEncodedInteger();

    _reusableRowPacket =
        new ResultSetRowPacket.reusable(_protocol, packet._columnCount!);

    return packet;
  }

  ResultSetColumnDefinitionPacket _skipResultSetColumnDefinitionPacket() {
    var packet = _reusableColumnPacket.reuse(_payloadLength, _sequenceId);

    _skipBytes(_payloadLength);

    return packet;
  }

  ResultSetColumnDefinitionPacket _readResultSetColumnDefinitionPacket() {
    var packet = _reusableColumnPacket.reuse(_payloadLength, _sequenceId);

    var dataRange;
    var i = 0;
    // lenenc_str     catalog
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     schema
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     table
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     org_table
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     name
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_str     org_name
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(_readLengthEncodedInteger(), dataRange);
    // lenenc_int     length of fixed-length fields [0c]
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readLengthEncodedDataRange(dataRange);
    // 2              character set
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(2, dataRange);
    // 4              column length
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(4, dataRange);
    // 1              type
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(1, dataRange);
    // 2              flags
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(2, dataRange);
    // 1              decimals
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(1, dataRange);
    // 2              filler [00] [00]
    dataRange = _reusableColumnPacket._getReusableDataRange(i++);
    _readFixedLengthDataRange(2, dataRange);

    return packet;
  }

  ResultSetRowPacket _skipResultSetRowPacket() {
    var packet = _reusableRowPacket!.reuse(_payloadLength, _sequenceId);

    _skipBytes(_payloadLength);

    return packet;
  }

  ResultSetRowPacket _readResultSetRowPacket() {
    var packet = _reusableRowPacket!.reuse(_payloadLength, _sequenceId);

    var i = 0;
    while (!_isAllRead) {
      var reusableRange = _reusableRowPacket!._getReusableDataRange(i++);
      if (_checkByte() != PREFIX_NULL) {
        _readFixedLengthDataRange(_readLengthEncodedInteger(), reusableRange);
      } else {
        _skipByte();
        reusableRange.reuseNil();
      }
    }

    return packet;
  }

  bool _isLocalInFilePacket() => _header == 0xfb;
}

class ResultSetColumnCountPacket extends Packet {
  int? _columnCount;

  ResultSetColumnCountPacket(int? payloadLength, int? sequenceId)
      : super(payloadLength, sequenceId);

  int? get columnCount => _columnCount;
}

class ResultSetColumnDefinitionPacket extends ReusablePacket {
  ResultSetColumnDefinitionPacket.reusable(Protocol protocol)
      : super.reusable(protocol, 13);

  ResultSetColumnDefinitionPacket reuse(int? payloadLength, int? sequenceId) =>
      _reuse(payloadLength, sequenceId) as ResultSetColumnDefinitionPacket;

  String? get catalog => getString(0);
  String? get schema => getString(1);
  String? get table => getString(2);
  String? get orgTable => getString(3);
  String? get name => getString(4);
  String? get orgName => getString(5);
  int get fieldsLength => getInteger(6);
  int get characterSet => getInteger(7);
  int get columnLength => getInteger(8);
  int get type => getInteger(9);
  int get flags => getInteger(10);
  int get decimals => getInteger(11);
}

class ResultSetRowPacket extends ReusablePacket {
  ResultSetRowPacket.reusable(Protocol protocol, int columnCount)
      : super.reusable(protocol, columnCount);

  ResultSetRowPacket reuse(int? payloadLength, int? sequenceId) =>
      _reuse(payloadLength, sequenceId) as ResultSetRowPacket;
}
