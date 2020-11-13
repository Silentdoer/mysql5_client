import 'dart:collection';
import 'dart:convert';

import 'package:crypto/crypto.dart';

class DigestAccumulatorSink implements ChunkedConversionSink<Digest> {
  List<Digest> get events => new UnmodifiableListView(_events);
  final _events = <Digest>[];

  bool get isClosed => _isClosed;
  var _isClosed = false;

  void clear() {
    _events.clear();
  }

  void add(Digest event) {
    if (_isClosed) {
      throw new StateError("无法添加到已关闭的sink里.");
    }

    _events.add(event);
  }

  void close() {
    _isClosed = true;
  }
}
