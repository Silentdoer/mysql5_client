part of mysql_client.protocol;

class _PacketBuffer {
  int? _sequenceId;

  late int _header;

  late ReaderBuffer _payload;

  _PacketBuffer(int sequenceId, int header, ReaderBuffer payload) {
    reuse(sequenceId, header, payload);
  }

  _PacketBuffer.reusable();

  _PacketBuffer reuse(int sequenceId, int header, ReaderBuffer payload) {
    // print("PacketBuffer: $sequenceId [${payload.dataLength}]");
    _sequenceId = sequenceId;
    _header = header;
    _payload = payload;

    return this;
  }

  void free() {
    _payload.free();
    _sequenceId = null;
  }

  int? get sequenceId => _sequenceId;

  ReaderBuffer get payload => _payload;

  int get header => _header;

  int get payloadLength => _payload.dataLength!;
}
