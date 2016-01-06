part of mysql_client.protocol;

class _PacketBuffer {
  int _sequenceId;

  int _header;

  ReaderBuffer _payload;

  _PacketBuffer(int sequenceId, int header, ReaderBuffer payload) {
    reuse(sequenceId, header, payload);
  }

  _PacketBuffer.reusable();

  int get header => _header;

  ReaderBuffer get payload => _payload;

  int get payloadLength => _payload.dataLength;

  int get sequenceId => _sequenceId;

  void free() {
    _payload.free();
    _sequenceId = null;
  }

  _PacketBuffer reuse(int sequenceId, int header, ReaderBuffer payload) {
    _sequenceId = sequenceId;
    _header = header;
    _payload = payload;

    return this;
  }
}
