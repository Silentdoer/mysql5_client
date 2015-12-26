library mysql_client.packet_buffer;

import "package:mysql_client/src/reader_buffer.dart";

class PacketBuffer {
  int _sequenceId;

  ReaderBuffer _payload;

  PacketBuffer(this._sequenceId, this._payload);

  PacketBuffer.reusable();

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
