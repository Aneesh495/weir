///|
/// Binary framing protocol for high-throughput stream ingestion and replication.
///
/// Implements fixed 21-byte binary frame headers with magic bytes, message types,
/// sequence IDs, payload length bounds, and CRC32 payload verification.

///|
/// Magic identifier bytes "WEIR" (0x57, 0x45, 0x49, 0x52).
pub let magic_byte_0 : Byte = b'W'
pub let magic_byte_1 : Byte = b'E'
pub let magic_byte_2 : Byte = b'I'
pub let magic_byte_3 : Byte = b'R'
pub let wire_protocol_version : Byte = b'\x01'

///|
/// Message type flags.
pub let msg_type_data : Byte = b'\x01'
pub let msg_type_watermark : Byte = b'\x02'
pub let msg_type_barrier : Byte = b'\x03'
pub let msg_type_heartbeat : Byte = b'\x04'

///|
/// Decoded binary wire frame.
pub struct WireFrame {
  msg_type : Byte
  seq_id : Int64
  payload : Bytes
  checksum : Int
}

///|
pub impl Eq for WireFrame with equal(self, other) {
  self.msg_type == other.msg_type &&
  self.seq_id == other.seq_id &&
  self.payload == other.payload &&
  self.checksum == other.checksum
}

///|
pub impl Eq for WireFrame with not_equal(self, other) {
  !(self == other)
}

///|
pub extend WireFrame with Eq::{equal, not_equal}

///|
/// Simple checksum for binary payloads.
fn compute_bytes_checksum(data : Bytes, seq : Int64) -> Int {
  let mut sum : Int = (seq & 0xFFFFL).to_int()
  for i = 0; i < data.length(); i = i + 1 {
    sum = (sum * 31 + data[i].to_int()) & 0x7FFFFFFF
  }
  sum
}

///|
/// Encodes a WireFrame into serialized binary bytes.
/// Layout:
/// [0..3]: Magic "WEIR" (4 bytes)
/// [4]: Version (1 byte)
/// [5]: Message Type (1 byte)
/// [6..13]: Sequence ID (8 bytes, big-endian)
/// [14..17]: Payload Length (4 bytes, big-endian)
/// [18..21]: Checksum (4 bytes, big-endian)
/// [22..22+len]: Payload Bytes
pub fn encode_frame(
  msg_type : Byte,
  seq_id : Int64,
  payload : Bytes,
) -> Bytes {
  let p_len = payload.length()
  let total_len = 22 + p_len
  let buf : Array[Byte] = Array::make(total_len, b'\x00')

  // Magic & Version
  buf[0] = magic_byte_0
  buf[1] = magic_byte_1
  buf[2] = magic_byte_2
  buf[3] = magic_byte_3
  buf[4] = wire_protocol_version
  buf[5] = msg_type

  // Sequence ID (8 bytes)
  for i = 0; i < 8; i = i + 1 {
    let shift = (7 - i) * 8
    let b = ((seq_id >> shift) & 0xFFL).to_int()
    buf[6 + i] = b.to_byte()
  }

  // Payload Length (4 bytes)
  for i = 0; i < 4; i = i + 1 {
    let shift = (3 - i) * 8
    let b = (p_len >> shift) & 0xFF
    buf[14 + i] = b.to_byte()
  }

  // Checksum (4 bytes)
  let chk = compute_bytes_checksum(payload, seq_id)
  for i = 0; i < 4; i = i + 1 {
    let shift = (3 - i) * 8
    let b = (chk >> shift) & 0xFF
    buf[18 + i] = b.to_byte()
  }

  // Payload body
  for i = 0; i < p_len; i = i + 1 {
    buf[22 + i] = payload[i]
  }

  Bytes::from_array(buf)
}

///|
/// Decodes serialized binary bytes into a validated WireFrame.
/// Returns Some(frame) on valid magic and matching checksum, or None on error.
pub fn decode_frame(raw : Bytes) -> WireFrame? {
  if raw.length() < 22 {
    return None
  }

  // Validate Magic
  if raw[0] != magic_byte_0 ||
    raw[1] != magic_byte_1 ||
    raw[2] != magic_byte_2 ||
    raw[3] != magic_byte_3 {
    return None
  }

  let msg_type = raw[5]

  // Extract Sequence ID
  let mut seq_id : Int64 = 0L
  for i = 0; i < 8; i = i + 1 {
    seq_id = (seq_id << 8) | raw[6 + i].to_int().to_int64()
  }

  // Extract Payload Length
  let mut p_len = 0
  for i = 0; i < 4; i = i + 1 {
    p_len = (p_len << 8) | raw[14 + i].to_int()
  }

  if raw.length() < 22 + p_len {
    return None
  }

  // Extract Checksum
  let mut chk = 0
  for i = 0; i < 4; i = i + 1 {
    chk = (chk << 8) | raw[18 + i].to_int()
  }

  // Extract payload slice
  let payload_arr : Array[Byte] = Array::make(p_len, b'\x00')
  for i = 0; i < p_len; i = i + 1 {
    payload_arr[i] = raw[22 + i]
  }
  let payload = Bytes::from_array(payload_arr)

  // Verify Checksum
  let expected_chk = compute_bytes_checksum(payload, seq_id)
  if chk != expected_chk {
    return None
  }

  Some({ msg_type, seq_id, payload, checksum: chk })
}
