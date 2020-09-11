///|
/// Comprehensive tests for binary wire protocol framing, checksums, and metrics codec.

///|
test "binary wire frame encoding and decoding roundtrip" {
  let payload = b"{\"event\":\"trade\",\"price\":142.50,\"volume\":100}"
  let seq_id = 9876543210L
  let msg_type = msg_type_data

  let encoded = encode_frame(msg_type, seq_id, payload)
  assert_true(encoded.length() >= 22)

  let decoded_opt = decode_frame(encoded)
  assert_true(decoded_opt is Some(_))

  let frame = match decoded_opt {
    Some(f) => f
    None => abort("Failed to decode frame")
  }

  assert_true(frame.msg_type == msg_type)
  assert_eq(frame.seq_id, seq_id)
  assert_true(frame.payload == payload)
}

///|
test "binary wire frame corruption detection" {
  let payload = b"critical_checkpoint_barrier_payload"
  let encoded = encode_frame(msg_type_barrier, 42L, payload)

  // 1. Truncated buffer (< 22 bytes)
  let truncated_arr : Array[Byte] = Array::make(10, b'\x00')
  for i = 0; i < 10; i = i + 1 {
    truncated_arr[i] = encoded[i]
  }
  let truncated = Bytes::from_array(truncated_arr)
  assert_true(decode_frame(truncated) is None)

  // 2. Corrupted magic header
  let corrupt_magic_arr : Array[Byte] = Array::make(encoded.length(), b'\x00')
  for i = 0; i < encoded.length(); i = i + 1 {
    corrupt_magic_arr[i] = encoded[i]
  }
  corrupt_magic_arr[0] = b'X' // Corrupt first magic byte
  let corrupt_magic = Bytes::from_array(corrupt_magic_arr)
  assert_true(decode_frame(corrupt_magic) is None)

  // 3. Corrupted payload byte (checksum mismatch)
  let corrupt_payload_arr : Array[Byte] = Array::make(encoded.length(), b'\x00')
  for i = 0; i < encoded.length(); i = i + 1 {
    corrupt_payload_arr[i] = encoded[i]
  }
  let last_idx = encoded.length() - 1
  corrupt_payload_arr[last_idx] = b'Z' // Corrupt last byte
  let corrupt_payload = Bytes::from_array(corrupt_payload_arr)
  assert_true(decode_frame(corrupt_payload) is None)
}

///|
test "json record serialization" {
  let ts = @types.Timestamp::from_seconds(1700000000L)
  let payload = @types.EventPayload::map([("temp", @types.EventPayload::double(98.6))])
  let record = @types.StreamRecord::data("sensor_1", ts, payload)

  let json_str = record_to_json(record)
  assert_true(json_str.length() > 0)
}

///|
test "prometheus metrics text exposition formatting" {
  let ts = @types.Timestamp::from_seconds(1700000000L)
  let prom_text = format_prometheus_metrics("window_aggregate", 1000L, 950L, 50L, ts)

  assert_true(prom_text.length() > 0)
}
