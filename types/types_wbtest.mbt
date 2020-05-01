///|
/// Comprehensive tests for StreamRecord, EventPayload, and Control markers.

///|
test "record headers and tags" {
  let ts = Timestamp::from_millis(5000L)
  let header = RecordHeader::default("orders", 2, 42L, ts)
  assert_eq(header.stream_id, "orders")
  assert_eq(header.partition_id, 2)
  assert_eq(header.sequence_number, 42L)
  assert_true(header.ingest_time == ts)
  assert_eq(header.to_string(), "Header(orders:p2:#42)")

  let tag = RecordTag::DataEvent
  assert_eq(tag.to_string(), "DataEvent")
  assert_true(RecordTag::WatermarkMarker == RecordTag::WatermarkMarker)
  assert_true(RecordTag::BarrierMarker != RecordTag::HeartbeatMarker)
}

///|
test "event payload variants and accessors" {
  let null_val = NullPayload
  let int_val = IntPayload(12345L)
  let dbl_val = DoublePayload(3.14159)
  let str_val = StringPayload("test_string")
  let bytes_val = BytesPayload(b"\x01\x02\x03\x04")
  let arr_val = ArrayPayload([int_val, dbl_val])
  let map_val = MapPayload([("key1", str_val), ("key2", arr_val)])

  assert_eq(int_val.as_int(), Some(12345L))
  assert_eq(dbl_val.as_double(), Some(3.14159))
  assert_eq(int_val.as_double(), Some(12345.0))
  assert_eq(str_val.as_string(), Some("test_string"))
  assert_true(null_val.as_int() is None)

  assert_true(map_val.get_field("key1") == Some(str_val))
  assert_true(map_val.get_field("missing") is None)

  assert_true(arr_val.get_index(0) == Some(int_val))
  assert_true(arr_val.get_index(1) == Some(dbl_val))
  assert_true(arr_val.get_index(2) is None)

  assert_eq(null_val.to_string(), "null")
  assert_eq(int_val.to_string(), "12345")
  assert_eq(str_val.to_string(), "\"test_string\"")
  assert_eq(bytes_val.to_string(), "<bytes len=4>")
  assert_eq(arr_val.to_string(), "[12345, 3.14159]")
}

///|
test "stream record creation and predicates" {
  let ts = Timestamp::from_seconds(100L)
  let payload = StringPayload("sensor_value")
  let record = StreamRecord::data("sensor_1", ts, payload)

  assert_true(record.is_data())
  assert_false(record.is_watermark())
  assert_false(record.is_barrier())
  assert_false(record.is_eos())
  assert_eq(record.get_key(), "sensor_1")
  assert_true(record.get_timestamp() == ts)
  assert_true(record.get_payload() == payload)

  let wm = StreamRecord::watermark(ts)
  assert_true(wm.is_watermark())
  assert_false(wm.is_data())

  let barrier = StreamRecord::barrier(7L, ts)
  assert_true(barrier.is_barrier())
  assert_true(barrier.get_payload() == IntPayload(7L))

  let hb = StreamRecord::heartbeat(ts)
  assert_true(hb.tag == RecordTag::HeartbeatMarker)

  let eos = StreamRecord::eos(ts)
  assert_true(eos.is_eos())
}

///|
test "dedicated control markers" {
  let ts = Timestamp::from_seconds(500L)
  let wm1 = Watermark::from_timestamp(ts)
  let wm2 = Watermark::from_millis(500000L)
  let wm3 = Watermark::from_seconds(600L)

  assert_true(wm1 == wm2)
  assert_true(wm1 < wm3)
  assert_true(wm1.get_timestamp() == ts)
  assert_eq(wm1.to_string(), "Watermark(Timestamp(500000000us))")

  let b1 = CheckpointBarrier::new(101L, ts)
  assert_eq(b1.get_id(), 101L)
  assert_true(b1.get_timestamp() == ts)
  assert_eq(b1.to_string(), "CheckpointBarrier(id=101, ts=Timestamp(500000000us))")
}
