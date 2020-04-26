///|
/// Stream record metadata and tagging primitives.
///
/// Encapsulates the envelope for all elements transiting through the
/// Weir stream processing pipeline, including data records, watermarks,
/// checkpoint barriers, and lifecycle control signals.

///|
/// Classification tag for records travelling through a stream.
pub enum RecordTag {
  DataEvent
  WatermarkMarker
  BarrierMarker
  HeartbeatMarker
  EndOfStreamMarker
}

///|
pub impl Eq for RecordTag with equal(self, other) {
  match (self, other) {
    (DataEvent, DataEvent) => true
    (WatermarkMarker, WatermarkMarker) => true
    (BarrierMarker, BarrierMarker) => true
    (HeartbeatMarker, HeartbeatMarker) => true
    (EndOfStreamMarker, EndOfStreamMarker) => true
    _ => false
  }
}

///|
pub impl Eq for RecordTag with not_equal(self, other) {
  !(self == other)
}

///|
pub extend RecordTag with Eq::{equal, not_equal}

///|
pub impl Show for RecordTag with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for RecordTag with to_string(self) {
  match self {
    DataEvent => "DataEvent"
    WatermarkMarker => "WatermarkMarker"
    BarrierMarker => "BarrierMarker"
    HeartbeatMarker => "HeartbeatMarker"
    EndOfStreamMarker => "EndOfStreamMarker"
  }
}

///|
pub extend RecordTag with Show::{output, to_string}

///|
/// Metadata header attached to stream records.
pub struct RecordHeader {
  stream_id : String
  partition_id : Int
  sequence_number : Int64
  ingest_time : Timestamp
}

///|
pub impl Eq for RecordHeader with equal(self, other) {
  self.stream_id == other.stream_id &&
  self.partition_id == other.partition_id &&
  self.sequence_number == other.sequence_number &&
  self.ingest_time == other.ingest_time
}

///|
pub impl Eq for RecordHeader with not_equal(self, other) {
  !(self == other)
}

///|
pub extend RecordHeader with Eq::{equal, not_equal}

///|
pub impl Show for RecordHeader with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for RecordHeader with to_string(self) {
  "Header(" +
  self.stream_id +
  ":p" +
  self.partition_id.to_string() +
  ":#" +
  self.sequence_number.to_string() +
  ")"
}

///|
pub extend RecordHeader with Show::{output, to_string}

///|
/// Creates a default header for a stream and partition.
pub fn RecordHeader::default(
  stream_id : String,
  partition_id : Int,
  seq : Int64,
  ingest_time : Timestamp,
) -> RecordHeader {
  { stream_id, partition_id, sequence_number: seq, ingest_time, }
}
///|
/// Core stream record envelope carrying data or control signals.
pub struct StreamRecord {
  header : RecordHeader
  key : String
  timestamp : Timestamp
  payload : EventPayload
  tag : RecordTag
}

///|
pub impl Eq for StreamRecord with equal(self, other) {
  self.header == other.header &&
  self.key == other.key &&
  self.timestamp == other.timestamp &&
  self.payload == other.payload &&
  self.tag == other.tag
}

///|
pub impl Eq for StreamRecord with not_equal(self, other) {
  !(self == other)
}

///|
pub extend StreamRecord with Eq::{equal, not_equal}

///|
pub impl Show for StreamRecord with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for StreamRecord with to_string(self) {
  "StreamRecord(" +
  self.tag.to_string() +
  ", key=" +
  self.key +
  ", ts=" +
  self.timestamp.to_string() +
  ", payload=" +
  self.payload.to_string() +
  ")"
}

///|
pub extend StreamRecord with Show::{output, to_string}

///|
/// Creates a standard data record with default header.
pub fn StreamRecord::data(
  key : String,
  timestamp : Timestamp,
  payload : EventPayload,
) -> StreamRecord {
  let header = RecordHeader::default("default", 0, 0L, timestamp)
  { header, key, timestamp, payload, tag: DataEvent }
}

///|
/// Creates a data record with explicit stream and partition coordinates.
pub fn StreamRecord::data_with_header(
  header : RecordHeader,
  key : String,
  timestamp : Timestamp,
  payload : EventPayload,
) -> StreamRecord {
  { header, key, timestamp, payload, tag: DataEvent }
}

///|
/// Creates a watermark control record carrying the high-watermark timestamp.
pub fn StreamRecord::watermark(timestamp : Timestamp) -> StreamRecord {
  let header = RecordHeader::default("system", 0, 0L, timestamp)
  { header, key: "$watermark", timestamp, payload: NullPayload, tag: WatermarkMarker }
}

///|
/// Creates a checkpoint barrier record carrying the checkpoint identifier.
pub fn StreamRecord::barrier(checkpoint_id : Int64, timestamp : Timestamp) -> StreamRecord {
  let header = RecordHeader::default("system", 0, 0L, timestamp)
  {
    header,
    key: "$barrier",
    timestamp,
    payload: IntPayload(checkpoint_id),
    tag: BarrierMarker,
  }
}

///|
/// Creates a heartbeat keepalive record.
pub fn StreamRecord::heartbeat(timestamp : Timestamp) -> StreamRecord {
  let header = RecordHeader::default("system", 0, 0L, timestamp)
  { header, key: "$heartbeat", timestamp, payload: NullPayload, tag: HeartbeatMarker }
}

///|
/// Creates an end-of-stream signal record.
pub fn StreamRecord::eos(timestamp : Timestamp) -> StreamRecord {
  let header = RecordHeader::default("system", 0, 0L, timestamp)
  { header, key: "$eos", timestamp, payload: NullPayload, tag: EndOfStreamMarker }
}

///|
/// Checks if record is a data event.
pub fn StreamRecord::is_data(self : StreamRecord) -> Bool {
  self.tag == DataEvent
}

///|
/// Checks if record is a watermark marker.
pub fn StreamRecord::is_watermark(self : StreamRecord) -> Bool {
  self.tag == WatermarkMarker
}

///|
/// Checks if record is a checkpoint barrier.
pub fn StreamRecord::is_barrier(self : StreamRecord) -> Bool {
  self.tag == BarrierMarker
}

///|
/// Checks if record is an end-of-stream signal.
pub fn StreamRecord::is_eos(self : StreamRecord) -> Bool {
  self.tag == EndOfStreamMarker
}

///|
/// Returns the record key.
pub fn StreamRecord::get_key(self : StreamRecord) -> String {
  self.key
}

///|
/// Returns the record event timestamp.
pub fn StreamRecord::get_timestamp(self : StreamRecord) -> Timestamp {
  self.timestamp
}

///|
/// Returns the record payload.
pub fn StreamRecord::get_payload(self : StreamRecord) -> EventPayload {
  self.payload
}
