///|
/// Control markers for stream coordination and state consistency.
///
/// Implements dedicated control marker structures for event-time watermarking
/// and Chandy-Lamport distributed snapshot coordination.

///|
/// Watermark marker signalling that event-time has progressed up to this timestamp.
pub struct Watermark {
  timestamp : Timestamp
}

///|
pub impl Eq for Watermark with equal(self, other) {
  self.timestamp == other.timestamp
}

///|
pub impl Eq for Watermark with not_equal(self, other) {
  self.timestamp != other.timestamp
}

///|
pub extend Watermark with Eq::{equal, not_equal}

///|
pub impl Compare for Watermark with compare(self, other) {
  self.timestamp.compare(other.timestamp)
}

///|
pub extend Watermark with Compare::{compare, op_lt, op_le, op_ge, op_gt}

///|
pub impl Show for Watermark with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for Watermark with to_string(self) {
  "Watermark(" + self.timestamp.to_string() + ")"
}

///|
pub extend Watermark with Show::{output, to_string}

///|
/// Constructs a Watermark from an existing Timestamp.
pub fn Watermark::from_timestamp(timestamp : Timestamp) -> Watermark {
  { timestamp, }
}

///|
/// Constructs a Watermark from millisecond epoch.
pub fn Watermark::from_millis(millis : Int64) -> Watermark {
  { timestamp: Timestamp::from_millis(millis) }
}

///|
/// Constructs a Watermark from second epoch.
pub fn Watermark::from_seconds(seconds : Int64) -> Watermark {
  { timestamp: Timestamp::from_seconds(seconds) }
}

///|
/// Constructs a Watermark from microsecond epoch.
pub fn Watermark::from_micros(micros : Int64) -> Watermark {
  { timestamp: Timestamp::from_micros(micros) }
}

///|
/// Returns the watermark timestamp.
pub fn Watermark::get_timestamp(self : Watermark) -> Timestamp {
  self.timestamp
}

///|
/// Checkpoint barrier injected into stream channels to trigger snapshots.
pub struct CheckpointBarrier {
  checkpoint_id : Int64
  timestamp : Timestamp
}

///|
pub impl Eq for CheckpointBarrier with equal(self, other) {
  self.checkpoint_id == other.checkpoint_id && self.timestamp == other.timestamp
}

///|
pub impl Eq for CheckpointBarrier with not_equal(self, other) {
  !(self == other)
}

///|
pub extend CheckpointBarrier with Eq::{equal, not_equal}

///|
pub impl Show for CheckpointBarrier with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for CheckpointBarrier with to_string(self) {
  "CheckpointBarrier(id=" +
  self.checkpoint_id.to_string() +
  ", ts=" +
  self.timestamp.to_string() +
  ")"
}

///|
pub extend CheckpointBarrier with Show::{output, to_string}

///|
/// Creates a new CheckpointBarrier.
pub fn CheckpointBarrier::new(
  checkpoint_id : Int64,
  timestamp : Timestamp,
) -> CheckpointBarrier {
  { checkpoint_id, timestamp }
}

///|
/// Returns the checkpoint identifier.
pub fn CheckpointBarrier::get_id(self : CheckpointBarrier) -> Int64 {
  self.checkpoint_id
}

///|
/// Returns the barrier timestamp.
pub fn CheckpointBarrier::get_timestamp(self : CheckpointBarrier) -> Timestamp {
  self.timestamp
}
