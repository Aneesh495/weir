///|
/// Late event detection and side-output routing.
///
/// Implements allowed lateness classification and routes tardy events exceeding
/// tolerances to a Dead-Letter Queue (DLQ) or side-output channel.

///|
/// Classification decision for an incoming event relative to current watermark.
pub enum LateDecision {
  OnTime
  LateWithinAllowance
  DroppedLateToSideOutput
}

///|
pub impl Eq for LateDecision with equal(self, other) {
  match (self, other) {
    (OnTime, OnTime) => true
    (LateWithinAllowance, LateWithinAllowance) => true
    (DroppedLateToSideOutput, DroppedLateToSideOutput) => true
    _ => false
  }
}

///|
pub impl Eq for LateDecision with not_equal(self, other) {
  !(self == other)
}

///|
pub extend LateDecision with Eq::{equal, not_equal}

///|
pub impl Show for LateDecision with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for LateDecision with to_string(self) {
  match self {
    OnTime => "OnTime"
    LateWithinAllowance => "LateWithinAllowance"
    DroppedLateToSideOutput => "DroppedLateToSideOutput"
  }
}

///|
pub extend LateDecision with Show::{output, to_string}

///|
/// Handles late data classification and dead-letter queue buffering.
pub struct LateDataHandler {
  allowed_lateness : @types.Duration
  side_output : @buffer.BoundedChannel[@types.StreamRecord]
  mut on_time_count : Int64
  mut late_tolerated_count : Int64
  mut dropped_count : Int64
}

///|
/// Creates a LateDataHandler with allowed lateness span and side-output queue capacity.
pub fn LateDataHandler::new(
  allowed_lateness : @types.Duration,
  side_output_capacity : Int,
) -> LateDataHandler {
  {
    allowed_lateness,
    side_output: @buffer.BoundedChannel::new(
      side_output_capacity,
      @buffer.OverflowPolicy::drop_oldest(),
    ),
    on_time_count: 0L,
    late_tolerated_count: 0L,
    dropped_count: 0L,
  }
}

///|
/// Evaluates a stream record against the current watermark and allowed lateness.
pub fn LateDataHandler::evaluate(
  self : LateDataHandler,
  record : @types.StreamRecord,
  current_watermark : @types.Watermark,
) -> LateDecision {
  let event_ts = record.get_timestamp()
  let wm_ts = current_watermark.get_timestamp()

  if event_ts >= wm_ts {
    self.on_time_count = self.on_time_count + 1L
    OnTime
  } else {
    let deadline = event_ts.add_duration(self.allowed_lateness)
    if deadline >= wm_ts {
      self.late_tolerated_count = self.late_tolerated_count + 1L
      LateWithinAllowance
    } else {
      self.dropped_count = self.dropped_count + 1L
      let _ = self.side_output.push(record)
      DroppedLateToSideOutput
    }
  }
}

///|
/// Returns count of on-time events observed.
pub fn LateDataHandler::get_on_time_count(self : LateDataHandler) -> Int64 {
  self.on_time_count
}

///|
/// Returns count of late events processed within allowed lateness.
pub fn LateDataHandler::get_late_tolerated_count(self : LateDataHandler) -> Int64 {
  self.late_tolerated_count
}

///|
/// Returns count of dropped events diverted to side output.
pub fn LateDataHandler::get_dropped_count(self : LateDataHandler) -> Int64 {
  self.dropped_count
}

///|
/// Accesses the side-output channel for late events.
pub fn LateDataHandler::get_side_output(
  self : LateDataHandler,
) -> @buffer.BoundedChannel[@types.StreamRecord] {
  self.side_output
}
