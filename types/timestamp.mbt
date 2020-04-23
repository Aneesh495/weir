///|
/// Timestamp and duration primitives for event-time stream processing.
///
/// Timestamps represent absolute points in time measured in microseconds
/// since Unix epoch (January 1, 1970 00:00:00 UTC). Durations represent
/// positive or signed time deltas in microseconds.

///|
/// Representation of an absolute event-time instant in microseconds.
pub struct Timestamp {
  micros : Int64
}

///|
pub impl Eq for Timestamp with equal(self, other) {
  self.micros == other.micros
}

///|
pub impl Eq for Timestamp with not_equal(self, other) {
  self.micros != other.micros
}

///|
pub extend Timestamp with Eq::{equal, not_equal}

///|
pub impl Compare for Timestamp with compare(self, other) {
  if self.micros < other.micros {
    -1
  } else if self.micros > other.micros {
    1
  } else {
    0
  }
}

///|
pub extend Timestamp with Compare::{compare, op_lt, op_le, op_ge, op_gt}

///|
pub impl Show for Timestamp with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for Timestamp with to_string(self) {
  "Timestamp(" + self.micros.to_string() + "us)"
}

///|
pub extend Timestamp with Show::{output, to_string}

///|
/// Constructs a Timestamp from raw microsecond epoch value.
pub fn Timestamp::from_micros(micros : Int64) -> Timestamp {
  { micros, }
}

///|
/// Constructs a Timestamp from millisecond epoch value.
pub fn Timestamp::from_millis(millis : Int64) -> Timestamp {
  { micros: millis * 1000L }
}

///|
/// Constructs a Timestamp from second epoch value.
pub fn Timestamp::from_seconds(seconds : Int64) -> Timestamp {
  { micros: seconds * 1000000L }
}

///|
/// Returns the microsecond integer value.
pub fn Timestamp::to_micros(self : Timestamp) -> Int64 {
  self.micros
}

///|
/// Returns the millisecond integer value (truncated).
pub fn Timestamp::to_millis(self : Timestamp) -> Int64 {
  self.micros / 1000L
}

///|
/// Returns the second integer value (truncated).
pub fn Timestamp::to_seconds(self : Timestamp) -> Int64 {
  self.micros / 1000000L
}

///|
/// Returns a sentinel timestamp representing the earliest possible time.
pub fn Timestamp::min_value() -> Timestamp {
  { micros: -9223372036854775807L }
}

///|
/// Returns a sentinel timestamp representing the latest possible time.
pub fn Timestamp::max_value() -> Timestamp {
  { micros: 9223372036854775807L }
}

///|
/// Adds a duration to a timestamp.
pub fn Timestamp::add_duration(self : Timestamp, dur : Duration) -> Timestamp {
  { micros: self.micros + dur.micros }
}

///|
/// Subtracts a duration from a timestamp.
pub fn Timestamp::sub_duration(self : Timestamp, dur : Duration) -> Timestamp {
  { micros: self.micros - dur.micros }
}

///|
/// Calculates the duration delta between two timestamps (self - other).
pub fn Timestamp::diff(self : Timestamp, other : Timestamp) -> Duration {
  { micros: self.micros - other.micros }
}

///|
/// Checks if this timestamp is strictly before another.
pub fn Timestamp::is_before(self : Timestamp, other : Timestamp) -> Bool {
  self.micros < other.micros
}

///|
/// Checks if this timestamp is strictly after another.
pub fn Timestamp::is_after(self : Timestamp, other : Timestamp) -> Bool {
  self.micros > other.micros
}

///|
/// Representation of a relative time interval in microseconds.
pub struct Duration {
  micros : Int64
}

///|
pub impl Eq for Duration with equal(self, other) {
  self.micros == other.micros
}

///|
pub impl Eq for Duration with not_equal(self, other) {
  self.micros != other.micros
}

///|
pub extend Duration with Eq::{equal, not_equal}

///|
pub impl Compare for Duration with compare(self, other) {
  if self.micros < other.micros {
    -1
  } else if self.micros > other.micros {
    1
  } else {
    0
  }
}

///|
pub extend Duration with Compare::{compare, op_lt, op_le, op_ge, op_gt}

///|
pub impl Show for Duration with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for Duration with to_string(self) {
  if self.micros % 1000000L == 0L {
    (self.micros / 1000000L).to_string() + "s"
  } else if self.micros % 1000L == 0L {
    (self.micros / 1000L).to_string() + "ms"
  } else {
    self.micros.to_string() + "us"
  }
}

///|
pub extend Duration with Show::{output, to_string}

///|
/// Constructs a Duration from raw microsecond span.
pub fn Duration::from_micros(micros : Int64) -> Duration {
  { micros, }
}

///|
/// Constructs a Duration from millisecond span.
pub fn Duration::from_millis(millis : Int64) -> Duration {
  { micros: millis * 1000L }
}

///|
/// Constructs a Duration from second span.
pub fn Duration::from_seconds(seconds : Int64) -> Duration {
  { micros: seconds * 1000000L }
}

///|
/// Constructs a Duration from minute span.
pub fn Duration::from_minutes(minutes : Int64) -> Duration {
  { micros: minutes * 60000000L }
}

///|
/// Returns raw microseconds.
pub fn Duration::to_micros(self : Duration) -> Int64 {
  self.micros
}

///|
/// Returns milliseconds (truncated).
pub fn Duration::to_millis(self : Duration) -> Int64 {
  self.micros / 1000L
}

///|
/// Returns seconds (truncated).
pub fn Duration::to_seconds(self : Duration) -> Int64 {
  self.micros / 1000000L
}

///|
/// Zero duration constant.
pub fn Duration::zero() -> Duration {
  { micros: 0L }
}

///|
/// Adds two durations.
pub fn Duration::add(self : Duration, other : Duration) -> Duration {
  { micros: self.micros + other.micros }
}

///|
/// Subtracts another duration from this one.
pub fn Duration::sub(self : Duration, other : Duration) -> Duration {
  { micros: self.micros - other.micros }
}
