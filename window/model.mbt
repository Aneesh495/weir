///|
/// Temporal window models and window key definitions.
///
/// Implements closed-open [start, end) interval windows and compound keys
/// for windowed stream partition state.

///|
/// Temporal window interval bounded by [start, end) in event-time.
pub struct TimeWindow {
  start : @types.Timestamp
  end : @types.Timestamp
}

///|
pub impl Eq for TimeWindow with equal(self, other) {
  self.start == other.start && self.end == other.end
}

///|
pub impl Eq for TimeWindow with not_equal(self, other) {
  !(self == other)
}

///|
pub extend TimeWindow with Eq::{equal, not_equal}

///|
pub impl Compare for TimeWindow with compare(self, other) {
  let cmp_start = self.start.compare(other.start)
  if cmp_start != 0 {
    cmp_start
  } else {
    self.end.compare(other.end)
  }
}

///|
pub extend TimeWindow with Compare::{compare, op_lt, op_le, op_ge, op_gt}

///|
pub impl Show for TimeWindow with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for TimeWindow with to_string(self) {
  "[" + self.start.to_string() + ".." + self.end.to_string() + ")"
}

///|
pub extend TimeWindow with Show::{output, to_string}

///|
/// Constructs a TimeWindow with explicit start and end bounds.
pub fn TimeWindow::new(
  start : @types.Timestamp,
  end : @types.Timestamp,
) -> TimeWindow {
  { start, end }
}

///|
/// Returns the window start boundary.
pub fn TimeWindow::get_start(self : TimeWindow) -> @types.Timestamp {
  self.start
}

///|
/// Returns the window end boundary.
pub fn TimeWindow::get_end(self : TimeWindow) -> @types.Timestamp {
  self.end
}

///|
/// Checks if a timestamp falls within the [start, end) interval.
pub fn TimeWindow::covers(self : TimeWindow, ts : @types.Timestamp) -> Bool {
  ts >= self.start && ts < self.end
}

///|
/// Checks if this window overlaps with another window.
pub fn TimeWindow::intersects(self : TimeWindow, other : TimeWindow) -> Bool {
  self.start < other.end && other.start < self.end
}

///|
/// Merges two overlapping or contiguous windows into a single enclosing window.
pub fn TimeWindow::merge(self : TimeWindow, other : TimeWindow) -> TimeWindow {
  let new_start = if self.start < other.start { self.start } else { other.start }
  let new_end = if self.end > other.end { self.end } else { other.end }
  { start: new_start, end: new_end }
}

///|
/// Returns the duration span of the window.
pub fn TimeWindow::duration(self : TimeWindow) -> @types.Duration {
  self.end.diff(self.start)
}

///|
/// Compound key identifying a partitioned window instance.
pub struct WindowKey {
  key : String
  window : TimeWindow
}

///|
pub impl Eq for WindowKey with equal(self, other) {
  self.key == other.key && self.window == other.window
}

///|
pub impl Eq for WindowKey with not_equal(self, other) {
  !(self == other)
}

///|
pub extend WindowKey with Eq::{equal, not_equal}

///|
pub impl Show for WindowKey with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for WindowKey with to_string(self) {
  "WindowKey(" + self.key + " @ " + self.window.to_string() + ")"
}

///|
pub extend WindowKey with Show::{output, to_string}

///|
/// Constructs a new compound WindowKey.
pub fn WindowKey::new(key : String, window : TimeWindow) -> WindowKey {
  { key, window }
}

///|
/// Returns the partition key string.
pub fn WindowKey::get_key(self : WindowKey) -> String {
  self.key
}

///|
/// Returns the associated TimeWindow.
pub fn WindowKey::get_window(self : WindowKey) -> TimeWindow {
  self.window
}
