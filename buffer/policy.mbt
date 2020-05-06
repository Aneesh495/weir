///|
/// Overflow policies and telemetry tracking for bounded stream buffers.

///|
/// Action taken when pushing into a saturated buffer.
pub enum OverflowPolicy {
  Block
  DropOldest
  DropNewest
}

///|
pub impl Eq for OverflowPolicy with equal(self, other) {
  match (self, other) {
    (Block, Block) => true
    (DropOldest, DropOldest) => true
    (DropNewest, DropNewest) => true
    _ => false
  }
}

///|
pub impl Eq for OverflowPolicy with not_equal(self, other) {
  !(self == other)
}

///|
pub extend OverflowPolicy with Eq::{equal, not_equal}

///|
pub impl Show for OverflowPolicy with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for OverflowPolicy with to_string(self) {
  match self {
    Block => "Block"
    DropOldest => "DropOldest"
    DropNewest => "DropNewest"
  }
}

///|
pub extend OverflowPolicy with Show::{output, to_string}

///|
/// Performance and saturation counters for a bounded stream channel.
pub struct ChannelStats {
  mut pushed : Int64
  mut popped : Int64
  mut dropped : Int64
  mut peak_occupancy : Int
}

///|
pub impl Eq for ChannelStats with equal(self, other) {
  self.pushed == other.pushed &&
  self.popped == other.popped &&
  self.dropped == other.dropped &&
  self.peak_occupancy == other.peak_occupancy
}

///|
pub impl Eq for ChannelStats with not_equal(self, other) {
  !(self == other)
}

///|
pub extend ChannelStats with Eq::{equal, not_equal}

///|
pub impl Show for ChannelStats with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for ChannelStats with to_string(self) {
  "ChannelStats(pushed=" +
  self.pushed.to_string() +
  ", popped=" +
  self.popped.to_string() +
  ", dropped=" +
  self.dropped.to_string() +
  ", peak=" +
  self.peak_occupancy.to_string() +
  ")"
}

///|
pub extend ChannelStats with Show::{output, to_string}

///|
/// Creates a clean zeroed ChannelStats tracker.
pub fn ChannelStats::new() -> ChannelStats {
  { pushed: 0L, popped: 0L, dropped: 0L, peak_occupancy: 0 }
}

///|
/// Records an admitted item and updates peak watermark.
pub fn ChannelStats::record_push(self : ChannelStats, current_size : Int) -> Unit {
  self.pushed = self.pushed + 1L
  if current_size > self.peak_occupancy {
    self.peak_occupancy = current_size
  }
}

///|
/// Records a popped item.
pub fn ChannelStats::record_pop(self : ChannelStats) -> Unit {
  self.popped = self.popped + 1L
}

///|
/// Records a dropped item due to buffer saturation.
pub fn ChannelStats::record_drop(self : ChannelStats) -> Unit {
  self.dropped = self.dropped + 1L
}
///|
/// Returns the Block policy.
pub fn OverflowPolicy::block() -> OverflowPolicy {
  Block
}

///|
/// Returns the DropOldest policy.
pub fn OverflowPolicy::drop_oldest() -> OverflowPolicy {
  DropOldest
}

///|
/// Returns the DropNewest policy.
pub fn OverflowPolicy::drop_newest() -> OverflowPolicy {
  DropNewest
}
