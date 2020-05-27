///|
/// Event-time watermark generators for out-of-order stream processing.
///
/// Implements monotonic ascending, bounded out-of-orderness, and punctuated
/// watermark generation strategies.

///|
/// Generator for strictly ascending or monotonic timestamp streams.
pub struct MonotonicWatermarkGenerator {
  mut current_watermark : @types.Watermark
}

///|
/// Creates a new MonotonicWatermarkGenerator.
pub fn MonotonicWatermarkGenerator::new() -> MonotonicWatermarkGenerator {
  { current_watermark: @types.Watermark::from_timestamp(@types.Timestamp::min_value()) }
}

///|
/// Observes an event and advances the watermark monotonically.
pub fn MonotonicWatermarkGenerator::on_event(
  self : MonotonicWatermarkGenerator,
  timestamp : @types.Timestamp,
) -> @types.Watermark? {
  if timestamp > self.current_watermark.get_timestamp() {
    self.current_watermark = @types.Watermark::from_timestamp(timestamp)
    Some(self.current_watermark)
  } else {
    None
  }
}

///|
/// Returns the current watermark.
pub fn MonotonicWatermarkGenerator::get_watermark(
  self : MonotonicWatermarkGenerator,
) -> @types.Watermark {
  self.current_watermark
}

///|
/// Generator that tolerates events arriving out-of-order up to max_lateness.
pub struct BoundedOutOfOrdernessGenerator {
  max_lateness : @types.Duration
  mut max_timestamp : @types.Timestamp
  mut last_emitted : @types.Watermark
}

///|
/// Creates a BoundedOutOfOrdernessGenerator with maximum allowed lateness.
pub fn BoundedOutOfOrdernessGenerator::new(
  max_lateness : @types.Duration,
) -> BoundedOutOfOrdernessGenerator {
  let min_ts = @types.Timestamp::min_value()
  {
    max_lateness,
    max_timestamp: min_ts,
    last_emitted: @types.Watermark::from_timestamp(min_ts),
  }
}

///|
/// Observes an event timestamp and updates the peak observed timestamp.
pub fn BoundedOutOfOrdernessGenerator::on_event(
  self : BoundedOutOfOrdernessGenerator,
  timestamp : @types.Timestamp,
) -> Unit {
  if timestamp > self.max_timestamp {
    self.max_timestamp = timestamp
  }
}

///|
/// Generates and emits a watermark based on max_timestamp minus max_lateness.
/// Guarantees that emitted watermarks are monotonically non-decreasing.
pub fn BoundedOutOfOrdernessGenerator::emit_watermark(
  self : BoundedOutOfOrdernessGenerator,
) -> @types.Watermark? {
  let candidate_ts = self.max_timestamp.sub_duration(self.max_lateness)
  if candidate_ts > self.last_emitted.get_timestamp() {
    let wm = @types.Watermark::from_timestamp(candidate_ts)
    self.last_emitted = wm
    Some(wm)
  } else {
    None
  }
}

///|
/// Returns the most recently emitted watermark.
pub fn BoundedOutOfOrdernessGenerator::get_watermark(
  self : BoundedOutOfOrdernessGenerator,
) -> @types.Watermark {
  self.last_emitted
}

///|
/// Generator that extracts watermarks based on an explicit record predicate.
pub struct PunctuatedWatermarkGenerator {
  extractor : (@types.StreamRecord) -> @types.Timestamp?
  mut last_emitted : @types.Watermark
}

///|
/// Creates a PunctuatedWatermarkGenerator with a custom extraction function.
pub fn PunctuatedWatermarkGenerator::new(
  extractor : (@types.StreamRecord) -> @types.Timestamp?,
) -> PunctuatedWatermarkGenerator {
  {
    extractor,
    last_emitted: @types.Watermark::from_timestamp(@types.Timestamp::min_value()),
  }
}

///|
/// Evaluates record and conditionally emits an advanced watermark.
pub fn PunctuatedWatermarkGenerator::on_event(
  self : PunctuatedWatermarkGenerator,
  record : @types.StreamRecord,
) -> @types.Watermark? {
  match (self.extractor)(record) {
    Some(ts) => {
      if ts > self.last_emitted.get_timestamp() {
        let wm = @types.Watermark::from_timestamp(ts)
        self.last_emitted = wm
        Some(wm)
      } else {
        None
      }
    }
    None => None
  }
}

///|
/// Returns the current punctuated watermark.
pub fn PunctuatedWatermarkGenerator::get_watermark(
  self : PunctuatedWatermarkGenerator,
) -> @types.Watermark {
  self.last_emitted
}
