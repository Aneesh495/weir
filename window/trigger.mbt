///|
/// Window firing triggers and lifecycle state machines.
///
/// Determines when the current buffered contents of a window should be
/// evaluated, emitted downstream, or purged from memory.

///|
/// Decision returned by a window trigger on event or watermark arrival.
pub enum TriggerResult {
  Continue
  Fire
  Purge
  FireAndPurge
}

///|
pub impl Eq for TriggerResult with equal(self, other) {
  match (self, other) {
    (Continue, Continue) => true
    (Fire, Fire) => true
    (Purge, Purge) => true
    (FireAndPurge, FireAndPurge) => true
    _ => false
  }
}

///|
pub impl Eq for TriggerResult with not_equal(self, other) {
  !(self == other)
}

///|
pub extend TriggerResult with Eq::{equal, not_equal}

///|
pub impl Show for TriggerResult with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for TriggerResult with to_string(self) {
  match self {
    Continue => "Continue"
    Fire => "Fire"
    Purge => "Purge"
    FireAndPurge => "FireAndPurge"
  }
}

///|
pub extend TriggerResult with Show::{output, to_string}

///|
/// Returns the Continue decision.
pub fn TriggerResult::continue_() -> TriggerResult {
  Continue
}

///|
/// Returns the Fire decision.
pub fn TriggerResult::fire() -> TriggerResult {
  Fire
}

///|
/// Returns the Purge decision.
pub fn TriggerResult::purge() -> TriggerResult {
  Purge
}

///|
/// Returns the FireAndPurge decision.
pub fn TriggerResult::fire_and_purge() -> TriggerResult {
  FireAndPurge
}

///|
/// Standard trigger that fires and purges when watermark passes the window end.
pub struct EventTimeTrigger {
  _unit : Unit
}

///|
/// Creates an EventTimeTrigger instance.
pub fn EventTimeTrigger::new() -> EventTimeTrigger {
  { _unit: () }
}

///|
/// Evaluates watermark advancement against window end boundary.
pub fn EventTimeTrigger::on_watermark(
  window : TimeWindow,
  watermark : @types.Watermark,
) -> TriggerResult {
  if watermark.get_timestamp() >= window.get_end() {
    FireAndPurge
  } else {
    Continue
  }
}

///|
/// Trigger that fires whenever accumulated event count reaches a threshold.
pub struct CountTrigger {
  max_count : Int
}

///|
/// Creates a count-based trigger.
pub fn CountTrigger::of(max_count : Int) -> CountTrigger {
  { max_count, }
}

///|
/// Evaluates whether current element count satisfies firing threshold.
pub fn CountTrigger::on_element(self : CountTrigger, current_count : Int) -> TriggerResult {
  if current_count >= self.max_count {
    FireAndPurge
  } else {
    Continue
  }
}

///|
/// Trigger that fires when numerical difference between successive values exceeds a delta.
pub struct DeltaTrigger {
  threshold : Double
  mut last_value : Double?
}

///|
/// Creates a numerical delta trigger.
pub fn DeltaTrigger::of(threshold : Double) -> DeltaTrigger {
  { threshold, last_value: None }
}

///|
/// Evaluates numerical delta from previous element value.
pub fn DeltaTrigger::on_element(self : DeltaTrigger, value : Double) -> TriggerResult {
  match self.last_value {
    Some(last) => {
      let mut diff = value - last
      if diff < 0.0 {
        diff = -diff
      }
      if diff >= self.threshold {
        self.last_value = Some(value)
        Fire
      } else {
        Continue
      }
    }
    None => {
      self.last_value = Some(value)
      Continue
    }
  }
}
