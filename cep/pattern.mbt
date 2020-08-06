///|
/// Complex Event Processing (CEP) Pattern specification DSL.
///
/// Enables declarative specification of sequential event patterns with temporal
/// contiguity constraints, negation, and maximum duration windows.

///|
/// Single stage specification within a composite event pattern.
pub struct PatternStage {
  name : String
  condition : (@types.StreamRecord) -> Bool
  is_optional : Bool
  is_repeat : Bool
  is_negated : Bool
}

///|
/// Declarative pattern builder defining an ordered event sequence.
pub struct PatternSequence {
  stages : Array[PatternStage]
  mut window_time : @types.Duration?
}

///|
/// Begins a new pattern sequence with the initial qualifying event stage.
pub fn PatternSequence::begin(
  name : String,
  condition : (@types.StreamRecord) -> Bool,
) -> PatternSequence {
  let stage : PatternStage = {
    name,
    condition,
    is_optional: false,
    is_repeat: false,
    is_negated: false,
  }
  { stages: [stage], window_time: None }
}

///|
/// Appends a subsequent required stage following relaxed contiguity.
pub fn PatternSequence::followed_by(
  self : PatternSequence,
  name : String,
  condition : (@types.StreamRecord) -> Bool,
) -> PatternSequence {
  let stage : PatternStage = {
    name,
    condition,
    is_optional: false,
    is_repeat: false,
    is_negated: false,
  }
  self.stages.push(stage)
  self
}

///|
/// Appends a negative stage that must NOT occur between adjacent pattern matches.
pub fn PatternSequence::not_followed_by(
  self : PatternSequence,
  name : String,
  condition : (@types.StreamRecord) -> Bool,
) -> PatternSequence {
  let stage : PatternStage = {
    name,
    condition,
    is_optional: false,
    is_repeat: false,
    is_negated: true,
  }
  self.stages.push(stage)
  self
}

///|
/// Constrains the total duration between first and last events in a match.
pub fn PatternSequence::within(
  self : PatternSequence,
  window_time : @types.Duration,
) -> PatternSequence {
  self.window_time = Some(window_time)
  self
}

///|
/// Returns total stages in the pattern sequence.
pub fn PatternSequence::stage_count(self : PatternSequence) -> Int {
  self.stages.length()
}

///|
/// Returns the configured temporal duration constraint.
pub fn PatternSequence::get_window_time(
  self : PatternSequence,
) -> @types.Duration? {
  self.window_time
}
