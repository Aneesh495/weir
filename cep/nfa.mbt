///|
/// Non-deterministic Finite Automaton (NFA) model for Complex Event Processing.
///
/// Compiles high-level declarative pattern sequences into state transition graphs
/// capable of handling relaxed contiguity and concurrent branching matches.

///|
/// Transition edge type in the NFA.
pub enum TransitionType {
  Take
  Ignore
}

///|
/// State transition edge with condition predicate and target state identifier.
pub struct NFATransition {
  target_state : Int
  condition : (@types.StreamRecord) -> Bool
  trans_type : TransitionType
}

///|
/// Individual automaton state node.
pub struct NFAState {
  state_id : Int
  name : String
  is_final : Bool
  transitions : Array[NFATransition]
}

///|
/// Compiled NFA graph representing the full pattern sequence.
pub struct NFA {
  states : Array[NFAState]
  window_time : @types.Duration?
}

///|
/// Compiler translating declarative PatternSequence into an executable NFA.
pub struct NFACompiler {
  _unit : Unit
}

///|
/// Creates an NFACompiler instance.
pub fn NFACompiler::new() -> NFACompiler {
  { _unit: () }
}

///|
/// Compiles a PatternSequence into an NFA.
pub fn NFACompiler::compile(pattern : PatternSequence) -> NFA {
  let num_stages = pattern.stage_count()
  let states : Array[NFAState] = []

  // Create states: 0 .. num_stages. State num_stages is final.
  for i = 0; i <= num_stages; i = i + 1 {
    let is_final = i == num_stages
    let name = if is_final { "$final" } else { pattern.stages[i].name }
    let state : NFAState = {
      state_id: i,
      name,
      is_final,
      transitions: [],
    }
    states.push(state)
  }

  // Wire transitions
  for i = 0; i < num_stages; i = i + 1 {
    let stage = pattern.stages[i]

    // 1. Take transition to next state on condition match
    let take_trans : NFATransition = {
      target_state: i + 1,
      condition: stage.condition,
      trans_type: Take,
    }
    states[i].transitions.push(take_trans)

    // 2. Relaxed contiguity: intermediate states (i > 0) have self-loop to ignore unrelated events
    if i > 0 {
      let ignore_trans : NFATransition = {
        target_state: i,
        condition: fn(_r) { true },
        trans_type: Ignore,
      }
      states[i].transitions.push(ignore_trans)
    }
  }

  { states, window_time: pattern.get_window_time() }
}
