///|
/// CEP Execution Engine driving NFA state transitions and match evaluation.
///
/// Evaluates streaming events concurrently across branching partial matches and
/// emits full pattern sequences upon reaching accepting states.

///|
/// In-flight partial pattern match traversing the NFA state graph.
pub struct PartialMatch {
  mut current_state : Int
  start_time : @types.Timestamp
  events : Array[@types.StreamRecord]
}

///|
/// Creates a new PartialMatch beginning at state 0.
pub fn PartialMatch::new(
  start_event : @types.StreamRecord,
) -> PartialMatch {
  {
    current_state: 1,
    start_time: start_event.get_timestamp(),
    events: [start_event],
  }
}

///|
/// Clones a partial match for branching state exploration.
pub fn PartialMatch::clone(self : PartialMatch) -> PartialMatch {
  let ev_copy : Array[@types.StreamRecord] = []
  for i = 0; i < self.events.length(); i = i + 1 {
    ev_copy.push(self.events[i])
  }
  {
    current_state: self.current_state,
    start_time: self.start_time,
    events: ev_copy,
  }
}

///|
/// Runtime execution engine managing active pattern matches.
pub struct CEPEngine {
  nfa : NFA
  active_matches : Array[PartialMatch]
  final_state_id : Int
}

///|
/// Creates a CEPEngine from a compiled NFA.
pub fn CEPEngine::new(nfa : NFA) -> CEPEngine {
  let final_id = nfa.states.length() - 1
  { nfa, active_matches: [], final_state_id: final_id }
}

///|
/// Processes an incoming event through the NFA.
/// Returns all complete pattern match sequences triggered by this event.
pub fn CEPEngine::process_event(
  self : CEPEngine,
  record : @types.StreamRecord,
) -> Array[Array[@types.StreamRecord]] {
  if !record.is_data() {
    return []
  }

  let completed : Array[Array[@types.StreamRecord]] = []
  let next_active : Array[PartialMatch] = []
  let cur_ts = record.get_timestamp()

  // 1. Advance existing active matches
  for i = 0; i < self.active_matches.length(); i = i + 1 {
    let p_match = self.active_matches[i]

    // Check temporal window expiration
    match self.nfa.window_time {
      Some(max_dur) => {
        if cur_ts.diff(p_match.start_time) > max_dur {
          continue // Match timed out
        }
      }
      None => ()
    }

    let state = self.nfa.states[p_match.current_state]
    let mut took_step = false

    for j = 0; j < state.transitions.length(); j = j + 1 {
      let trans = state.transitions[j]
      match trans.trans_type {
        Take => {
          if (trans.condition)(record) {
            let advanced = p_match.clone()
            advanced.current_state = trans.target_state
            advanced.events.push(record)

            if advanced.current_state == self.final_state_id {
              completed.push(advanced.events)
            } else {
              next_active.push(advanced)
            }
            took_step = true
          }
        }
        Ignore => ()
      }
    }

    // If relaxed contiguity permits ignoring this event, retain original match
    if !took_step {
      next_active.push(p_match)
    }
  }

  // 2. Check if a brand-new match begins with this event (State 0 Take transition)
  let state_0 = self.nfa.states[0]
  if state_0.transitions.length() > 0 {
    let start_trans = state_0.transitions[0]
    if (start_trans.condition)(record) {
      if start_trans.target_state == self.final_state_id {
        // Single-event pattern
        completed.push([record])
      } else {
        let new_match = PartialMatch::new(record)
        next_active.push(new_match)
      }
    }
  }

  self.active_matches.clear()
  for i = 0; i < next_active.length(); i = i + 1 {
    self.active_matches.push(next_active[i])
  }

  completed
}

///|
/// Returns count of active in-flight partial matches.
pub fn CEPEngine::active_match_count(self : CEPEngine) -> Int {
  self.active_matches.length()
}
