///|
/// Failure recovery and deterministic state restoration coordinator.
///
/// Restores persistent operator state backends from completed checkpoint
/// snapshot manifests upon node restart or pipeline failure.

///|
/// Restoration plan binding operator identifiers to their snapshot entries.
pub struct RecoveryPlan {
  checkpoint_id : Int64
  operator_states : Array[(String, Array[@state.MemTableEntry])]
}

///|
/// Creates a RecoveryPlan from a checkpoint identifier and saved operator state snapshots.
pub fn RecoveryPlan::new(
  checkpoint_id : Int64,
  operator_states : Array[(String, Array[@state.MemTableEntry])],
) -> RecoveryPlan {
  { checkpoint_id, operator_states }
}

///|
/// Restores an individual operator's StateBackend from the recovery plan.
/// Returns true if operator snapshot was found and restored, or false if not present.
pub fn RecoveryPlan::restore_operator(
  self : RecoveryPlan,
  operator_id : String,
  backend : @state.StateBackend,
) -> Bool {
  let mut found = false
  for i = 0; i < self.operator_states.length(); i = i + 1 {
    let (op_id, entries) = self.operator_states[i]
    if op_id == operator_id {
      backend.restore(entries)
      found = true
      break
    }
  }
  found
}

///|
/// Returns total count of operators included in this recovery plan.
pub fn RecoveryPlan::operator_count(self : RecoveryPlan) -> Int {
  self.operator_states.length()
}
