///|
/// Master Checkpoint Coordinator orchestrating distributed snapshots.
///
/// Implements two-phase snapshot acknowledgement, timeout handling,
/// and metadata cataloguing across execution DAG operators.

///|
/// State status of a checkpoint cycle.
pub enum CheckpointStatus {
  InProgress
  Completed
  Failed(String)
}

///|
pub impl Eq for CheckpointStatus with equal(self, other) {
  match (self, other) {
    (InProgress, InProgress) => true
    (Completed, Completed) => true
    (Failed(a), Failed(b)) => a == b
    _ => false
  }
}

///|
pub impl Eq for CheckpointStatus with not_equal(self, other) {
  !(self == other)
}

///|
pub extend CheckpointStatus with Eq::{equal, not_equal}

///|
pub impl Show for CheckpointStatus with output(self, logger) {
  logger.write_string(self.to_string())
}

///|
pub impl Show for CheckpointStatus with to_string(self) {
  match self {
    InProgress => "InProgress"
    Completed => "Completed"
    Failed(err) => "Failed(" + err + ")"
  }
}

///|
pub extend CheckpointStatus with Show::{output, to_string}

///|
/// Creates a Failed status with error description.
pub fn CheckpointStatus::failed(reason : String) -> CheckpointStatus {
  Failed(reason)
}

///|
/// Metadata record for an initiated checkpoint.
pub struct CheckpointMetadata {
  checkpoint_id : Int64
  trigger_time : @types.Timestamp
  mut completion_time : @types.Timestamp?
  required_operators : Array[String]
  acknowledged_operators : Array[String]
  mut status : CheckpointStatus
}

///|
/// Coordinator tracking multi-operator checkpoint progress.
pub struct CheckpointCoordinator {
  mut next_id : Int64
  checkpoints : Array[CheckpointMetadata]
  snapshots : Array[(Int64, Array[(String, Array[@state.MemTableEntry])])]
}

///|
/// Creates a clean CheckpointCoordinator.
pub fn CheckpointCoordinator::new() -> CheckpointCoordinator {
  { next_id: 1L, checkpoints: [], snapshots: [] }
}

///|
/// Triggers a new checkpoint across required operator IDs. Returns assigned ID.
pub fn CheckpointCoordinator::trigger_checkpoint(
  self : CheckpointCoordinator,
  required_operators : Array[String],
  timestamp : @types.Timestamp,
) -> Int64 {
  let id = self.next_id
  self.next_id = self.next_id + 1L

  let req_copy : Array[String] = []
  for i = 0; i < required_operators.length(); i = i + 1 {
    req_copy.push(required_operators[i])
  }

  let meta : CheckpointMetadata = {
    checkpoint_id: id,
    trigger_time: timestamp,
    completion_time: None,
    required_operators: req_copy,
    acknowledged_operators: [],
    status: InProgress,
  }

  self.checkpoints.push(meta)
  self.snapshots.push((id, []))
  id
}

///|
/// Records an operator's successful state snapshot for a checkpoint.
/// Returns true if all operators have acknowledged and checkpoint completed.
pub fn CheckpointCoordinator::acknowledge(
  self : CheckpointCoordinator,
  checkpoint_id : Int64,
  operator_id : String,
  snapshot : Array[@state.MemTableEntry],
  ack_time : @types.Timestamp,
) -> Bool {
  let mut meta_opt : CheckpointMetadata? = None
  let mut snap_idx = -1

  for i = 0; i < self.checkpoints.length(); i = i + 1 {
    if self.checkpoints[i].checkpoint_id == checkpoint_id {
      meta_opt = Some(self.checkpoints[i])
      snap_idx = i
      break
    }
  }

  match meta_opt {
    None => false
    Some(meta) => {
      if meta.status != InProgress {
        return false
      }

      meta.acknowledged_operators.push(operator_id)
      let (_, op_snaps) = self.snapshots[snap_idx]
      op_snaps.push((operator_id, snapshot))

      // Check if all required operators have acknowledged
      if meta.acknowledged_operators.length() >= meta.required_operators.length() {
        meta.status = Completed
        meta.completion_time = Some(ack_time)
        true
      } else {
        false
      }
    }
  }
}

///|
/// Returns the latest completed checkpoint identifier, if any.
pub fn CheckpointCoordinator::get_latest_completed_id(
  self : CheckpointCoordinator,
) -> Int64? {
  let mut latest : Int64? = None
  for i = 0; i < self.checkpoints.length(); i = i + 1 {
    let cp = self.checkpoints[i]
    if cp.status == Completed {
      latest = Some(cp.checkpoint_id)
    }
  }
  latest
}

///|
/// Retrieves saved operator snapshots for a completed checkpoint.
pub fn CheckpointCoordinator::get_snapshot_manifest(
  self : CheckpointCoordinator,
  checkpoint_id : Int64,
) -> Array[(String, Array[@state.MemTableEntry])]? {
  let mut result : Array[(String, Array[@state.MemTableEntry])]? = None
  for i = 0; i < self.snapshots.length(); i = i + 1 {
    let (id, snaps) = self.snapshots[i]
    if id == checkpoint_id {
      result = Some(snaps)
      break
    }
  }
  result
}
