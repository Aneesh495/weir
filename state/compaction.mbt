///|
/// Background LSM compaction worker for SSTable merging and garbage collection.
///
/// Implements K-way merge sort, sequence number deduplication, and tombstone
/// purging to consolidate fragmented storage tiers.

///|
/// Compaction worker combining multiple SSTables into a consolidated SSTable.
pub struct CompactionWorker {
  target_level : Int
  purge_tombstones : Bool
}

///|
/// Creates a new CompactionWorker for a target LSM level.
pub fn CompactionWorker::new(
  target_level : Int,
  purge_tombstones : Bool,
) -> CompactionWorker {
  { target_level, purge_tombstones }
}

///|
/// Merges two sorted arrays of entries, resolving duplicate keys by highest sequence number.
fn merge_two_entry_arrays(
  left : Array[MemTableEntry],
  right : Array[MemTableEntry],
  purge_tombstones : Bool,
) -> Array[MemTableEntry] {
  let merged : Array[MemTableEntry] = []
  let mut i = 0
  let mut j = 0

  while i < left.length() && j < right.length() {
    let e_left = left[i]
    let e_right = right[j]

    if e_left.key < e_right.key {
      if !purge_tombstones || !e_left.is_tombstone() {
        merged.push(e_left)
      }
      i = i + 1
    } else if e_left.key > e_right.key {
      if !purge_tombstones || !e_right.is_tombstone() {
        merged.push(e_right)
      }
      j = j + 1
    } else {
      // Keys are identical: select highest sequence number
      let winner = if e_left.seq >= e_right.seq { e_left } else { e_right }
      if !purge_tombstones || !winner.is_tombstone() {
        merged.push(winner)
      }
      i = i + 1
      j = j + 1
    }
  }

  while i < left.length() {
    let e = left[i]
    if !purge_tombstones || !e.is_tombstone() {
      merged.push(e)
    }
    i = i + 1
  }

  while j < right.length() {
    let e = right[j]
    if !purge_tombstones || !e.is_tombstone() {
      merged.push(e)
    }
    j = j + 1
  }

  merged
}

///|
/// Executes compaction over a list of SSTables, returning a single consolidated SSTable.
pub fn CompactionWorker::compact(
  self : CompactionWorker,
  tables : Array[SSTable],
  new_table_id : Int64,
) -> SSTable {
  if tables.length() == 0 {
    return SSTable::from_entries(new_table_id, self.target_level, [], 16)
  }

  let mut current_entries = tables[0].get_entries()
  for i = 1; i < tables.length(); i = i + 1 {
    current_entries = merge_two_entry_arrays(
      current_entries,
      tables[i].get_entries(),
      self.purge_tombstones,
    )
  }

  SSTable::from_entries(new_table_id, self.target_level, current_entries, 16)
}
