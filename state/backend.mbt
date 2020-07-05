///|
/// Integrated Log-Structured Merge (LSM) state backend.
///
/// Unifies in-memory MemTable, Write-Ahead Log (WAL), immutable SSTables,
/// and compaction into a coherent stateful storage engine.

///|
/// High-performance LSM state backend with hierarchical tier querying.
pub struct StateBackend {
  memtable : MemTable
  wal : WALLog
  sstables : Array[SSTable]
  mut next_seq : Int64
  mut next_table_id : Int64
  memtable_flush_threshold : Int
}

///|
/// Creates a new StateBackend with default flush thresholds.
pub fn StateBackend::new() -> StateBackend {
  {
    memtable: MemTable::new(),
    wal: WALLog::new(),
    sstables: [],
    next_seq: 1L,
    next_table_id: 100L,
    memtable_flush_threshold: 64,
  }
}

///|
/// Inserts or updates a key-value pair with WAL write and active MemTable indexing.
pub fn StateBackend::put(
  self : StateBackend,
  key : String,
  value : @types.EventPayload,
) -> Unit {
  let seq = self.next_seq
  self.next_seq = self.next_seq + 1L

  self.wal.append_put(key, value, seq)
  self.memtable.put(key, value, seq)

  if self.memtable.entry_count() >= self.memtable_flush_threshold {
    let _ = self.flush()
  }
}

///|
/// Deletes a key by appending a tombstone to WAL and MemTable.
pub fn StateBackend::delete(self : StateBackend, key : String) -> Unit {
  let seq = self.next_seq
  self.next_seq = self.next_seq + 1L

  self.wal.append_delete(key, seq)
  self.memtable.delete(key, seq)
}

///|
/// Queries a key across memory and persistent tiers in hierarchical precedence:
/// 1. Active MemTable (newest)
/// 2. SSTables in reverse chronological order (newest table to oldest table)
pub fn StateBackend::get(self : StateBackend, key : String) -> @types.EventPayload? {
  // 1. Check active MemTable
  let (idx, found_in_mem) = self.memtable.find_index(key)
  if found_in_mem {
    return self.memtable.entries[idx].value
  }

  // 2. Search SSTables newest to oldest
  for i = self.sstables.length() - 1; i >= 0; i = i - 1 {
    let sst = self.sstables[i]
    let res = sst.get(key)
    if res is Some(_) {
      return res
    }
  }

  None
}

///|
/// Flushes the active MemTable into an immutable SSTable and resets the WAL.
pub fn StateBackend::flush(self : StateBackend) -> SSTable? {
  if self.memtable.entry_count() == 0 {
    return None
  }

  let table_id = self.next_table_id
  self.next_table_id = self.next_table_id + 1L

  let sst = SSTable::from_memtable(table_id, 0, self.memtable)
  self.sstables.push(sst)
  self.memtable.clear()
  self.wal.clear()

  Some(sst)
}

///|
/// Triggers compaction across existing SSTables when count exceeds threshold.
pub fn StateBackend::maybe_compact(self : StateBackend) -> Unit {
  if self.sstables.length() >= 4 {
    let worker = CompactionWorker::new(1, false)
    let compacted = worker.compact(self.sstables, self.next_table_id)
    self.next_table_id = self.next_table_id + 1L
    self.sstables.clear()
    self.sstables.push(compacted)
  }
}

///|
/// Creates a consistent snapshot of all active and flushed state for checkpointing.
pub fn StateBackend::snapshot(self : StateBackend) -> Array[MemTableEntry] {
  let worker = CompactionWorker::new(0, false)
  let all_tables : Array[SSTable] = []
  for i = 0; i < self.sstables.length(); i = i + 1 {
    all_tables.push(self.sstables[i])
  }

  if self.memtable.entry_count() > 0 {
    let mem_sst = SSTable::from_memtable(0L, 0, self.memtable)
    all_tables.push(mem_sst)
  }

  if all_tables.length() == 0 {
    []
  } else {
    let consolidated = worker.compact(all_tables, 0L)
    consolidated.get_entries()
  }
}

///|
/// Restores the StateBackend from a recovered checkpoint snapshot.
pub fn StateBackend::restore(
  self : StateBackend,
  snapshot_entries : Array[MemTableEntry],
) -> Unit {
  self.memtable.clear()
  self.wal.clear()
  self.sstables.clear()

  let table_id = self.next_table_id
  self.next_table_id = self.next_table_id + 1L
  let sst = SSTable::from_entries(table_id, 0, snapshot_entries, 16)
  self.sstables.push(sst)
}

///|
/// Returns total count of SSTable files currently active.
pub fn StateBackend::sstable_count(self : StateBackend) -> Int {
  self.sstables.length()
}
