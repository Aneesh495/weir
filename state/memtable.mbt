///|
/// In-memory sorted active buffer (MemTable) for LSM state backend.
///
/// Maintains sorted order across active mutations to facilitate fast binary
/// search queries and sequential SSTable flushing.

///|
/// Key-value mutation record in the MemTable.
pub struct MemTableEntry {
  key : String
  value : @types.EventPayload?
  seq : Int64
}

///|
pub impl Eq for MemTableEntry with equal(self, other) {
  self.key == other.key && self.value == other.value && self.seq == other.seq
}

///|
pub impl Eq for MemTableEntry with not_equal(self, other) {
  !(self == other)
}

///|
pub extend MemTableEntry with Eq::{equal, not_equal}

///|
/// Creates a new MemTableEntry.
pub fn MemTableEntry::new(
  key : String,
  value : @types.EventPayload?,
  seq : Int64,
) -> MemTableEntry {
  { key, value, seq }
}

///|
/// Returns true if entry represents a deletion tombstone.
pub fn MemTableEntry::is_tombstone(self : MemTableEntry) -> Bool {
  self.value is None
}

///|
/// In-memory sorted table storing mutations before flush.
pub struct MemTable {
  entries : Array[MemTableEntry]
  mut approx_bytes : Int
}

///|
/// Creates an empty MemTable.
pub fn MemTable::new() -> MemTable {
  { entries: [], approx_bytes: 0 }
}

///|
/// Performs binary search over entries to find key index or insertion position.
/// Returns (index, found).
fn MemTable::find_index(self : MemTable, key : String) -> (Int, Bool) {
  let mut low = 0
  let mut high = self.entries.length() - 1

  while low <= high {
    let mid = (low + high) / 2
    let mid_key = self.entries[mid].key
    if mid_key == key {
      return (mid, true)
    } else if mid_key < key {
      low = mid + 1
    } else {
      high = mid - 1
    }
  }

  (low, false)
}

///|
/// Inserts or updates a key-value pair in the sorted MemTable.
pub fn MemTable::put(
  self : MemTable,
  key : String,
  value : @types.EventPayload,
  seq : Int64,
) -> Unit {
  let (idx, found) = self.find_index(key)
  let entry = MemTableEntry::new(key, Some(value), seq)

  if found {
    self.entries[idx] = entry
  } else {
    self.entries.insert(idx, entry)
    self.approx_bytes = self.approx_bytes + key.length() + 32
  }
}

///|
/// Writes a deletion tombstone for a key.
pub fn MemTable::delete(self : MemTable, key : String, seq : Int64) -> Unit {
  let (idx, found) = self.find_index(key)
  let entry = MemTableEntry::new(key, None, seq)

  if found {
    self.entries[idx] = entry
  } else {
    self.entries.insert(idx, entry)
    self.approx_bytes = self.approx_bytes + key.length() + 16
  }
}

///|
/// Retrieves a value for a key from the MemTable.
/// Returns Some(value) if active, or None if not found or deleted by tombstone.
pub fn MemTable::get(self : MemTable, key : String) -> @types.EventPayload? {
  let (idx, found) = self.find_index(key)
  if found {
    self.entries[idx].value
  } else {
    None
  }
}

///|
/// Checks if the MemTable contains an active (non-tombstone) value for key.
pub fn MemTable::contains(self : MemTable, key : String) -> Bool {
  match self.get(key) {
    Some(_) => true
    None => false
  }
}

///|
/// Returns total entries currently in the MemTable.
pub fn MemTable::entry_count(self : MemTable) -> Int {
  self.entries.length()
}

///|
/// Returns approximate memory size in bytes.
pub fn MemTable::approximate_size(self : MemTable) -> Int {
  self.approx_bytes
}

///|
/// Returns a shallow copy of all entries in sorted key order.
pub fn MemTable::to_entries(self : MemTable) -> Array[MemTableEntry] {
  let copy : Array[MemTableEntry] = []
  for i = 0; i < self.entries.length(); i = i + 1 {
    copy.push(self.entries[i])
  }
  copy
}

///|
/// Clears the MemTable.
pub fn MemTable::clear(self : MemTable) -> Unit {
  self.entries.clear()
  self.approx_bytes = 0
}
