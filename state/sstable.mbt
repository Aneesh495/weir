///|
/// Immutable Sorted String Table (SSTable) file format for persistent state tiers.
///
/// Implements two-level sparse block indexing, range boundary checks, and Bloom
/// filter pre-filtering for fast point queries and sequential scans.

///|
/// Sparse block pointer tracking the starting key and entry bounds of a block.
pub struct BlockHandle {
  first_key : String
  start_index : Int
  entry_count : Int
}

///|
/// Summary metadata for an immutable SSTable.
pub struct SSTableMetadata {
  table_id : Int64
  level : Int
  min_key : String
  max_key : String
  entry_count : Int
}

///|
/// Immutable SSTable holding sorted entries, sparse index, and Bloom filter.
pub struct SSTable {
  metadata : SSTableMetadata
  bloom : BloomFilter
  sparse_index : Array[BlockHandle]
  entries : Array[MemTableEntry]
}

///|
/// Constructs an SSTable from a sorted list of MemTable entries.
pub fn SSTable::from_entries(
  table_id : Int64,
  level : Int,
  entries : Array[MemTableEntry],
  block_size : Int,
) -> SSTable {
  let count = entries.length()
  if count == 0 {
    let empty_meta : SSTableMetadata = {
      table_id,
      level,
      min_key: "",
      max_key: "",
      entry_count: 0,
    }
    let empty_bloom = BloomFilter::new(10, 10)
    {
      metadata: empty_meta,
      bloom: empty_bloom,
      sparse_index: [],
      entries: [],
    }
  } else {
    let min_key = entries[0].key
    let max_key = entries[count - 1].key
    let meta : SSTableMetadata = {
      table_id,
      level,
      min_key,
      max_key,
      entry_count: count,
    }

    // Build Bloom filter
    let bloom = BloomFilter::new(count, 10)
    for i = 0; i < count; i = i + 1 {
      bloom.add(entries[i].key)
    }

    // Build sparse two-level block index
    let index : Array[BlockHandle] = []
    let actual_block_sz = if block_size < 1 { 16 } else { block_size }
    let mut offset = 0
    while offset < count {
      let remaining = count - offset
      let cur_sz = if remaining < actual_block_sz { remaining } else { actual_block_sz }
      let handle : BlockHandle = {
        first_key: entries[offset].key,
        start_index: offset,
        entry_count: cur_sz,
      }
      index.push(handle)
      offset = offset + cur_sz
    }

    { metadata: meta, bloom, sparse_index: index, entries }
  }
}

///|
/// Constructs an SSTable by freezing an active MemTable.
pub fn SSTable::from_memtable(
  table_id : Int64,
  level : Int,
  memtable : MemTable,
) -> SSTable {
  SSTable::from_entries(table_id, level, memtable.to_entries(), 16)
}

///|
/// Fast query point lookup in the SSTable.
/// Uses key range pruning, Bloom filter probing, sparse index binary search,
/// and block scan. Returns Some(val) if found, or None if not found or deleted.
pub fn SSTable::get(self : SSTable, key : String) -> @types.EventPayload? {
  if self.metadata.entry_count == 0 {
    return None
  }

  // 1. Range bounds check
  if key < self.metadata.min_key || key > self.metadata.max_key {
    return None
  }

  // 2. Bloom filter membership test
  if !self.bloom.might_contain(key) {
    return None
  }

  // 3. Binary search on sparse index to identify target block
  let mut low = 0
  let mut high = self.sparse_index.length() - 1
  let mut target_block_idx = 0

  while low <= high {
    let mid = (low + high) / 2
    if self.sparse_index[mid].first_key <= key {
      target_block_idx = mid
      low = mid + 1
    } else {
      high = mid - 1
    }
  }

  // 4. Scan within candidate block
  let handle = self.sparse_index[target_block_idx]
  let start = handle.start_index
  let end = start + handle.entry_count

  let mut found_val : @types.EventPayload? = None
  for i = start; i < end; i = i + 1 {
    let entry = self.entries[i]
    if entry.key == key {
      found_val = entry.value
      break
    } else if entry.key > key {
      break
    }
  }

  found_val
}

///|
/// Returns SSTable metadata.
pub fn SSTable::get_metadata(self : SSTable) -> SSTableMetadata {
  self.metadata
}

///|
/// Returns raw entries for compaction merging.
pub fn SSTable::get_entries(self : SSTable) -> Array[MemTableEntry] {
  self.entries
}
