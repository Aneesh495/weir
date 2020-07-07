///|
/// Comprehensive tests for LSM state backend: BloomFilter, MemTable, WAL, SSTable, Compaction, and StateBackend.

///|
test "bloom filter membership and false negative guarantee" {
  let bloom = BloomFilter::new(50, 10)
  assert_eq(bloom.get_num_bits(), 512)

  bloom.add("user:1001")
  bloom.add("user:1002")
  bloom.add("device:alpha")

  // Must have 0 false negatives
  assert_true(bloom.might_contain("user:1001"))
  assert_true(bloom.might_contain("user:1002"))
  assert_true(bloom.might_contain("device:alpha"))

  // Fast negative verification (keys that are definitely not added)
  let mut negative_misses = 0
  for i = 0; i < 50; i = i + 1 {
    if !bloom.might_contain("nonexistent_random_key_" + i.to_string()) {
      negative_misses = negative_misses + 1
    }
  }
  // With 10 bits/key, false positive rate is < 2%, so almost all should miss
  assert_true(negative_misses > 45)
}

///|
test "memtable sorted insertions, updates, and tombstones" {
  let mem = MemTable::new()
  assert_eq(mem.entry_count(), 0)

  mem.put("k3", @types.EventPayload::string("val3"), 1L)
  mem.put("k1", @types.EventPayload::string("val1"), 2L)
  mem.put("k2", @types.EventPayload::string("val2"), 3L)

  assert_eq(mem.entry_count(), 3)
  assert_true(mem.contains("k1"))
  assert_true(mem.contains("k2"))
  assert_true(mem.contains("k3"))
  assert_false(mem.contains("k4"))

  // Verify binary search sorted order
  let entries = mem.to_entries()
  assert_eq(entries[0].key, "k1")
  assert_eq(entries[1].key, "k2")
  assert_eq(entries[2].key, "k3")

  // Update k2
  mem.put("k2", @types.EventPayload::string("val2_updated"), 4L)
  assert_eq(mem.entry_count(), 3)
  assert_true(mem.get("k2") == Some(@types.EventPayload::string("val2_updated")))

  // Delete k1 (tombstone)
  mem.delete("k1", 5L)
  assert_false(mem.contains("k1"))
  assert_true(mem.get("k1") is None)
}

///|
test "write-ahead log checksum validation and crash recovery replay" {
  let wal = WALLog::new()
  wal.append_put("stock:AAPL", @types.EventPayload::double(185.5), 10L)
  wal.append_put("stock:GOOG", @types.EventPayload::double(140.2), 11L)
  wal.append_delete("stock:AAPL", 12L)

  assert_eq(wal.frame_count(), 3)
  assert_true(wal.frames[0].is_valid())
  assert_true(wal.frames[1].is_valid())
  assert_true(wal.frames[2].is_valid())

  // Replay recovers state
  let recovered = wal.replay()
  assert_eq(recovered.length(), 2)
  assert_eq(recovered[0].key, "stock:AAPL")
  assert_true(recovered[0].is_tombstone())
  assert_eq(recovered[1].key, "stock:GOOG")
  assert_true(recovered[1].value == Some(@types.EventPayload::double(140.2)))
}

///|
test "sstable sparse index, bloom probing, and point queries" {
  let mem = MemTable::new()
  for i = 0; i < 40; i = i + 1 {
    let key = "metric_" + (if i < 10 { "0" + i.to_string() } else { i.to_string() })
    mem.put(key, @types.EventPayload::int(i.to_int64()), i.to_int64() + 1L)
  }

  // Freeze into SSTable with block size 8
  let sst = SSTable::from_entries(1L, 0, mem.to_entries(), 8)
  assert_eq(sst.get_metadata().entry_count, 40)
  assert_eq(sst.get_metadata().min_key, "metric_00")
  assert_eq(sst.get_metadata().max_key, "metric_39")
  assert_eq(sst.sparse_index.length(), 5)

  // Point lookups
  assert_true(sst.get("metric_00") == Some(@types.EventPayload::int(0L)))
  assert_true(sst.get("metric_25") == Some(@types.EventPayload::int(25L)))
  assert_true(sst.get("metric_39") == Some(@types.EventPayload::int(39L)))

  // Boundary lookups outside range
  assert_true(sst.get("aaa_below_min") is None)
  assert_true(sst.get("zzz_above_max") is None)
  assert_true(sst.get("metric_99_missing") is None)
}

///|
test "compaction worker merging and deduplication" {
  // SSTable 1: k1 (seq=1), k2 (seq=2)
  let mem1 = MemTable::new()
  mem1.put("k1", @types.EventPayload::string("v1_old"), 1L)
  mem1.put("k2", @types.EventPayload::string("v2"), 2L)
  let sst1 = SSTable::from_memtable(1L, 0, mem1)

  // SSTable 2: k1 (seq=5, updated), k3 (seq=3), k2 (seq=4, deleted)
  let mem2 = MemTable::new()
  mem2.put("k1", @types.EventPayload::string("v1_new"), 5L)
  mem2.delete("k2", 4L)
  mem2.put("k3", @types.EventPayload::string("v3"), 3L)
  let sst2 = SSTable::from_memtable(2L, 0, mem2)

  // Compact with tombstone purging enabled
  let worker = CompactionWorker::new(1, true)
  let compacted = worker.compact([sst1, sst2], 3L)

  assert_true(compacted.get("k1") == Some(@types.EventPayload::string("v1_new")))
  assert_true(compacted.get("k2") is None) // Deleted!
  assert_true(compacted.get("k3") == Some(@types.EventPayload::string("v3")))
  assert_eq(compacted.get_metadata().entry_count, 2)
}

///|
test "integrated state backend hierarchical lookup and snapshots" {
  let backend = StateBackend::new()

  // 1. Insert into active MemTable
  backend.put("order_1", @types.EventPayload::double(99.5))
  backend.put("order_2", @types.EventPayload::double(150.0))
  assert_true(backend.get("order_1") == Some(@types.EventPayload::double(99.5)))

  // 2. Flush to SSTable
  let flushed = backend.flush()
  assert_true(flushed is Some(_))
  assert_eq(backend.sstable_count(), 1)
  assert_true(backend.get("order_1") == Some(@types.EventPayload::double(99.5)))

  // 3. Update order_1 in MemTable (shadows older SSTable version)
  backend.put("order_1", @types.EventPayload::double(199.5))
  assert_true(backend.get("order_1") == Some(@types.EventPayload::double(199.5)))

  // 4. Create snapshot for checkpointing
  let snap = backend.snapshot()
  assert_true(snap.length() >= 2)

  // 5. Restore into a fresh backend
  let restored = StateBackend::new()
  restored.restore(snap)
  assert_true(restored.get("order_1") == Some(@types.EventPayload::double(199.5)))
  assert_true(restored.get("order_2") == Some(@types.EventPayload::double(150.0)))
}
