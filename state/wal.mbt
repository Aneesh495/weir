///|
/// Write-Ahead Log (WAL) with frame checksum validation for crash recovery.
///
/// Records every state mutation synchronously before memory admission to ensure
/// durability across unexpected failures.

///|
/// Simple CRC32 checksum implementation for WAL frame verification.
pub fn calculate_crc32(data : String, seq : Int64, op : Int) -> Int64 {
  let mut crc : Int64 = 0xFFFFFFFFL
  let seed = seq ^ (op.to_int64() << 16)

  for i = 0; i < data.length(); i = i + 1 {
    let byte_val = (data[i].to_int().to_int64() ^ seed) & 0xFFL
    crc = crc ^ byte_val
    for j = 0; j < 8; j = j + 1 {
      let mask = -(crc & 1L)
      crc = (crc >> 1) ^ (0xEDB88320L & mask)
    }
  }

  crc ^ 0xFFFFFFFFL
}

///|
/// Serialized mutation frame in the Write-Ahead Log.
pub struct WALFrame {
  seq : Int64
  op : Int // 1 = Put, 2 = Delete
  key : String
  value : @types.EventPayload?
  checksum : Int64
}

///|
pub impl Eq for WALFrame with equal(self, other) {
  self.seq == other.seq &&
  self.op == other.op &&
  self.key == other.key &&
  self.value == other.value &&
  self.checksum == other.checksum
}

///|
pub impl Eq for WALFrame with not_equal(self, other) {
  !(self == other)
}

///|
pub extend WALFrame with Eq::{equal, not_equal}

///|
/// Checks whether the frame's CRC32 checksum matches its payload.
pub fn WALFrame::is_valid(self : WALFrame) -> Bool {
  let val_str = match self.value {
    Some(v) => v.to_string()
    None => "$null$"
  }
  let payload = self.key + ":" + val_str
  let expected = calculate_crc32(payload, self.seq, self.op)
  self.checksum == expected
}

///|
/// In-memory Write-Ahead Log segment buffer.
pub struct WALLog {
  frames : Array[WALFrame]
}

///|
/// Creates a clean WALLog.
pub fn WALLog::new() -> WALLog {
  { frames: [] }
}

///|
/// Appends a Put mutation to the WAL.
pub fn WALLog::append_put(
  self : WALLog,
  key : String,
  value : @types.EventPayload,
  seq : Int64,
) -> Unit {
  let payload = key + ":" + value.to_string()
  let checksum = calculate_crc32(payload, seq, 1)
  self.frames.push({ seq, op: 1, key, value: Some(value), checksum })
}

///|
/// Appends a Delete mutation to the WAL.
pub fn WALLog::append_delete(self : WALLog, key : String, seq : Int64) -> Unit {
  let payload = key + ":$null$"
  let checksum = calculate_crc32(payload, seq, 2)
  self.frames.push({ seq, op: 2, key, value: None, checksum })
}

///|
/// Total recorded frames in log.
pub fn WALLog::frame_count(self : WALLog) -> Int {
  self.frames.length()
}

///|
/// Replays all valid WAL frames and reconstructs the latest MemTable state.
pub fn WALLog::replay(self : WALLog) -> Array[MemTableEntry] {
  let mem = MemTable::new()
  for i = 0; i < self.frames.length(); i = i + 1 {
    let frame = self.frames[i]
    if frame.is_valid() {
      match frame.value {
        Some(val) => mem.put(frame.key, val, frame.seq)
        None => mem.delete(frame.key, frame.seq)
      }
    }
  }
  mem.to_entries()
}

///|
/// Clears the WAL after a successful SSTable flush or checkpoint commit.
pub fn WALLog::clear(self : WALLog) -> Unit {
  self.frames.clear()
}
