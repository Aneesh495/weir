///|
/// Output sinks for streaming pipeline termination.
///
/// Implements in-memory diagnostic buffers and blackhole sinks.

///|
/// In-memory sink recording all received records for testing and verification.
pub struct MemorySink {
  records : Array[@types.StreamRecord]
}

///|
/// Creates a clean MemorySink.
pub fn MemorySink::new() -> MemorySink {
  { records: [] }
}

///|
/// Receives and buffers a stream record.
pub fn MemorySink::collect(self : MemorySink, record : @types.StreamRecord) -> Unit {
  self.records.push(record)
}

///|
/// Returns total count of records received.
pub fn MemorySink::count(self : MemorySink) -> Int {
  self.records.length()
}

///|
/// Drains all buffered records in FIFO order.
pub fn MemorySink::drain(self : MemorySink) -> Array[@types.StreamRecord] {
  let result : Array[@types.StreamRecord] = []
  for i = 0; i < self.records.length(); i = i + 1 {
    result.push(self.records[i])
  }
  self.records.clear()
  result
}

///|
/// High-throughput blackhole sink for benchmarking raw pipeline throughput.
pub struct DiscardSink {
  mut count : Int64
}

///|
/// Creates a clean DiscardSink.
pub fn DiscardSink::new() -> DiscardSink {
  { count: 0L }
}

///|
/// Discards incoming record while advancing atomic observation counter.
pub fn DiscardSink::collect(self : DiscardSink, _record : @types.StreamRecord) -> Unit {
  self.count = self.count + 1L
}

///|
/// Returns total count of discarded records.
pub fn DiscardSink::get_count(self : DiscardSink) -> Int64 {
  self.count
}
