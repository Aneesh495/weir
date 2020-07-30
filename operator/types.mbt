///|
/// Core interfaces and collector primitives for stream operators.
///
/// Manages operator telemetry counters and downstream emission buffers.

///|
/// Performance and event throughput metrics for an active operator.
pub struct OperatorMetrics {
  mut in_records : Int64
  mut out_records : Int64
  mut dropped_records : Int64
  mut current_watermark : @types.Watermark
}

///|
/// Creates a new clean OperatorMetrics tracker.
pub fn OperatorMetrics::new() -> OperatorMetrics {
  {
    in_records: 0L,
    out_records: 0L,
    dropped_records: 0L,
    current_watermark: @types.Watermark::from_timestamp(@types.Timestamp::min_value()),
  }
}

///|
/// Records an incoming record.
pub fn OperatorMetrics::record_in(self : OperatorMetrics) -> Unit {
  self.in_records = self.in_records + 1L
}

///|
/// Records an emitted output record.
pub fn OperatorMetrics::record_out(self : OperatorMetrics) -> Unit {
  self.out_records = self.out_records + 1L
}

///|
/// Records a dropped record.
pub fn OperatorMetrics::record_drop(self : OperatorMetrics) -> Unit {
  self.dropped_records = self.dropped_records + 1L
}

///|
/// Updates the operator watermark.
pub fn OperatorMetrics::update_watermark(
  self : OperatorMetrics,
  wm : @types.Watermark,
) -> Unit {
  if wm > self.current_watermark {
    self.current_watermark = wm
  }
}

///|
/// Downstream emission collector buffering records emitted by an operator.
pub struct Collector {
  outputs : Array[@types.StreamRecord]
}

///|
/// Creates a clean output Collector.
pub fn Collector::new() -> Collector {
  { outputs: [] }
}

///|
/// Emits a record into the collector.
pub fn Collector::collect(self : Collector, record : @types.StreamRecord) -> Unit {
  self.outputs.push(record)
}

///|
/// Returns count of collected records.
pub fn Collector::length(self : Collector) -> Int {
  self.outputs.length()
}

///|
/// Drains all collected records in FIFO order.
pub fn Collector::drain(self : Collector) -> Array[@types.StreamRecord] {
  let drained : Array[@types.StreamRecord] = []
  for i = 0; i < self.outputs.length(); i = i + 1 {
    drained.push(self.outputs[i])
  }
  self.outputs.clear()
  drained
}
