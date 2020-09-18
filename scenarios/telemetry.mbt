///|
/// Real-world scenario: Distributed Microservice Mesh SLA & Anomaly Tracking.
///
/// Ingests RPC span traces, computes sliding error rates and latency percentiles,
/// and fires automated circuit-breaker alerts when SLA thresholds are breached.

///|
/// Distributed RPC span trace observation.
pub struct RpcSpan {
  trace_id : String
  service_name : String
  endpoint : String
  latency_ms : Double
  status_code : Int
  timestamp : @types.Timestamp
}

///|
/// Constructs an RpcSpan observation.
pub fn RpcSpan::new(
  trace_id : String,
  service_name : String,
  endpoint : String,
  latency_ms : Double,
  status_code : Int,
  timestamp : @types.Timestamp,
) -> RpcSpan {
  { trace_id, service_name, endpoint, latency_ms, status_code, timestamp }
}

///|
/// Converts RpcSpan into a StreamRecord.
pub fn RpcSpan::to_record(self : RpcSpan) -> @types.StreamRecord {
  let payload = @types.EventPayload::map([
    ("trace_id", @types.EventPayload::string(self.trace_id)),
    ("endpoint", @types.EventPayload::string(self.endpoint)),
    ("latency", @types.EventPayload::double(self.latency_ms)),
    ("status", @types.EventPayload::int(self.status_code.to_int64())),
  ])
  @types.StreamRecord::data(self.service_name, self.timestamp, payload)
}

///|
/// Rolling service latency and error SLA accumulator.
pub struct SlaAggregator {
  mut total_requests : Int64
  mut error_requests : Int64
  latencies : Array[Double]
}

///|
/// Creates a clean SlaAggregator.
pub fn SlaAggregator::new() -> SlaAggregator {
  { total_requests: 0L, error_requests: 0L, latencies: [] }
}

///|
/// Ingests an RPC span.
pub fn SlaAggregator::add_span(
  self : SlaAggregator,
  latency_ms : Double,
  status_code : Int,
) -> Unit {
  self.total_requests = self.total_requests + 1L
  if status_code >= 500 {
    self.error_requests = self.error_requests + 1L
  }
  self.latencies.push(latency_ms)
}

///|
/// Computes current error rate (0.0 to 1.0).
pub fn SlaAggregator::error_rate(self : SlaAggregator) -> Double {
  if self.total_requests == 0L {
    0.0
  } else {
    self.error_requests.to_double() / self.total_requests.to_double()
  }
}

///|
/// Sorts buffered latencies in ascending order.
fn SlaAggregator::sort_latencies(self : SlaAggregator) -> Unit {
  // Simple insertion sort for bounded sample window
  let len = self.latencies.length()
  for i = 1; i < len; i = i + 1 {
    let key = self.latencies[i]
    let mut j = i - 1
    while j >= 0 && self.latencies[j] > key {
      self.latencies[j + 1] = self.latencies[j]
      j = j - 1
    }
    self.latencies[j + 1] = key
  }
}

///|
/// Computes specified percentile latency (e.g. 0.99 for p99).
pub fn SlaAggregator::percentile(
  self : SlaAggregator,
  pct : Double,
) -> Double {
  let len = self.latencies.length()
  if len == 0 {
    0.0
  } else {
    self.sort_latencies()
    let mut idx = (len.to_double() * pct).to_int()
    if idx >= len {
      idx = len - 1
    }
    self.latencies[idx]
  }
}

///|
/// Checks if current metrics breach operational SLA thresholds.
pub fn SlaAggregator::breaches_sla(
  self : SlaAggregator,
  p99_sla_ms : Double,
  max_error_rate : Double,
) -> Bool {
  let cur_error_rate = self.error_rate()
  let cur_p99 = self.percentile(0.99)
  cur_error_rate > max_error_rate || cur_p99 > p99_sla_ms
}
