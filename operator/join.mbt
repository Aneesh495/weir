///|
/// Temporal Window Join Operator for streaming stream-to-stream correlation.
///
/// Buffers records from two streams within a temporal window relative to event-time
/// and emits matched pairs that share matching partition keys.

///|
/// Temporal interval window join operator.
pub struct TemporalJoinOperator {
  name : String
  lower_bound : @types.Duration
  upper_bound : @types.Duration
  left_buffer : Array[@types.StreamRecord]
  right_buffer : Array[@types.StreamRecord]
  metrics : OperatorMetrics
}

///|
/// Creates a TemporalJoinOperator with symmetric or asymmetric time bounds.
pub fn TemporalJoinOperator::new(
  name : String,
  lower_bound : @types.Duration,
  upper_bound : @types.Duration,
) -> TemporalJoinOperator {
  {
    name,
    lower_bound,
    upper_bound,
    left_buffer: [],
    right_buffer: [],
    metrics: OperatorMetrics::new(),
  }
}

///|
/// Processes a record arriving from the Left stream.
pub fn TemporalJoinOperator::process_left(
  self : TemporalJoinOperator,
  record : @types.StreamRecord,
  out : Collector,
) -> Unit {
  self.metrics.record_in()
  if record.is_data() {
    let key = record.get_key()
    let ts = record.get_timestamp()
    let min_ts = ts.sub_duration(self.lower_bound)
    let max_ts = ts.add_duration(self.upper_bound)

    // Probe right buffer for matching keys within [min_ts, max_ts]
    for i = 0; i < self.right_buffer.length(); i = i + 1 {
      let r_entry = self.right_buffer[i]
      if r_entry.get_key() == key {
        let r_ts = r_entry.get_timestamp()
        if r_ts >= min_ts && r_ts <= max_ts {
          let joined_payload = @types.EventPayload::map([
            ("left", record.get_payload()),
            ("right", r_entry.get_payload()),
          ])
          let joined_rec = @types.StreamRecord::data(key, ts, joined_payload)
          self.metrics.record_out()
          out.collect(joined_rec)
        }
      }
    }

    self.left_buffer.push(record)
  }
}

///|
/// Processes a record arriving from the Right stream.
pub fn TemporalJoinOperator::process_right(
  self : TemporalJoinOperator,
  record : @types.StreamRecord,
  out : Collector,
) -> Unit {
  self.metrics.record_in()
  if record.is_data() {
    let key = record.get_key()
    let ts = record.get_timestamp()
    let min_ts = ts.sub_duration(self.upper_bound)
    let max_ts = ts.add_duration(self.lower_bound)

    // Probe left buffer for matching keys within [min_ts, max_ts]
    for i = 0; i < self.left_buffer.length(); i = i + 1 {
      let l_entry = self.left_buffer[i]
      if l_entry.get_key() == key {
        let l_ts = l_entry.get_timestamp()
        if l_ts >= min_ts && l_ts <= max_ts {
          let joined_payload = @types.EventPayload::map([
            ("left", l_entry.get_payload()),
            ("right", record.get_payload()),
          ])
          let joined_rec = @types.StreamRecord::data(key, ts, joined_payload)
          self.metrics.record_out()
          out.collect(joined_rec)
        }
      }
    }

    self.right_buffer.push(record)
  }
}

///|
/// Advances watermark and purges expired entries from both buffers.
pub fn TemporalJoinOperator::on_watermark(
  self : TemporalJoinOperator,
  wm : @types.Watermark,
) -> Unit {
  let cutoff = wm.get_timestamp().sub_duration(self.lower_bound)

  let new_left : Array[@types.StreamRecord] = []
  for i = 0; i < self.left_buffer.length(); i = i + 1 {
    if self.left_buffer[i].get_timestamp() >= cutoff {
      new_left.push(self.left_buffer[i])
    }
  }
  self.left_buffer.clear()
  for i = 0; i < new_left.length(); i = i + 1 {
    self.left_buffer.push(new_left[i])
  }

  let new_right : Array[@types.StreamRecord] = []
  for i = 0; i < self.right_buffer.length(); i = i + 1 {
    if self.right_buffer[i].get_timestamp() >= cutoff {
      new_right.push(self.right_buffer[i])
    }
  }
  self.right_buffer.clear()
  for i = 0; i < new_right.length(); i = i + 1 {
    self.right_buffer.push(new_right[i])
  }
}
