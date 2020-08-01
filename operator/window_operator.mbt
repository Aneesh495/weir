///|
/// Stateful Window Operator coordinating window assignment, state buffering, and triggering.
///
/// Accumulates stream values into windowed state partitions and fires aggregated
/// results when event-time watermarks advance past window boundaries.

///|
/// Window entry tracking an active window partition and its observation count.
pub struct ActiveWindowState {
  key : String
  window : @window.TimeWindow
  mut count : Int64
  mut sum : Double
}

///|
/// Stateful window aggregation operator.
pub struct WindowOperator {
  name : String
  assigner : (@types.Timestamp) -> Array[@window.TimeWindow]
  active_windows : Array[ActiveWindowState]
  metrics : OperatorMetrics
}

///|
/// Creates a new WindowOperator with window assigner.
pub fn WindowOperator::new(
  name : String,
  assigner : (@types.Timestamp) -> Array[@window.TimeWindow],
) -> WindowOperator {
  {
    name,
    assigner,
    active_windows: [],
    metrics: OperatorMetrics::new(),
  }
}

///|
/// Finds or creates an active window state entry.
fn WindowOperator::get_or_create_window(
  self : WindowOperator,
  key : String,
  window : @window.TimeWindow,
) -> ActiveWindowState {
  for i = 0; i < self.active_windows.length(); i = i + 1 {
    let w = self.active_windows[i]
    if w.key == key && w.window == window {
      return w
    }
  }

  let new_entry : ActiveWindowState = { key, window, count: 0L, sum: 0.0 }
  self.active_windows.push(new_entry)
  new_entry
}

///|
/// Processes a record through the window operator.
pub fn WindowOperator::process_element(
  self : WindowOperator,
  record : @types.StreamRecord,
  out : Collector,
) -> Unit {
  self.metrics.record_in()

  if record.is_data() {
    let windows = (self.assigner)(record.get_timestamp())
    let val_dbl = match record.get_payload() {
      @types.EventPayload::DoublePayload(d) => d
      @types.EventPayload::IntPayload(i) => i.to_double()
      _ => 1.0
    }

    for i = 0; i < windows.length(); i = i + 1 {
      let win = windows[i]
      let state = self.get_or_create_window(record.get_key(), win)
      state.count = state.count + 1L
      state.sum = state.sum + val_dbl
    }
  } else if record.is_watermark() {
    let wm = @types.Watermark::from_timestamp(record.get_timestamp())
    self.metrics.update_watermark(wm)

    // Evaluate triggers for completed windows
    let remaining : Array[ActiveWindowState] = []
    for i = 0; i < self.active_windows.length(); i = i + 1 {
      let w = self.active_windows[i]
      if wm.get_timestamp() >= w.window.get_end() {
        // Window is complete: emit aggregated summary record
        let summary_entries = [
          ("count", @types.EventPayload::int(w.count)),
          ("sum", @types.EventPayload::double(w.sum)),
          ("window_start", @types.EventPayload::int(w.window.get_start().to_millis())),
          ("window_end", @types.EventPayload::int(w.window.get_end().to_millis())),
        ]
        let out_payload = @types.EventPayload::map(summary_entries)
        let out_record = @types.StreamRecord::data(
          w.key,
          w.window.get_end(),
          out_payload,
        )
        self.metrics.record_out()
        out.collect(out_record)
      } else {
        remaining.push(w)
      }
    }

    self.active_windows.clear()
    for i = 0; i < remaining.length(); i = i + 1 {
      self.active_windows.push(remaining[i])
    }

    // Forward watermark downstream
    self.metrics.record_out()
    out.collect(record)
  } else {
    // Forward other control signals (barriers, heartbeats)
    self.metrics.record_out()
    out.collect(record)
  }
}

///|
/// Returns count of active windows currently in memory.
pub fn WindowOperator::active_window_count(self : WindowOperator) -> Int {
  self.active_windows.length()
}
