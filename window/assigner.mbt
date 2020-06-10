///|
/// Window assigners for streaming event-time partitioning.
///
/// Implements tumbling, sliding (hopping), and session window allocation strategies.

///|
/// Assigns events to fixed-size, non-overlapping tumbling windows.
pub struct TumblingEventTimeWindows {
  size : @types.Duration
  offset : @types.Duration
}

///|
/// Creates a tumbling window assigner with specified duration size.
pub fn TumblingEventTimeWindows::of(
  size : @types.Duration,
) -> TumblingEventTimeWindows {
  { size, offset: @types.Duration::zero() }
}

///|
/// Creates a tumbling window assigner with specified duration size and phase offset.
pub fn TumblingEventTimeWindows::of_with_offset(
  size : @types.Duration,
  offset : @types.Duration,
) -> TumblingEventTimeWindows {
  { size, offset }
}

///|
/// Assigns a timestamp to its corresponding tumbling TimeWindow.
pub fn TumblingEventTimeWindows::assign_windows(
  self : TumblingEventTimeWindows,
  timestamp : @types.Timestamp,
) -> Array[TimeWindow] {
  let ts = timestamp.to_micros()
  let off = self.offset.to_micros()
  let sz = self.size.to_micros()

  let mut rem = (ts - off) % sz
  if rem < 0L {
    rem = rem + sz
  }
  let start_micros = ts - rem
  let end_micros = start_micros + sz

  [
    TimeWindow::new(
      @types.Timestamp::from_micros(start_micros),
      @types.Timestamp::from_micros(end_micros),
    ),
  ]
}

///|
/// Assigns events to fixed-size, overlapping sliding (hopping) windows.
pub struct SlidingEventTimeWindows {
  size : @types.Duration
  slide : @types.Duration
  offset : @types.Duration
}

///|
/// Creates a sliding window assigner with size and slide intervals.
pub fn SlidingEventTimeWindows::of(
  size : @types.Duration,
  slide : @types.Duration,
) -> SlidingEventTimeWindows {
  { size, slide, offset: @types.Duration::zero() }
}

///|
/// Assigns a timestamp to all overlapping sliding windows.
pub fn SlidingEventTimeWindows::assign_windows(
  self : SlidingEventTimeWindows,
  timestamp : @types.Timestamp,
) -> Array[TimeWindow] {
  let ts = timestamp.to_micros()
  let off = self.offset.to_micros()
  let slide_sz = self.slide.to_micros()
  let win_sz = self.size.to_micros()

  let mut rem = (ts - off) % slide_sz
  if rem < 0L {
    rem = rem + slide_sz
  }
  let last_start = ts - rem

  let windows : Array[TimeWindow] = []
  let mut current_start = last_start
  while current_start > ts - win_sz {
    let current_end = current_start + win_sz
    windows.push(
      TimeWindow::new(
        @types.Timestamp::from_micros(current_start),
        @types.Timestamp::from_micros(current_end),
      ),
    )
    current_start = current_start - slide_sz
  }

  // Reverse so windows are returned in chronological start order
  let result : Array[TimeWindow] = []
  for i = windows.length() - 1; i >= 0; i = i - 1 {
    result.push(windows[i])
  }
  result
}

///|
/// Assigns events to dynamic session windows separated by inactivity gaps.
pub struct SessionEventTimeWindows {
  inactivity_gap : @types.Duration
}

///|
/// Creates a session window assigner with maximum inactivity gap.
pub fn SessionEventTimeWindows::with_gap(
  inactivity_gap : @types.Duration,
) -> SessionEventTimeWindows {
  { inactivity_gap, }
}

///|
/// Assigns an event to an initial session window spanning [ts, ts + inactivity_gap).
pub fn SessionEventTimeWindows::assign_windows(
  self : SessionEventTimeWindows,
  timestamp : @types.Timestamp,
) -> Array[TimeWindow] {
  [TimeWindow::new(timestamp, timestamp.add_duration(self.inactivity_gap))]
}
