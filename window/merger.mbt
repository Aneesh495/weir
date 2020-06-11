///|
/// Dynamic interval merger for session window defragmentation.
///
/// Merges overlapping or contiguous session intervals into consolidated
/// windows and identifies merged-out redundant intervals for state collation.

///|
/// Result of merging an incoming session window with existing active sessions.
pub struct MergeResult {
  merged_window : TimeWindow
  retired_windows : Array[TimeWindow]
}

///|
/// Checks and consolidates overlapping session windows.
pub struct SessionWindowMerger {
  _unit : Unit
}

///|
/// Creates a SessionWindowMerger instance.
pub fn SessionWindowMerger::new() -> SessionWindowMerger {
  { _unit: () }
}

///|
/// Merges a new candidate window into a list of existing active windows.
/// Returns the new merged window and any existing windows that were absorbed.
pub fn SessionWindowMerger::merge(
  existing : Array[TimeWindow],
  candidate : TimeWindow,
) -> MergeResult {
  let mut current = candidate
  let retired : Array[TimeWindow] = []

  for i = 0; i < existing.length(); i = i + 1 {
    let win = existing[i]
    if current.intersects(win) || current.end == win.start || win.end == current.start {
      current = current.merge(win)
      retired.push(win)
    }
  }

  { merged_window: current, retired_windows: retired }
}

///|
/// Replaces absorbed windows with the merged window in an active window set.
pub fn SessionWindowMerger::update_window_list(
  existing : Array[TimeWindow],
  result : MergeResult,
) -> Array[TimeWindow] {
  let updated : Array[TimeWindow] = []
  for i = 0; i < existing.length(); i = i + 1 {
    let mut was_retired = false
    for j = 0; j < result.retired_windows.length(); j = j + 1 {
      if existing[i] == result.retired_windows[j] {
        was_retired = true
        break
      }
    }
    if !was_retired {
      updated.push(existing[i])
    }
  }
  updated.push(result.merged_window)
  updated
}
