///|
/// Comprehensive whitebox unit tests for TimeWindow, Assigner, Merger, Trigger, and Accumulator.

///|
test "time window model and boundaries" {
  let t1 = @types.Timestamp::from_seconds(10L)
  let t2 = @types.Timestamp::from_seconds(20L)
  let w = TimeWindow::new(t1, t2)

  assert_true(w.get_start() == t1)
  assert_true(w.get_end() == t2)
  assert_eq(w.duration().to_seconds(), 10L)

  assert_true(w.covers(@types.Timestamp::from_seconds(10L)))
  assert_true(w.covers(@types.Timestamp::from_seconds(15L)))
  assert_false(w.covers(@types.Timestamp::from_seconds(20L)))
  assert_false(w.covers(@types.Timestamp::from_seconds(9L)))

  let w2 = TimeWindow::new(
    @types.Timestamp::from_seconds(15L),
    @types.Timestamp::from_seconds(25L),
  )
  assert_true(w.intersects(w2))

  let merged = w.merge(w2)
  assert_true(merged.get_start() == t1)
  assert_true(merged.get_end() == @types.Timestamp::from_seconds(25L))
}

///|
test "tumbling window assigner" {
  let sz = @types.Duration::from_seconds(10L)
  let assigner = TumblingEventTimeWindows::of(sz)

  // ts = 14s falls in [10s..20s)
  let wins1 = assigner.assign_windows(@types.Timestamp::from_seconds(14L))
  assert_eq(wins1.length(), 1)
  assert_true(wins1[0].get_start() == @types.Timestamp::from_seconds(10L))
  assert_true(wins1[0].get_end() == @types.Timestamp::from_seconds(20L))

  // boundary ts = 20s falls in [20s..30s)
  let wins2 = assigner.assign_windows(@types.Timestamp::from_seconds(20L))
  assert_eq(wins2.length(), 1)
  assert_true(wins2[0].get_start() == @types.Timestamp::from_seconds(20L))
  assert_true(wins2[0].get_end() == @types.Timestamp::from_seconds(30L))
}

///|
test "sliding window assigner" {
  let sz = @types.Duration::from_seconds(10L)
  let slide = @types.Duration::from_seconds(5L)
  let assigner = SlidingEventTimeWindows::of(sz, slide)

  // ts = 12s should fall in [5s..15s) and [10s..20s)
  let wins = assigner.assign_windows(@types.Timestamp::from_seconds(12L))
  assert_eq(wins.length(), 2)
  assert_true(wins[0].get_start() == @types.Timestamp::from_seconds(5L))
  assert_true(wins[0].get_end() == @types.Timestamp::from_seconds(15L))
  assert_true(wins[1].get_start() == @types.Timestamp::from_seconds(10L))
  assert_true(wins[1].get_end() == @types.Timestamp::from_seconds(20L))
}

///|
test "session window assigner and interval merging" {
  let gap = @types.Duration::from_seconds(5L)
  let assigner = SessionEventTimeWindows::with_gap(gap)

  // e1 at 10s -> [10s..15s)
  let w1 = assigner.assign_windows(@types.Timestamp::from_seconds(10L))[0]
  assert_true(w1.get_start() == @types.Timestamp::from_seconds(10L))
  assert_true(w1.get_end() == @types.Timestamp::from_seconds(15L))

  // e2 at 13s -> [13s..18s). Merging with w1 should yield [10s..18s)!
  let w2 = assigner.assign_windows(@types.Timestamp::from_seconds(13L))[0]
  let m_res = SessionWindowMerger::merge([w1], w2)
  assert_true(m_res.merged_window.get_start() == @types.Timestamp::from_seconds(10L))
  assert_true(m_res.merged_window.get_end() == @types.Timestamp::from_seconds(18L))
  assert_eq(m_res.retired_windows.length(), 1)
  assert_true(m_res.retired_windows[0] == w1)

  // Update window list replaces w1 with merged [10s..18s)
  let updated = SessionWindowMerger::update_window_list([w1], m_res)
  assert_eq(updated.length(), 1)
  assert_true(updated[0] == m_res.merged_window)
}

///|
test "window triggers: event time, count, delta" {
  let w = TimeWindow::new(
    @types.Timestamp::from_seconds(0L),
    @types.Timestamp::from_seconds(10L),
  )

  // Watermark at 9s does not trigger
  let wm_early = @types.Watermark::from_seconds(9L)
  assert_true(EventTimeTrigger::on_watermark(w, wm_early) == TriggerResult::Continue)

  // Watermark at 10s triggers FireAndPurge
  let wm_fire = @types.Watermark::from_seconds(10L)
  assert_true(EventTimeTrigger::on_watermark(w, wm_fire) == TriggerResult::FireAndPurge)

  // Count trigger fires when count >= 3
  let count_trig = CountTrigger::of(3)
  assert_true(count_trig.on_element(1) == TriggerResult::Continue)
  assert_true(count_trig.on_element(2) == TriggerResult::Continue)
  assert_true(count_trig.on_element(3) == TriggerResult::FireAndPurge)

  // Delta trigger fires on delta >= 5.0
  let delta_trig = DeltaTrigger::of(5.0)
  assert_true(delta_trig.on_element(10.0) == TriggerResult::Continue)
  assert_true(delta_trig.on_element(12.0) == TriggerResult::Continue)
  assert_true(delta_trig.on_element(18.0) == TriggerResult::Fire)
}

///|
test "window accumulators: count, sum, stats" {
  let cnt = CountAccumulator::new()
  cnt.add()
  cnt.add()
  assert_eq(cnt.get(), 2L)

  let sum = SumDoubleAccumulator::new()
  sum.add(10.5)
  sum.add(20.5)
  assert_eq(sum.get(), 31.0)

  let stats = StatsAccumulator::new()
  stats.add(10.0)
  stats.add(20.0)
  stats.add(30.0)
  assert_eq(stats.get_count(), 3L)
  assert_eq(stats.get_sum(), 60.0)
  assert_eq(stats.get_mean(), 20.0)
  assert_eq(stats.get_min(), 10.0)
  assert_eq(stats.get_max(), 30.0)

  let stats2 = StatsAccumulator::new()
  stats2.add(5.0)
  stats2.add(35.0)
  stats.merge(stats2)
  assert_eq(stats.get_count(), 5L)
  assert_eq(stats.get_sum(), 100.0)
  assert_eq(stats.get_mean(), 20.0)
  assert_eq(stats.get_min(), 5.0)
  assert_eq(stats.get_max(), 35.0)
}
