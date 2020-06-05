///|
/// Comprehensive tests for watermark generators, alignment, and late data handling.

///|
test "monotonic watermark generator" {
  let gen = MonotonicWatermarkGenerator::new()
  let t1 = @types.Timestamp::from_seconds(10L)
  let t2 = @types.Timestamp::from_seconds(15L)
  let t_old = @types.Timestamp::from_seconds(8L)

  assert_true(gen.on_event(t1) == Some(@types.Watermark::from_timestamp(t1)))
  assert_true(gen.on_event(t_old) is None)
  assert_true(gen.on_event(t2) == Some(@types.Watermark::from_timestamp(t2)))
  assert_true(gen.get_watermark() == @types.Watermark::from_timestamp(t2))
}

///|
test "bounded out-of-orderness watermark generator" {
  let lateness = @types.Duration::from_seconds(5L)
  let gen = BoundedOutOfOrdernessGenerator::new(lateness)

  // t=10s arrives, watermark = 10s - 5s = 5s
  gen.on_event(@types.Timestamp::from_seconds(10L))
  let wm1 = gen.emit_watermark()
  assert_true(wm1 == Some(@types.Watermark::from_seconds(5L)))

  // out of order t=8s arrives (less than max 10s), watermark stays 5s
  gen.on_event(@types.Timestamp::from_seconds(8L))
  let wm2 = gen.emit_watermark()
  assert_true(wm2 is None)

  // t=20s arrives, watermark advances to 20s - 5s = 15s
  gen.on_event(@types.Timestamp::from_seconds(20L))
  let wm3 = gen.emit_watermark()
  assert_true(wm3 == Some(@types.Watermark::from_seconds(15L)))
  assert_true(gen.get_watermark() == @types.Watermark::from_seconds(15L))
}

///|
test "punctuated watermark generator" {
  let gen = PunctuatedWatermarkGenerator::new(fn(rec) {
    if rec.get_key() == "checkpoint_sync" {
      Some(rec.get_timestamp())
    } else {
      None
    }
  })

  let t1 = @types.Timestamp::from_seconds(100L)
  let r_data = @types.StreamRecord::data("sensor_data", t1, @types.EventPayload::null())
  assert_true(gen.on_event(r_data) is None)

  let r_sync = @types.StreamRecord::data("checkpoint_sync", t1, @types.EventPayload::null())
  assert_true(gen.on_event(r_sync) == Some(@types.Watermark::from_timestamp(t1)))
  assert_true(gen.get_watermark() == @types.Watermark::from_timestamp(t1))
}

///|
test "partition watermark alignment across 3 channels" {
  let tracker = PartitionWatermarkTracker::new(3)
  let min_ts = @types.Timestamp::min_value()
  assert_true(tracker.get_operator_watermark() == @types.Watermark::from_timestamp(min_ts))

  // Partition 0 advances to 10s. Operator stays at min_value because partitions 1 and 2 are at min_value.
  let adv0 = tracker.advance_partition(0, @types.Watermark::from_seconds(10L))
  assert_true(adv0 is None)

  // Partition 1 advances to 12s. Operator still held by partition 2.
  let adv1 = tracker.advance_partition(1, @types.Watermark::from_seconds(12L))
  assert_true(adv1 is None)

  // Partition 2 advances to 8s. Now min(10, 12, 8) = 8s! Operator advances to 8s!
  let adv2 = tracker.advance_partition(2, @types.Watermark::from_seconds(8L))
  assert_true(adv2 == Some(@types.Watermark::from_seconds(8L)))
  assert_true(tracker.get_operator_watermark() == @types.Watermark::from_seconds(8L))

  // Partition 2 marks idle. Active partitions are 0 (10s) and 1 (12s). Operator advances to 10s!
  let idle_adv = tracker.mark_idle(2)
  assert_true(idle_adv == Some(@types.Watermark::from_seconds(10L)))
  assert_true(tracker.get_operator_watermark() == @types.Watermark::from_seconds(10L))

  // Partition 2 resumes active with 15s. Min(10, 12, 15) = 10s (no regression).
  tracker.mark_active(2)
  let adv2_resumed = tracker.advance_partition(2, @types.Watermark::from_seconds(15L))
  assert_true(adv2_resumed is None)

  // Partition 0 advances to 14s. Min(14, 12, 15) = 12s! Operator advances to 12s!
  let adv0_next = tracker.advance_partition(0, @types.Watermark::from_seconds(14L))
  assert_true(adv0_next == Some(@types.Watermark::from_seconds(12L)))
  assert_true(tracker.get_operator_watermark() == @types.Watermark::from_seconds(12L))
}

///|
test "late data handler classification and dead letter routing" {
  let allowed = @types.Duration::from_seconds(10L)
  let handler = LateDataHandler::new(allowed, 8)
  let current_wm = @types.Watermark::from_seconds(100L)

  // 1. On-time event (t=105s >= 100s)
  let r_ontime = @types.StreamRecord::data(
    "k1",
    @types.Timestamp::from_seconds(105L),
    @types.EventPayload::null(),
  )
  assert_true(handler.evaluate(r_ontime, current_wm) == LateDecision::OnTime)
  assert_eq(handler.get_on_time_count(), 1L)

  // 2. Late event within allowance (t=95s, 95 + 10 = 105s >= 100s)
  let r_late_tolerated = @types.StreamRecord::data(
    "k2",
    @types.Timestamp::from_seconds(95L),
    @types.EventPayload::null(),
  )
  assert_true(handler.evaluate(r_late_tolerated, current_wm) == LateDecision::LateWithinAllowance)
  assert_eq(handler.get_late_tolerated_count(), 1L)

  // 3. Tardy event exceeding allowance (t=80s, 80 + 10 = 90s < 100s) -> dropped to side output
  let r_dropped = @types.StreamRecord::data(
    "k3",
    @types.Timestamp::from_seconds(80L),
    @types.EventPayload::null(),
  )
  assert_true(handler.evaluate(r_dropped, current_wm) == LateDecision::DroppedLateToSideOutput)
  assert_eq(handler.get_dropped_count(), 1L)

  // Verify side output channel contains the dropped record
  let side = handler.get_side_output()
  assert_eq(side.length(), 1)
  let popped = side.pop()
  assert_true(popped == Some(r_dropped))
}
