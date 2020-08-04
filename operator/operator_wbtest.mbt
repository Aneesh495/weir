///|
/// Comprehensive tests for streaming operators: Map, FlatMap, Filter, KeyBy, WindowOperator, and TemporalJoin.

///|
test "map operator transforms records and forwards control" {
  let map_op = MapOperator::new("double_metric", fn(rec) {
    let new_val = match rec.get_payload() {
      @types.EventPayload::DoublePayload(d) => @types.EventPayload::double(d * 2.0)
      _ => rec.get_payload()
    }
    @types.StreamRecord::data(rec.get_key(), rec.get_timestamp(), new_val)
  })

  let col = Collector::new()
  let ts = @types.Timestamp::from_seconds(10L)
  let r_in = @types.StreamRecord::data("temp", ts, @types.EventPayload::double(25.0))
  map_op.process_element(r_in, col)

  assert_eq(col.length(), 1)
  let r_out = col.drain()[0]
  assert_true(r_out.get_payload() == @types.EventPayload::double(50.0))
  assert_eq(map_op.metrics.in_records, 1L)
  assert_eq(map_op.metrics.out_records, 1L)
}

///|
test "flatmap operator expands single record into multiple" {
  let flat_op = FlatMapOperator::new("word_expand", fn(rec) {
    let ts = rec.get_timestamp()
    [
      @types.StreamRecord::data("word", ts, @types.EventPayload::string("token_1")),
      @types.StreamRecord::data("word", ts, @types.EventPayload::string("token_2")),
    ]
  })

  let col = Collector::new()
  let ts = @types.Timestamp::from_seconds(10L)
  let r_in = @types.StreamRecord::data("doc", ts, @types.EventPayload::null())
  flat_op.process_element(r_in, col)

  assert_eq(col.length(), 2)
  let drained = col.drain()
  assert_true(drained[0].get_payload() == @types.EventPayload::string("token_1"))
  assert_true(drained[1].get_payload() == @types.EventPayload::string("token_2"))
}

///|
test "filter operator discards non-matching events" {
  let filter_op = FilterOperator::new("high_temp", fn(rec) {
    match rec.get_payload() {
      @types.EventPayload::DoublePayload(d) => d > 30.0
      _ => false
    }
  })

  let col = Collector::new()
  let ts = @types.Timestamp::from_seconds(10L)
  let r_cold = @types.StreamRecord::data("temp", ts, @types.EventPayload::double(20.0))
  let r_hot = @types.StreamRecord::data("temp", ts, @types.EventPayload::double(35.0))

  filter_op.process_element(r_cold, col)
  assert_eq(col.length(), 0)
  assert_eq(filter_op.metrics.dropped_records, 1L)

  filter_op.process_element(r_hot, col)
  assert_eq(col.length(), 1)
  assert_eq(filter_op.metrics.out_records, 1L)
}

///|
test "key_by operator rekeys stream records" {
  let keyby = KeyByOperator::new("region_key", fn(rec) {
    match rec.get_payload().get_field("region") {
      Some(@types.EventPayload::StringPayload(r)) => r
      _ => "unknown"
    }
  })

  let col = Collector::new()
  let ts = @types.Timestamp::from_seconds(10L)
  let payload = @types.EventPayload::map([("region", @types.EventPayload::string("us-east"))])
  let r_in = @types.StreamRecord::data("raw_event", ts, payload)

  keyby.process_element(r_in, col)
  assert_eq(col.length(), 1)
  let r_out = col.drain()[0]
  assert_eq(r_out.get_key(), "us-east")
}

///|
test "window operator accumulates and triggers on watermark" {
  let sz = @types.Duration::from_seconds(10L)
  let assigner = @window.TumblingEventTimeWindows::of(sz)
  let win_op = WindowOperator::new("tumble_10s", fn(ts) {
    assigner.assign_windows(ts)
  })

  let col = Collector::new()

  // Ingest events in [0..10s) window
  let r1 = @types.StreamRecord::data("cpu", @types.Timestamp::from_seconds(2L), @types.EventPayload::double(10.0))
  let r2 = @types.StreamRecord::data("cpu", @types.Timestamp::from_seconds(6L), @types.EventPayload::double(20.0))
  win_op.process_element(r1, col)
  win_op.process_element(r2, col)
  assert_eq(col.length(), 0) // Window has not fired yet!
  assert_eq(win_op.active_window_count(), 1)

  // Watermark at 5s does not trigger window ending at 10s
  let wm_5 = @types.StreamRecord::watermark(@types.Timestamp::from_seconds(5L))
  win_op.process_element(wm_5, col)
  assert_eq(col.length(), 1) // Only watermark forwarded
  let _ = col.drain()

  // Watermark at 10s FIRES the [0..10s) window!
  let wm_10 = @types.StreamRecord::watermark(@types.Timestamp::from_seconds(10L))
  win_op.process_element(wm_10, col)
  assert_eq(col.length(), 2) // 1 window result record + 1 watermark record!

  let outputs = col.drain()
  let win_res = outputs[0]
  assert_eq(win_res.get_key(), "cpu")
  assert_true(win_res.get_payload().get_field("count") == Some(@types.EventPayload::int(2L)))
  assert_true(win_res.get_payload().get_field("sum") == Some(@types.EventPayload::double(30.0)))
  assert_eq(win_op.active_window_count(), 0) // State purged!
}

///|
test "temporal join operator matches left and right stream events within window" {
  let lower = @types.Duration::from_seconds(2L)
  let upper = @types.Duration::from_seconds(2L)
  let join_op = TemporalJoinOperator::new("order_payment_join", lower, upper)
  let col = Collector::new()

  // Order arrives at t=10s on Left
  let r_order = @types.StreamRecord::data(
    "order_100",
    @types.Timestamp::from_seconds(10L),
    @types.EventPayload::string("order_created"),
  )
  join_op.process_left(r_order, col)
  assert_eq(col.length(), 0) // No match on Right yet

  // Payment arrives at t=11s on Right (within [8s..12s])
  let r_pay = @types.StreamRecord::data(
    "order_100",
    @types.Timestamp::from_seconds(11L),
    @types.EventPayload::string("payment_cleared"),
  )
  join_op.process_right(r_pay, col)
  assert_eq(col.length(), 1) // Match found!

  let joined = col.drain()[0]
  assert_eq(joined.get_key(), "order_100")
  assert_true(joined.get_payload().get_field("left") == Some(@types.EventPayload::string("order_created")))
  assert_true(joined.get_payload().get_field("right") == Some(@types.EventPayload::string("payment_cleared")))
}
