///|
/// Comprehensive tests for DAG topology, cycle detection, topological sort, and pipeline scheduling.

///|
test "topology node management and cycle detection" {
  let topo = ExecutionTopology::new()
  let n0 = topo.add_node("source")
  let n1 = topo.add_node("filter")
  let n2 = topo.add_node("sink")

  assert_eq(topo.node_count(), 3)

  // Linear connections: 0 -> 1 -> 2
  assert_true(topo.connect(n0, n1, 16))
  assert_true(topo.connect(n1, n2, 16))
  assert_eq(topo.edge_count(), 2)
  assert_false(topo.has_cycle())

  // Attempting to add back-edge 2 -> 0 MUST be rejected!
  assert_false(topo.connect(n2, n0, 16))
  assert_eq(topo.edge_count(), 2) // Edge was rolled back
  assert_false(topo.has_cycle())
}

///|
test "topological sort ordering" {
  let topo = ExecutionTopology::new()
  let src = topo.add_node("source")
  let branch_a = topo.add_node("branch_a")
  let branch_b = topo.add_node("branch_b")
  let join = topo.add_node("join")

  let _ = topo.connect(src, branch_a, 16)
  let _ = topo.connect(src, branch_b, 16)
  let _ = topo.connect(branch_a, join, 16)
  let _ = topo.connect(branch_b, join, 16)

  let order_opt = topo.topological_sort()
  assert_true(order_opt is Some(_))

  let order = match order_opt {
    Some(o) => o
    None => []
  }
  assert_eq(order.length(), 4)
  assert_eq(order[0], src)
  assert_eq(order[3], join)
}

///|
test "pipeline scheduler multi-stage stream execution" {
  let topo = ExecutionTopology::new()
  let n_in = topo.add_node("input_feeder")
  let n_map = topo.add_node("multiplier")
  let n_sink = topo.add_node("sink")

  let _ = topo.connect(n_in, n_map, 16)
  let _ = topo.connect(n_map, n_sink, 16)

  let scheduler = PipelineScheduler::new(topo)

  // Register multiplier map handler (x10)
  let map_handler = NodeHandler::new(fn(rec, col) {
    if rec.is_data() {
      let val = match rec.get_payload() {
        @types.EventPayload::IntPayload(i) => i * 10L
        _ => 0L
      }
      col.collect(@types.StreamRecord::data(rec.get_key(), rec.get_timestamp(), @types.EventPayload::int(val)))
    }
  })
  scheduler.register_handler(n_map, map_handler)

  // Register sink handler capturing outputs
  let captured : Array[@types.StreamRecord] = []
  let sink_handler = NodeHandler::new(fn(rec, _col) {
    captured.push(rec)
  })
  scheduler.register_handler(n_sink, sink_handler)

  // Inject records into edge targeting multiplier
  let ts = @types.Timestamp::from_seconds(100L)
  let ok1 = scheduler.inject_record(n_map, @types.StreamRecord::data("m1", ts, @types.EventPayload::int(5L)))
  let ok2 = scheduler.inject_record(n_map, @types.StreamRecord::data("m2", ts, @types.EventPayload::int(8L)))
  assert_true(ok1)
  assert_true(ok2)

  // Run pipeline until idle
  let steps = scheduler.run_until_idle(10)
  assert_true(steps >= 1)

  // Verify captured transformed records at the sink!
  assert_eq(captured.length(), 2)
  assert_eq(captured[0].get_key(), "m1")
  assert_true(captured[0].get_payload() == @types.EventPayload::int(50L))
  assert_eq(captured[1].get_key(), "m2")
  assert_true(captured[1].get_payload() == @types.EventPayload::int(80L))
}
