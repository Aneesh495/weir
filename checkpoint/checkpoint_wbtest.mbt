///|
/// Comprehensive tests for Chandy-Lamport barrier alignment, coordinator, and state recovery.

///|
test "chandy-lamport two-channel barrier alignment and event buffering" {
  let tracker = BarrierAlignmentTracker::new(2, 16)
  assert_false(tracker.is_aligning())

  let ts = @types.Timestamp::from_seconds(100L)
  let e_pre0 = @types.StreamRecord::data("k0", ts, @types.EventPayload::string("pre_c0"))
  let e_pre1 = @types.StreamRecord::data("k1", ts, @types.EventPayload::string("pre_c1"))

  // 1. Pre-barrier records on both channels: ProcessNow
  match tracker.on_record(0, e_pre0) {
    ProcessNow(r) => assert_true(r == e_pre0)
    _ => assert_true(false)
  }
  match tracker.on_record(1, e_pre1) {
    ProcessNow(r) => assert_true(r == e_pre1)
    _ => assert_true(false)
  }

  // 2. Barrier 42 arrives on Channel 0
  let b42 = @types.StreamRecord::barrier(42L, ts)
  match tracker.on_record(0, b42) {
    BufferChannel(_) => ()
    _ => assert_true(false)
  }
  assert_true(tracker.is_aligning())
  assert_eq(tracker.get_aligned_channels_count(), 1)

  // 3. Post-barrier record arrives on Channel 0: MUST be buffered!
  let e_post0 = @types.StreamRecord::data("k0", ts, @types.EventPayload::string("post_c0"))
  match tracker.on_record(0, e_post0) {
    BufferChannel(r) => assert_true(r == e_post0)
    _ => assert_true(false)
  }

  // 4. Pre-barrier record arrives on Channel 1: MUST be processed immediately!
  let e_pre1_more = @types.StreamRecord::data("k1", ts, @types.EventPayload::string("pre_c1_more"))
  match tracker.on_record(1, e_pre1_more) {
    ProcessNow(r) => assert_true(r == e_pre1_more)
    _ => assert_true(false)
  }

  // 5. Barrier 42 arrives on Channel 1: Alignment COMPLETE!
  match tracker.on_record(1, b42) {
    AlignmentComplete(cp_id, buffered) => {
      assert_eq(cp_id, 42L)
      assert_eq(buffered.length(), 1)
      assert_true(buffered[0] == e_post0)
    }
    _ => assert_true(false)
  }

  assert_false(tracker.is_aligning())
  assert_eq(tracker.get_aligned_channels_count(), 0)
}

///|
test "checkpoint coordinator multi-operator two-phase commit" {
  let coordinator = CheckpointCoordinator::new()
  let t_start = @types.Timestamp::from_seconds(1000L)
  let t_ack = @types.Timestamp::from_seconds(1002L)

  let req_ops = ["op_source", "op_window"]
  let cp_id = coordinator.trigger_checkpoint(req_ops, t_start)
  assert_eq(cp_id, 1L)

  // First operator acks
  let dummy_entry = @state.MemTableEntry::new(
    "key1",
    Some(@types.EventPayload::int(42L)),
    1L,
  )
  let op1_done = coordinator.acknowledge(1L, "op_source", [dummy_entry], t_ack)
  assert_false(op1_done) // Still waiting for op_window!

  // Second operator acks -> checkpoint completes!
  let op2_done = coordinator.acknowledge(1L, "op_window", [dummy_entry], t_ack)
  assert_true(op2_done)

  assert_eq(coordinator.get_latest_completed_id(), Some(1L))
  let manifest = coordinator.get_snapshot_manifest(1L)
  assert_true(manifest is Some(_))
  let op_snaps = match manifest {
    Some(s) => s
    None => []
  }
  assert_eq(op_snaps.length(), 2)
}

///|
test "recovery plan restores state backend accurately" {
  let snap_entries = [
    @state.MemTableEntry::new("acc:101", Some(@types.EventPayload::double(500.0)), 1L),
    @state.MemTableEntry::new("acc:102", Some(@types.EventPayload::double(750.0)), 2L),
  ]

  let plan = RecoveryPlan::new(1L, [("op_bank", snap_entries)])
  assert_eq(plan.operator_count(), 1)

  let backend = @state.StateBackend::new()
  let ok = plan.restore_operator("op_bank", backend)
  assert_true(ok)

  assert_true(backend.get("acc:101") == Some(@types.EventPayload::double(500.0)))
  assert_true(backend.get("acc:102") == Some(@types.EventPayload::double(750.0)))
  assert_true(backend.get("acc:999") is None)
}
