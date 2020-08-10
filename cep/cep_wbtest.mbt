///|
/// Comprehensive tests for Complex Event Processing (CEP) pattern DSL, NFA, and runtime engine.

///|
test "cep pattern dsl and nfa compilation" {
  let pattern = PatternSequence::begin("login_fail", fn(rec) {
    rec.get_key() == "login_fail"
  }).followed_by("login_success", fn(rec) {
    rec.get_key() == "login_success"
  }).within(@types.Duration::from_seconds(10L))

  assert_eq(pattern.stage_count(), 2)
  assert_true(pattern.get_window_time() == Some(@types.Duration::from_seconds(10L)))

  let nfa = NFACompiler::compile(pattern)
  assert_eq(nfa.states.length(), 3) // States 0, 1, and 2 ($final)
  assert_false(nfa.states[0].is_final)
  assert_false(nfa.states[1].is_final)
  assert_true(nfa.states[2].is_final)
}

///|
test "cep engine simple sequential match" {
  let pattern = PatternSequence::begin("order_placed", fn(rec) {
    rec.get_key() == "order_placed"
  }).followed_by("order_cancelled", fn(rec) {
    rec.get_key() == "order_cancelled"
  })

  let nfa = NFACompiler::compile(pattern)
  let engine = CEPEngine::new(nfa)

  let t1 = @types.Timestamp::from_seconds(10L)
  let t2 = @types.Timestamp::from_seconds(12L)

  let e1 = @types.StreamRecord::data("order_placed", t1, @types.EventPayload::string("user_1"))
  let m1 = engine.process_event(e1)
  assert_eq(m1.length(), 0)
  assert_eq(engine.active_match_count(), 1)

  let e2 = @types.StreamRecord::data("order_cancelled", t2, @types.EventPayload::string("user_1"))
  let m2 = engine.process_event(e2)
  assert_eq(m2.length(), 1)
  let matched_events = m2[0]
  assert_eq(matched_events.length(), 2)
  assert_true(matched_events[0] == e1)
  assert_true(matched_events[1] == e2)
}

///|
test "cep engine relaxed contiguity with intervening noise" {
  let pattern = PatternSequence::begin("stage_a", fn(rec) {
    rec.get_key() == "A"
  }).followed_by("stage_b", fn(rec) {
    rec.get_key() == "B"
  })

  let nfa = NFACompiler::compile(pattern)
  let engine = CEPEngine::new(nfa)

  let t0 = @types.Timestamp::from_seconds(100L)
  let ea = @types.StreamRecord::data("A", t0, @types.EventPayload::null())
  let ex1 = @types.StreamRecord::data("NOISE_1", t0.add_duration(@types.Duration::from_seconds(1L)), @types.EventPayload::null())
  let ex2 = @types.StreamRecord::data("NOISE_2", t0.add_duration(@types.Duration::from_seconds(2L)), @types.EventPayload::null())
  let eb = @types.StreamRecord::data("B", t0.add_duration(@types.Duration::from_seconds(3L)), @types.EventPayload::null())

  assert_eq(engine.process_event(ea).length(), 0)
  assert_eq(engine.process_event(ex1).length(), 0) // Noise ignored
  assert_eq(engine.process_event(ex2).length(), 0) // Noise ignored

  let matches = engine.process_event(eb)
  assert_eq(matches.length(), 1)
  assert_eq(matches[0].length(), 2)
  assert_true(matches[0][0] == ea)
  assert_true(matches[0][1] == eb)
}

///|
test "cep engine window time expiration drops tardy matches" {
  let pattern = PatternSequence::begin("step_1", fn(rec) {
    rec.get_key() == "1"
  }).followed_by("step_2", fn(rec) {
    rec.get_key() == "2"
  }).within(@types.Duration::from_seconds(5L))

  let nfa = NFACompiler::compile(pattern)
  let engine = CEPEngine::new(nfa)

  let t1 = @types.Timestamp::from_seconds(10L)
  let e1 = @types.StreamRecord::data("1", t1, @types.EventPayload::null())
  assert_eq(engine.process_event(e1).length(), 0)

  // Step 2 arrives at t=20s (10s later, which exceeds within(5s) window)
  let t2_late = @types.Timestamp::from_seconds(20L)
  let e2 = @types.StreamRecord::data("2", t2_late, @types.EventPayload::null())
  let matches = engine.process_event(e2)
  assert_eq(matches.length(), 0) // Window expired, no match emitted!
}
