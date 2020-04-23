///|
/// Unit tests for Timestamp and Duration primitives.

///|
test "timestamp conversion roundtrips" {
  let ts = Timestamp::from_millis(1700000000123L)
  assert_eq(ts.to_millis(), 1700000000123L)
  assert_eq(ts.to_micros(), 1700000000123000L)
  assert_eq(ts.to_seconds(), 1700000000L)
}

///|
test "timestamp comparisons and ordering" {
  let t1 = Timestamp::from_seconds(100L)
  let t2 = Timestamp::from_seconds(200L)
  let t3 = Timestamp::from_seconds(100L)

  assert_true(t1 < t2)
  assert_true(t2 > t1)
  assert_true(t1 <= t3)
  assert_true(t1 >= t3)
  assert_true(t1 == t3)
  assert_false(t1 != t3)
  assert_true(t1.is_before(t2))
  assert_true(t2.is_after(t1))
}

///|
test "timestamp arithmetic with duration" {
  let t0 = Timestamp::from_seconds(1000L)
  let d1 = Duration::from_seconds(30L)
  let t1 = t0.add_duration(d1)
  assert_eq(t1.to_seconds(), 1030L)

  let t2 = t1.sub_duration(Duration::from_millis(5000L))
  assert_eq(t2.to_seconds(), 1025L)

  let diff = t1.diff(t0)
  assert_eq(diff.to_seconds(), 30L)
}

///|
test "duration formatting and conversions" {
  let d_sec = Duration::from_seconds(5L)
  assert_eq(d_sec.to_string(), "5s")

  let d_ms = Duration::from_millis(250L)
  assert_eq(d_ms.to_string(), "250ms")

  let d_us = Duration::from_micros(15L)
  assert_eq(d_us.to_string(), "15us")

  let d_min = Duration::from_minutes(2L)
  assert_eq(d_min.to_seconds(), 120L)

  let sum = d_sec.add(Duration::from_seconds(10L))
  assert_eq(sum.to_seconds(), 15L)
}
