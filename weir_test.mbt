///|
/// Unit tests for top-level runtime metadata.

///|
test "engine_banner returns expected string" {
  assert_eq(@weir.engine_banner(), "weir v0.1.0 [stateful stream processing runtime]")
}
