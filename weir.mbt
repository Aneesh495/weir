///|
/// Weir: High-throughput stateful stream processing runtime in MoonBit.
///
/// This package exposes core versioning, metadata, and top-level pipeline
/// builder interfaces for the Weir streaming engine.

///|
/// Version constant for the Weir streaming runtime.
pub let version : String = "0.1.0"

///|
/// Returns the engine banner string.
pub fn engine_banner() -> String {
  "weir v" + version + " [stateful stream processing runtime]"
}
