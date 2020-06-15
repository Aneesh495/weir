///|
/// Window aggregation state accumulators for real-time statistical summaries.
///
/// Implements running count, running sum, and multi-variable statistics accumulators
/// that support incremental fold updates and cross-window state merging.

///|
/// Running count accumulator for windowed stream counting.
pub struct CountAccumulator {
  mut count : Int64
}

///|
/// Creates a clean count accumulator.
pub fn CountAccumulator::new() -> CountAccumulator {
  { count: 0L }
}

///|
/// Increments the count by 1.
pub fn CountAccumulator::add(self : CountAccumulator) -> Unit {
  self.count = self.count + 1L
}

///|
/// Returns the accumulated count.
pub fn CountAccumulator::get(self : CountAccumulator) -> Int64 {
  self.count
}

///|
/// Merges another count accumulator into this one.
pub fn CountAccumulator::merge(
  self : CountAccumulator,
  other : CountAccumulator,
) -> Unit {
  self.count = self.count + other.count
}

///|
/// Running sum accumulator for floating-point values.
pub struct SumDoubleAccumulator {
  mut sum : Double
}

///|
/// Creates a clean sum accumulator.
pub fn SumDoubleAccumulator::new() -> SumDoubleAccumulator {
  { sum: 0.0 }
}

///|
/// Adds a value to the running sum.
pub fn SumDoubleAccumulator::add(self : SumDoubleAccumulator, value : Double) -> Unit {
  self.sum = self.sum + value
}

///|
/// Returns the running sum.
pub fn SumDoubleAccumulator::get(self : SumDoubleAccumulator) -> Double {
  self.sum
}

///|
/// Merges another sum accumulator into this one.
pub fn SumDoubleAccumulator::merge(
  self : SumDoubleAccumulator,
  other : SumDoubleAccumulator,
) -> Unit {
  self.sum = self.sum + other.sum
}

///|
/// Comprehensive statistical summary accumulator (count, sum, min, max, mean).
pub struct StatsAccumulator {
  mut count : Int64
  mut sum : Double
  mut min : Double
  mut max : Double
}

///|
/// Creates a clean statistics accumulator.
pub fn StatsAccumulator::new() -> StatsAccumulator {
  { count: 0L, sum: 0.0, min: 1.0e300, max: -1.0e300 }
}

///|
/// Ingests a new numerical observation into the statistics accumulator.
pub fn StatsAccumulator::add(self : StatsAccumulator, value : Double) -> Unit {
  self.count = self.count + 1L
  self.sum = self.sum + value
  if value < self.min {
    self.min = value
  }
  if value > self.max {
    self.max = value
  }
}

///|
/// Returns sample count.
pub fn StatsAccumulator::get_count(self : StatsAccumulator) -> Int64 {
  self.count
}

///|
/// Returns sum.
pub fn StatsAccumulator::get_sum(self : StatsAccumulator) -> Double {
  self.sum
}

///|
/// Returns sample mean, or 0.0 if empty.
pub fn StatsAccumulator::get_mean(self : StatsAccumulator) -> Double {
  if self.count == 0L {
    0.0
  } else {
    self.sum / self.count.to_double()
  }
}

///|
/// Returns minimum observed value, or 0.0 if empty.
pub fn StatsAccumulator::get_min(self : StatsAccumulator) -> Double {
  if self.count == 0L {
    0.0
  } else {
    self.min
  }
}

///|
/// Returns maximum observed value, or 0.0 if empty.
pub fn StatsAccumulator::get_max(self : StatsAccumulator) -> Double {
  if self.count == 0L {
    0.0
  } else {
    self.max
  }
}

///|
/// Merges another statistics accumulator into this one.
pub fn StatsAccumulator::merge(
  self : StatsAccumulator,
  other : StatsAccumulator,
) -> Unit {
  if other.count > 0L {
    self.count = self.count + other.count
    self.sum = self.sum + other.sum
    if other.min < self.min {
      self.min = other.min
    }
    if other.max > self.max {
      self.max = other.max
    }
  }
}
