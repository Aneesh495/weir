///|
/// Core streaming transformation operators: Map, FlatMap, Filter, and KeyBy.
///
/// Implements functional stream transformations with metric attribution and
/// control signal forwarding.

///|
/// Map operator applying a 1-to-1 transformation function to stream records.
pub struct MapOperator {
  name : String
  transform : (@types.StreamRecord) -> @types.StreamRecord
  metrics : OperatorMetrics
}

///|
/// Creates a new MapOperator.
pub fn MapOperator::new(
  name : String,
  transform : (@types.StreamRecord) -> @types.StreamRecord,
) -> MapOperator {
  { name, transform, metrics: OperatorMetrics::new() }
}

///|
/// Processes a record through the MapOperator.
pub fn MapOperator::process_element(
  self : MapOperator,
  record : @types.StreamRecord,
  out : Collector,
) -> Unit {
  self.metrics.record_in()
  if record.is_data() {
    let transformed = (self.transform)(record)
    self.metrics.record_out()
    out.collect(transformed)
  } else {
    // Forward control signals (watermarks, barriers) transparently
    self.metrics.record_out()
    out.collect(record)
  }
}

///|
/// FlatMap operator expanding 1 incoming record into 0 or more output records.
pub struct FlatMapOperator {
  name : String
  transform : (@types.StreamRecord) -> Array[@types.StreamRecord]
  metrics : OperatorMetrics
}

///|
/// Creates a new FlatMapOperator.
pub fn FlatMapOperator::new(
  name : String,
  transform : (@types.StreamRecord) -> Array[@types.StreamRecord],
) -> FlatMapOperator {
  { name, transform, metrics: OperatorMetrics::new() }
}

///|
/// Processes a record through the FlatMapOperator.
pub fn FlatMapOperator::process_element(
  self : FlatMapOperator,
  record : @types.StreamRecord,
  out : Collector,
) -> Unit {
  self.metrics.record_in()
  if record.is_data() {
    let expanded = (self.transform)(record)
    for i = 0; i < expanded.length(); i = i + 1 {
      self.metrics.record_out()
      out.collect(expanded[i])
    }
  } else {
    self.metrics.record_out()
    out.collect(record)
  }
}

///|
/// Filter operator retaining records that satisfy a boolean predicate.
pub struct FilterOperator {
  name : String
  predicate : (@types.StreamRecord) -> Bool
  metrics : OperatorMetrics
}

///|
/// Creates a new FilterOperator.
pub fn FilterOperator::new(
  name : String,
  predicate : (@types.StreamRecord) -> Bool,
) -> FilterOperator {
  { name, predicate, metrics: OperatorMetrics::new() }
}

///|
/// Processes a record through the FilterOperator.
pub fn FilterOperator::process_element(
  self : FilterOperator,
  record : @types.StreamRecord,
  out : Collector,
) -> Unit {
  self.metrics.record_in()
  if record.is_data() {
    if (self.predicate)(record) {
      self.metrics.record_out()
      out.collect(record)
    } else {
      self.metrics.record_drop()
    }
  } else {
    self.metrics.record_out()
    out.collect(record)
  }
}

///|
/// KeyBy operator partitioning streams by re-tagging keys via key extractor.
pub struct KeyByOperator {
  name : String
  extractor : (@types.StreamRecord) -> String
  metrics : OperatorMetrics
}

///|
/// Creates a new KeyByOperator.
pub fn KeyByOperator::new(
  name : String,
  extractor : (@types.StreamRecord) -> String,
) -> KeyByOperator {
  { name, extractor, metrics: OperatorMetrics::new() }
}

///|
/// Processes a record through the KeyByOperator.
pub fn KeyByOperator::process_element(
  self : KeyByOperator,
  record : @types.StreamRecord,
  out : Collector,
) -> Unit {
  self.metrics.record_in()
  if record.is_data() {
    let new_key = (self.extractor)(record)
    let rekeyed = @types.StreamRecord::data_with_header(
      record.header,
      new_key,
      record.timestamp,
      record.payload,
    )
    self.metrics.record_out()
    out.collect(rekeyed)
  } else {
    self.metrics.record_out()
    out.collect(record)
  }
}
