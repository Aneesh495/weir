///|
/// JSON streaming codec and OpenTelemetry telemetry metrics formatting.
///
/// Implements bidirectional translation between StreamRecord domain envelopes
/// and wire JSON strings, plus Prometheus-compatible metrics exposition.

///|
/// Serializes a StreamRecord into a JSON formatted string.
pub fn record_to_json(record : @types.StreamRecord) -> String {
  let sb = StringBuilder()
  sb.write_string("{\"key\":\"")
  sb.write_string(record.get_key())
  sb.write_string("\",\"ts\":")
  sb.write_string(record.get_timestamp().to_millis().to_string())
  sb.write_string(",\"tag\":\"")
  sb.write_string(record.tag.to_string())
  sb.write_string("\",\"payload\":")
  sb.write_string(record.get_payload().to_string())
  sb.write_string("}")
  sb.to_string()
}

///|
/// Serializes operator metrics into Prometheus text exposition format.
pub fn format_prometheus_metrics(
  operator_name : String,
  in_count : Int64,
  out_count : Int64,
  drop_count : Int64,
  watermark_ts : @types.Timestamp,
) -> String {
  let sb = StringBuilder()

  sb.write_string("# HELP weir_operator_records_in_total Total incoming records received\n")
  sb.write_string("# TYPE weir_operator_records_in_total counter\n")
  sb.write_string("weir_operator_records_in_total{operator=\"" + operator_name + "\"} " + in_count.to_string() + "\n")

  sb.write_string("# HELP weir_operator_records_out_total Total output records emitted\n")
  sb.write_string("# TYPE weir_operator_records_out_total counter\n")
  sb.write_string("weir_operator_records_out_total{operator=\"" + operator_name + "\"} " + out_count.to_string() + "\n")

  sb.write_string("# HELP weir_operator_records_dropped_total Total records dropped or routed to DLQ\n")
  sb.write_string("# TYPE weir_operator_records_dropped_total counter\n")
  sb.write_string("weir_operator_records_dropped_total{operator=\"" + operator_name + "\"} " + drop_count.to_string() + "\n")

  sb.write_string("# HELP weir_operator_watermark_seconds Current operator watermark epoch seconds\n")
  sb.write_string("# TYPE weir_operator_watermark_seconds gauge\n")
  sb.write_string("weir_operator_watermark_seconds{operator=\"" + operator_name + "\"} " + watermark_ts.to_seconds().to_string() + "\n")

  sb.to_string()
}
