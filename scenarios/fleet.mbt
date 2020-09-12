///|
/// Real-world scenario: Connected Vehicle Fleet IoT Telemetry & Trip Analysis.
///
/// Processes high-frequency GPS telemetry, flags speed violations, and groups
/// movement bursts into driving sessions with summary statistics.

///|
/// Vehicle GPS observation record.
pub struct VehicleTelemetry {
  vehicle_id : String
  timestamp : @types.Timestamp
  latitude : Double
  longitude : Double
  speed_mph : Double
  fuel_level : Double
}

///|
/// Constructs a VehicleTelemetry record.
pub fn VehicleTelemetry::new(
  vehicle_id : String,
  timestamp : @types.Timestamp,
  latitude : Double,
  longitude : Double,
  speed_mph : Double,
  fuel_level : Double,
) -> VehicleTelemetry {
  { vehicle_id, timestamp, latitude, longitude, speed_mph, fuel_level }
}

///|
/// Converts VehicleTelemetry into a StreamRecord domain envelope.
pub fn VehicleTelemetry::to_record(self : VehicleTelemetry) -> @types.StreamRecord {
  let payload = @types.EventPayload::map([
    ("lat", @types.EventPayload::double(self.latitude)),
    ("lon", @types.EventPayload::double(self.longitude)),
    ("speed", @types.EventPayload::double(self.speed_mph)),
    ("fuel", @types.EventPayload::double(self.fuel_level)),
  ])
  @types.StreamRecord::data(self.vehicle_id, self.timestamp, payload)
}

///|
/// Speeding violation alert record.
pub struct SpeedingAlert {
  vehicle_id : String
  timestamp : @types.Timestamp
  speed_mph : Double
  speed_limit : Double
}

///|
/// Creates a SpeedingAlert record.
pub fn SpeedingAlert::new(
  vehicle_id : String,
  timestamp : @types.Timestamp,
  speed_mph : Double,
  speed_limit : Double,
) -> SpeedingAlert {
  { vehicle_id, timestamp, speed_mph, speed_limit }
}

///|
/// Pipeline operator filtering and alerting on vehicles exceeding speed limits.
pub fn create_speeding_detector(
  speed_limit : Double,
) -> @operator.FilterOperator {
  @operator.FilterOperator::new("speeding_detector", fn(rec) {
    match rec.get_payload().get_field("speed") {
      Some(@types.EventPayload::DoublePayload(s)) => s > speed_limit
      _ => false
    }
  })
}

///|
/// Evaluates trip statistics for a vehicle across a session window.
pub struct FleetTripAggregator {
  mut peak_speed : Double
  mut speeding_count : Int
  speed_limit : Double
}

///|
/// Creates a clean FleetTripAggregator.
pub fn FleetTripAggregator::new(speed_limit : Double) -> FleetTripAggregator {
  { peak_speed: 0.0, speeding_count: 0, speed_limit }
}

///|
/// Ingests a telemetry record into the running trip session.
pub fn FleetTripAggregator::add(
  self : FleetTripAggregator,
  speed_mph : Double,
) -> Unit {
  if speed_mph > self.peak_speed {
    self.peak_speed = speed_mph
  }
  if speed_mph > self.speed_limit {
    self.speeding_count = self.speeding_count + 1
  }
}
