///|
/// Comprehensive tests for production scenarios: Fleet IoT, Financial Market Microstructure, and Service Mesh SLA.

///|
test "fleet iot telemetry speeding detector and trip aggregator" {
  let detector = create_speeding_detector(70.0)
  let col = @operator.Collector::new()

  let ts = @types.Timestamp::from_seconds(100L)
  let t_normal : VehicleTelemetry = {
    vehicle_id: "van_10",
    timestamp: ts,
    latitude: 40.7128,
    longitude: -74.0060,
    speed_mph: 55.0,
    fuel_level: 85.0,
  }
  let t_speeding : VehicleTelemetry = {
    vehicle_id: "van_10",
    timestamp: ts.add_duration(@types.Duration::from_seconds(10L)),
    latitude: 40.7135,
    longitude: -74.0050,
    speed_mph: 82.5,
    fuel_level: 84.5,
  }

  detector.process_element(t_normal.to_record(), col)
  assert_eq(col.length(), 0) // Normal speed dropped

  detector.process_element(t_speeding.to_record(), col)
  assert_eq(col.length(), 1) // Speeding flagged!

  // Trip aggregator
  let agg = FleetTripAggregator::new(70.0)
  agg.add(55.0)
  agg.add(82.5)
  agg.add(65.0)
  assert_eq(agg.peak_speed, 82.5)
  assert_eq(agg.speeding_count, 1)
}

///|
test "financial market microstructure vwap calculation and wash trading cep" {
  // 1. VWAP calculation
  let vwap = VWAPAccumulator::new()
  vwap.add_trade(150.0, 100L) // 15,000
  vwap.add_trade(153.0, 200L) // 30,600
  // Total sum = 45,600 / 300 = 152.0
  assert_eq(vwap.get_vwap(), 152.0)

  // 2. Wash trading fraud pattern detection via CEP
  let cep = create_wash_trading_cep_engine("trader_alpha")

  let t0 = @types.Timestamp::from_seconds(1000L)
  let trade_sell : MarketTrade = {
    symbol: "NVDA",
    trader_id: "trader_alpha",
    side: "SELL",
    price: 495.0,
    volume: 500L,
    timestamp: t0,
  }
  let m1 = cep.process_event(trade_sell.to_record())
  assert_eq(m1.length(), 0) // Step 1 registered

  // Legitimate trade from another trader (should be ignored by relaxed contiguity)
  let trade_other : MarketTrade = {
    symbol: "NVDA",
    trader_id: "trader_beta",
    side: "BUY",
    price: 495.5,
    volume: 100L,
    timestamp: t0.add_duration(@types.Duration::from_seconds(1L)),
  }
  let m_other = cep.process_event(trade_other.to_record())
  assert_eq(m_other.length(), 0)

  // Suspicious buy by trader_alpha 2 seconds later (within 3s window)
  let trade_buy : MarketTrade = {
    symbol: "NVDA",
    trader_id: "trader_alpha",
    side: "BUY",
    price: 495.0,
    volume: 500L,
    timestamp: t0.add_duration(@types.Duration::from_seconds(2L)),
  }
  let m2 = cep.process_event(trade_buy.to_record())
  assert_eq(m2.length(), 1) // Wash trading circular fraud detected!
  assert_eq(m2[0].length(), 2)
}

///|
test "service mesh rpc telemetry latency percentiles and sla breach detection" {
  let sla = SlaAggregator::new()

  // Ingest 95 fast requests (25ms, status 200)
  for i = 0; i < 95; i = i + 1 {
    sla.add_span(25.0, 200)
  }

  // Ingest 5 slow/error requests (350ms, status 503)
  for i = 0; i < 5; i = i + 1 {
    sla.add_span(350.0, 503)
  }

  assert_eq(sla.total_requests, 100L)
  assert_eq(sla.error_requests, 5L)
  assert_eq(sla.error_rate(), 0.05) // 5% error rate

  // p50 is 25ms, p99 is 350ms
  assert_eq(sla.percentile(0.50), 25.0)
  assert_eq(sla.percentile(0.99), 350.0)

  // Breaches SLA of 200ms p99 and 2% max error rate
  assert_true(sla.breaches_sla(200.0, 0.02))

  // Complies with relaxed SLA of 500ms p99 and 10% max error rate
  assert_false(sla.breaches_sla(500.0, 0.10))
}
