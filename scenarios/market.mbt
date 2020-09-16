///|
/// Real-world scenario: Financial Market Microstructure & Wash Trading Detection.
///
/// Computes Volume-Weighted Average Price (VWAP) over sliding windows and flags
/// circular wash-trading manipulation patterns using the CEP engine.

///|
/// Trade execution event record.
pub struct MarketTrade {
  symbol : String
  trader_id : String
  side : String // "BUY" or "SELL"
  price : Double
  volume : Int64
  timestamp : @types.Timestamp
}

///|
/// Constructs a MarketTrade record.
pub fn MarketTrade::new(
  symbol : String,
  trader_id : String,
  side : String,
  price : Double,
  volume : Int64,
  timestamp : @types.Timestamp,
) -> MarketTrade {
  { symbol, trader_id, side, price, volume, timestamp }
}

///|
/// Converts MarketTrade to a StreamRecord.
pub fn MarketTrade::to_record(self : MarketTrade) -> @types.StreamRecord {
  let payload = @types.EventPayload::map([
    ("trader", @types.EventPayload::string(self.trader_id)),
    ("side", @types.EventPayload::string(self.side)),
    ("price", @types.EventPayload::double(self.price)),
    ("volume", @types.EventPayload::int(self.volume)),
  ])
  @types.StreamRecord::data(self.symbol, self.timestamp, payload)
}

///|
/// Volume-Weighted Average Price (VWAP) accumulator for financial tick streams.
pub struct VWAPAccumulator {
  mut price_volume_sum : Double
  mut total_volume : Int64
}

///|
/// Creates a clean VWAPAccumulator.
pub fn VWAPAccumulator::new() -> VWAPAccumulator {
  { price_volume_sum: 0.0, total_volume: 0L }
}

///|
/// Ingests a trade execution tick.
pub fn VWAPAccumulator::add_trade(
  self : VWAPAccumulator,
  price : Double,
  volume : Int64,
) -> Unit {
  self.price_volume_sum = self.price_volume_sum + price * volume.to_double()
  self.total_volume = self.total_volume + volume
}

///|
/// Calculates current VWAP, or 0.0 if volume is zero.
pub fn VWAPAccumulator::get_vwap(self : VWAPAccumulator) -> Double {
  if self.total_volume == 0L {
    0.0
  } else {
    self.price_volume_sum / self.total_volume.to_double()
  }
}

///|
/// Constructs a CEP pattern detector for detecting suspicious wash trading.
/// Flags when the same trader sells and subsequently buys within a 3-second window.
pub fn create_wash_trading_cep_engine(
  target_trader : String,
) -> @cep.CEPEngine {
  let pattern = @cep.PatternSequence::begin("trader_sell", fn(rec) {
    let trader_match = match rec.get_payload().get_field("trader") {
      Some(@types.EventPayload::StringPayload(t)) => t == target_trader
      _ => false
    }
    let side_match = match rec.get_payload().get_field("side") {
      Some(@types.EventPayload::StringPayload(s)) => s == "SELL"
      _ => false
    }
    trader_match && side_match
  }).followed_by("trader_buy", fn(rec) {
    let trader_match = match rec.get_payload().get_field("trader") {
      Some(@types.EventPayload::StringPayload(t)) => t == target_trader
      _ => false
    }
    let side_match = match rec.get_payload().get_field("side") {
      Some(@types.EventPayload::StringPayload(s)) => s == "BUY"
      _ => false
    }
    trader_match && side_match
  }).within(@types.Duration::from_seconds(3L))

  let nfa = @cep.NFACompiler::compile(pattern)
  @cep.CEPEngine::new(nfa)
}
