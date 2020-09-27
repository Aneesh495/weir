#!/usr/bin/env python3
"""
High-throughput synthetic stream telemetry generator for the Weir streaming runtime.

Generates realistic streaming records across three production domains:
1. Fleet Vehicle IoT GPS Telemetry
2. Financial Market Microstructure Ticks and Trades
3. Distributed Microservice RPC Span Traces
"""

import argparse
import json
import math
import random
import sys
import time
from typing import Dict, Any, Generator


def generate_fleet_telemetry(num_events: int, jitter_sec: float = 2.0) -> Generator[Dict[str, Any], None, None]:
    """Generates vehicle GPS telemetry with simulated speed and out-of-order timestamps."""
    vehicles = [f"truck_{i:02d}" for i in range(1, 11)]
    base_lat = 41.8781
    base_lon = -87.6298
    base_time = int(time.time() * 1000)

    for i in range(num_events):
        vehicle = random.choice(vehicles)
        delta_lat = (random.random() - 0.5) * 0.05
        delta_lon = (random.random() - 0.5) * 0.05
        speed = random.gauss(65.0, 15.0)
        speed = max(0.0, min(100.0, speed))
        fuel = max(10.0, 100.0 - (i * 0.05))

        # Add random out-of-order event-time jitter
        jitter_ms = int(random.uniform(-jitter_sec, jitter_sec) * 1000)
        event_time = base_time + (i * 500) + jitter_ms

        yield {
            "key": vehicle,
            "ts": event_time,
            "tag": "DataEvent",
            "payload": {
                "lat": round(base_lat + delta_lat, 5),
                "lon": round(base_lon + delta_lon, 5),
                "speed": round(speed, 2),
                "fuel": round(fuel, 1),
            }
        }


def generate_market_trades(num_events: int, inject_wash_trade: bool = True) -> Generator[Dict[str, Any], None, None]:
    """Generates financial market trades and optionally injects wash-trading patterns."""
    symbols = ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN"]
    traders = [f"trader_{i:02d}" for i in range(1, 8)]
    base_prices = {"NVDA": 490.0, "AAPL": 185.0, "MSFT": 420.0, "GOOGL": 140.0, "AMZN": 175.0}
    base_time = int(time.time() * 1000)

    for i in range(num_events):
        sym = random.choice(symbols)
        trader = random.choice(traders)
        side = "BUY" if random.random() > 0.5 else "SELL"
        price_drift = random.gauss(0.0, 0.5)
        price = round(base_prices[sym] + price_drift, 2)
        volume = random.choice([50, 100, 200, 500, 1000])
        event_time = base_time + (i * 100)

        yield {
            "key": sym,
            "ts": event_time,
            "tag": "DataEvent",
            "payload": {
                "trader": trader,
                "side": side,
                "price": price,
                "volume": volume,
            }
        }

    # Inject an intentional wash-trade pattern if requested
    if inject_wash_trade:
        wash_trader = "rogue_trader_01"
        t_now = base_time + (num_events * 100)
        yield {
            "key": "NVDA",
            "ts": t_now,
            "tag": "DataEvent",
            "payload": {
                "trader": wash_trader,
                "side": "SELL",
                "price": 495.0,
                "volume": 1000,
            }
        }
        yield {
            "key": "NVDA",
            "ts": t_now + 1500,
            "tag": "DataEvent",
            "payload": {
                "trader": wash_trader,
                "side": "BUY",
                "price": 495.0,
                "volume": 1000,
            }
        }


def generate_rpc_spans(num_events: int) -> Generator[Dict[str, Any], None, None]:
    """Generates distributed microservice RPC traces with simulated latency spikes."""
    services = ["auth-service", "checkout-service", "payment-service", "inventory-service"]
    endpoints = ["/v1/authenticate", "/v1/cart/checkout", "/v1/charge", "/v1/items/lookup"]
    base_time = int(time.time() * 1000)

    for i in range(num_events):
        svc = random.choice(services)
        ep = random.choice(endpoints)
        trace_id = f"trace_{random.randint(100000, 999999)}"

        # 95% normal latency, 5% latency spikes
        if random.random() > 0.05:
            latency = round(random.gauss(25.0, 5.0), 2)
            status = 200
        else:
            latency = round(random.gauss(400.0, 80.0), 2)
            status = random.choice([500, 503, 504])

        event_time = base_time + (i * 50)

        yield {
            "key": svc,
            "ts": event_time,
            "tag": "DataEvent",
            "payload": {
                "trace_id": trace_id,
                "endpoint": ep,
                "latency": max(1.0, latency),
                "status": status,
            }
        }


def main():
    parser = argparse.ArgumentParser(description="Synthetic stream telemetry generator for Weir runtime")
    parser.add_argument("--domain", choices=["fleet", "market", "telemetry"], default="fleet", help="Data domain")
    parser.add_argument("--count", type=int, default=100, help="Number of records to generate")
    parser.add_argument("--jitter", type=float, default=2.0, help="Out-of-order jitter in seconds")
    parser.add_argument("--output", type=str, default="", help="Output file path (default: stdout)")
    args = parser.parse_args()

    if args.domain == "fleet":
        generator = generate_fleet_telemetry(args.count, args.jitter)
    elif args.domain == "market":
        generator = generate_market_trades(args.count)
    else:
        generator = generate_rpc_spans(args.count)

    out_file = open(args.output, "w") if args.output else sys.stdout
    try:
        for record in generator:
            out_file.write(json.dumps(record) + "\n")
    finally:
        if args.output:
            out_file.close()


if __name__ == "__main__":
    main()
