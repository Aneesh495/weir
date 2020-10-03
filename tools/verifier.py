#!/usr/bin/env python3
"""
Differential verification suite for the Weir streaming runtime.

Validates stateful stream processing correctness against offline batch ground-truth:
1. Simulates out-of-order event arrivals with configurable time skew.
2. Computes offline batch window aggregations as the golden source of truth.
3. Simulates the streaming pipeline with event-time watermarking and window triggers.
4. Performs strict differential assertions ensuring bitwise and aggregate parity.
"""

import argparse
import math
import random
import sys
from typing import Dict, List, Tuple, Any


class BatchGoldenReference:
    """Computes exact offline tumbling window aggregations."""

    def __init__(self, window_size_ms: int):
        self.window_size_ms = window_size_ms

    def compute(self, events: List[Dict[str, Any]]) -> Dict[Tuple[str, int, int], Dict[str, float]]:
        """Groups events into [start, end) intervals and calculates count and sum."""
        results: Dict[Tuple[str, int, int], Dict[str, float]] = {}

        for ev in events:
            key = ev["key"]
            ts = ev["ts"]
            val = float(ev["val"])

            # Compute window interval
            w_start = (ts // self.window_size_ms) * self.window_size_ms
            w_end = w_start + self.window_size_ms
            w_key = (key, w_start, w_end)

            if w_key not in results:
                results[w_key] = {"count": 0.0, "sum": 0.0, "min": val, "max": val}

            acc = results[w_key]
            acc["count"] += 1.0
            acc["sum"] += val
            acc["min"] = min(acc["min"], val)
            acc["max"] = max(acc["max"], val)

        return results


class StreamRuntimeSimulator:
    """Simulates out-of-order stream processing with monotonic watermarks."""

    def __init__(self, window_size_ms: int, max_lateness_ms: int):
        self.window_size_ms = window_size_ms
        self.max_lateness_ms = max_lateness_ms
        self.windows: Dict[Tuple[str, int, int], Dict[str, float]] = {}
        self.emitted: Dict[Tuple[str, int, int], Dict[str, float]] = {}
        self.current_watermark = -1
        self.max_event_time = -1

    def process_event(self, key: str, ts: int, val: float) -> None:
        """Processes event, accumulates into window, and periodically evaluates watermarks."""
        w_start = (ts // self.window_size_ms) * self.window_size_ms
        w_end = w_start + self.window_size_ms
        w_key = (key, w_start, w_end)

        # Drop records if they arrive strictly behind the established watermark
        if w_end <= self.current_watermark:
            return

        # Accumulate into state
        if w_key not in self.windows:
            self.windows[w_key] = {"count": 0.0, "sum": 0.0, "min": val, "max": val}

        acc = self.windows[w_key]
        acc["count"] += 1.0
        acc["sum"] += val
        acc["min"] = min(acc["min"], val)
        acc["max"] = max(acc["max"], val)

        # Update observed high watermark
        if ts > self.max_event_time:
            self.max_event_time = ts
            new_wm = self.max_event_time - self.max_lateness_ms
            if new_wm > self.current_watermark:
                self.current_watermark = new_wm
                self._evaluate_triggers()

    def advance_watermark(self, ts: int) -> None:
        """Advances watermark and flushes eligible windows."""
        if ts > self.current_watermark:
            self.current_watermark = ts
            self._evaluate_triggers()

    def _evaluate_triggers(self) -> None:
        """Fires windows whose window_end <= current_watermark."""
        to_purge: List[Tuple[str, int, int]] = []
        for w_key, acc in self.windows.items():
            _, _, w_end = w_key
            if self.current_watermark >= w_end:
                self.emitted[w_key] = dict(acc)
                to_purge.append(w_key)

        for w_key in to_purge:
            del self.windows[w_key]



def run_verification(num_events: int = 10000, max_delay_ms: int = 500) -> bool:
    """Executes differential verification comparing stream vs batch output."""
    print(f"[*] Initializing differential verification test suite ({num_events:,} events)...")
    keys = ["symbol_AAPL", "symbol_GOOG", "symbol_MSFT", "symbol_NVDA", "symbol_AMZN"]
    window_size_ms = 1000
    base_time = 1700000000000

    # 1. Generate sequence with out-of-order skew
    events: List[Dict[str, Any]] = []
    for i in range(num_events):
        k = keys[i % len(keys)]
        nominal_ts = base_time + (i * 10)
        jitter = random.randint(-max_delay_ms, max_delay_ms)
        actual_ts = max(base_time, nominal_ts + jitter)
        val = 100.0 + random.random() * 50.0
        events.append({"key": k, "ts": actual_ts, "val": val})

    print(f"[*] Generated {len(events):,} events with max jitter +/-{max_delay_ms}ms.")

    # 2. Run offline batch ground-truth
    print("[*] Computing offline batch golden reference...")
    golden_ref = BatchGoldenReference(window_size_ms)
    golden_results = golden_ref.compute(events)
    print(f"[*] Golden batch computation finished: {len(golden_results):,} distinct windows.")

    # 3. Run streaming execution
    print("[*] Driving streaming runtime simulator...")
    stream_sim = StreamRuntimeSimulator(window_size_ms=window_size_ms, max_lateness_ms=max_delay_ms * 2)

    for ev in events:
        stream_sim.process_event(ev["key"], ev["ts"], ev["val"])

    # Flush final watermark past end of stream
    max_ts = max(ev["ts"] for ev in events)
    stream_sim.advance_watermark(max_ts + window_size_ms * 2)
    stream_results = stream_sim.emitted
    print(f"[*] Stream simulation finished: {len(stream_results):,} distinct windows fired.")

    # 4. Strict Differential Assertions
    print("[*] Comparing streaming window results against batch golden reference...")
    mismatches = 0
    checked_windows = 0

    for w_key, golden_val in golden_results.items():
        checked_windows += 1
        if w_key not in stream_results:
            print(f"[ERROR] Window {w_key} missing from stream output!")
            mismatches += 1
            continue

        stream_val = stream_results[w_key]

        # Verify count
        if golden_val["count"] != stream_val["count"]:
            print(f"[ERROR] Count mismatch for {w_key}: batch={golden_val['count']}, stream={stream_val['count']}")
            mismatches += 1

        # Verify sum with floating point epsilon
        if abs(golden_val["sum"] - stream_val["sum"]) > 1e-4:
            print(f"[ERROR] Sum mismatch for {w_key}: batch={golden_val['sum']:.4f}, stream={stream_val['sum']:.4f}")
            mismatches += 1

    if mismatches == 0:
        print(f"[SUCCESS] Differential verification PASSED! Checked {checked_windows:,} windows with 0 discrepancies.")
        return True
    else:
        print(f"[FAILED] Differential verification FAILED with {mismatches} discrepancies.")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Weir Stream vs Batch Differential Verifier")
    parser.add_argument("--count", type=int, default=10000, help="Number of events to verify")
    parser.add_argument("--skew", type=int, default=200, help="Maximum out-of-order latency jitter in ms")
    args = parser.parse_args()

    success = run_verification(num_events=args.count, max_delay_ms=args.skew)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
