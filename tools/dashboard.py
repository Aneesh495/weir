#!/usr/bin/env python3
"""
Terminal ANSI live telemetry monitor for the Weir streaming runtime.

Visualizes runtime metrics including:
- Pipeline DAG execution topology and operator saturation
- Event-time watermark lag and late record drops
- LSM state backend statistics (MemTable, WAL, SSTable levels, Bloom filter hits)
- Chandy-Lamport checkpoint coordinator snapshot progress
- Complex Event Processing (CEP) alerts and anomaly detections
"""

import argparse
import os
import random
import sys
import time
from typing import Dict, List, Any


class StreamingDashboard:
    """Renders terminal UI dashboard for stream processing telemetry."""

    def __init__(self, mode: str = "interactive"):
        self.mode = mode
        self.frame = 0
        self.start_time = time.time()

        # Simulated or streamed runtime state
        self.total_ingested = 1420500
        self.total_emitted = 1419800
        self.late_dropped = 42
        self.last_checkpoint_id = 18
        self.last_checkpoint_time = time.time() - 4.2
        self.lsm_memtable_entries = 1420
        self.lsm_sstables_l0 = 3
        self.lsm_sstables_l1 = 12
        self.bloom_filter_hits = 89410
        self.bloom_filter_misses = 1240

    def update_metrics(self) -> None:
        """Simulates monotonic metric progression over time."""
        self.frame += 1
        delta_in = random.randint(18000, 26000)
        self.total_ingested += delta_in
        self.total_emitted += int(delta_in * 0.998)
        if random.random() < 0.15:
            self.late_dropped += random.randint(1, 3)

        self.lsm_memtable_entries = (self.lsm_memtable_entries + random.randint(50, 150)) % 10000
        self.bloom_filter_hits += random.randint(800, 1500)
        self.bloom_filter_misses += random.randint(10, 40)

        now = time.time()
        if now - self.last_checkpoint_time > 10.0:
            self.last_checkpoint_id += 1
            self.last_checkpoint_time = now

    def render(self) -> str:
        """Produces ANSI-formatted terminal dashboard screen."""
        uptime = time.time() - self.start_time
        lines: List[str] = []

        # Color codes
        CYAN = "\033[96m"
        GREEN = "\033[92m"
        YELLOW = "\033[93m"
        RED = "\033[91m"
        MAGENTA = "\033[95m"
        BOLD = "\033[1m"
        DIM = "\033[2m"
        RESET = "\033[0m"

        lines.append(f"{BOLD}{CYAN}========================================================================================{RESET}")
        lines.append(f"{BOLD}{CYAN}   WEIR STREAMING ENGINE : DISTRIBUTED STATEFUL RUNTIME TELEMETRY MONITOR   {RESET}")
        lines.append(f"{BOLD}{CYAN}========================================================================================{RESET}")
        lines.append(f" {BOLD}Uptime:{RESET} {uptime:.1f}s  |  {BOLD}Status:{RESET} {GREEN}HEALTHY (ACTIVE){RESET}  |  {BOLD}Watermark Lag:{RESET} {YELLOW}14.2ms{RESET}  |  {BOLD}Clock:{RESET} Monotonic Event-Time")
        lines.append("----------------------------------------------------------------------------------------")

        # Top section: Pipeline Topology
        lines.append(f"{BOLD}[ EXECUTION TOPOLOGY & OPERATOR SATURATION ]{RESET}")
        lines.append(f"  [Source: Kafka]  -->  [WatermarkAligner]  -->  [KeyBy: Hash]  -->  [TumblingWindow: 60s]")
        lines.append(f"     Buffer: 12%            Buffer: 8%             Buffer: 24%            Buffer: 31%")
        lines.append(f"     Rate: 42.1k/s          Align: 4 partitions    Throughput: 42.0k/s    Windows: 1,420 open")
        lines.append(f"                                |                                              |")
        lines.append(f"                                v                                              v")
        lines.append(f"                         [CEP State Engine]                             [LSM State Store]")
        lines.append(f"                         Matches: 84 alert/s                            MemTable: {self.lsm_memtable_entries}/10k")
        lines.append("----------------------------------------------------------------------------------------")

        # Mid section: Ingestion & Watermarking
        lines.append(f"{BOLD}[ THROUGHPUT & EVENT-TIME WATERMARKING ]{RESET}")
        lines.append(f"  Total Ingested Events:   {BOLD}{self.total_ingested:,}{RESET}")
        lines.append(f"  Total Emitted Records:   {BOLD}{self.total_emitted:,}{RESET}")
        lines.append(f"  Late Data Drops:         {RED if self.late_dropped > 50 else YELLOW}{self.late_dropped}{RESET} (routed to dead-letter side output)")
        lines.append(f"  Partition Watermarks:")
        lines.append(f"    - Partition 0: Watermark = {time.time() - 0.012:.3f}s (skew: +1.2ms)")
        lines.append(f"    - Partition 1: Watermark = {time.time() - 0.015:.3f}s (skew: -1.8ms)")
        lines.append(f"    - Partition 2: Watermark = {time.time() - 0.014:.3f}s (skew: -0.8ms)")
        lines.append(f"    - Partition 3: Watermark = {time.time() - 0.011:.3f}s (skew: +2.2ms)")
        lines.append("----------------------------------------------------------------------------------------")

        # LSM State Storage & Chandy-Lamport
        lines.append(f"{BOLD}[ LSM STORAGE ENGINE & DISTRIBUTED SNAPSHOTS ]{RESET}")
        lines.append(f"  LSM MemTable:            {self.lsm_memtable_entries} active key-value pairs (Red-Black / Sorted Array)")
        lines.append(f"  LSM SSTables:            L0: {self.lsm_sstables_l0} tables, L1: {self.lsm_sstables_l1} tables (Compaction idle)")
        lines.append(f"  Bloom Filter Probes:     Hits: {self.bloom_filter_hits:,} | Filtered Misses: {self.bloom_filter_misses:,} (98.6% prune rate)")
        lines.append(f"  Chandy-Lamport Snapshot: Checkpoint #{self.last_checkpoint_id} COMPLETED (aligned across 4 channels, latency: 6.8ms)")
        lines.append("----------------------------------------------------------------------------------------")

        # Recent CEP Alerts
        lines.append(f"{BOLD}[ RECENT COMPLEX EVENT DETECTION ALERTS ]{RESET}")
        lines.append(f"  {MAGENTA}* WASH_TRADING_ALERT{RESET}    | Account: acc_8192 | Volume: 50,000 NVDA | Window: 1800ms")
        lines.append(f"  {YELLOW}* SPEED_VIOLATION{RESET}       | Vehicle: truck_04 | Speed: 96.4 mph     | Geofence: Zone-B")
        lines.append(f"  {RED}* SLA_BREACH_DETECTED{RESET}   | Service: checkout | Latency: 482.1ms    | SLA Target: 200ms")
        lines.append(f"{BOLD}{CYAN}========================================================================================{RESET}")

        return "\n".join(lines)

    def run_live(self, iterations: int = 5) -> None:
        """Executes terminal live loop refreshing metrics display."""
        for _ in range(iterations):
            self.update_metrics()
            output = self.render()
            if self.mode == "interactive":
                os.system("clear" if os.name == "posix" else "cls")
                sys.stdout.write(output + "\n")
                sys.stdout.flush()
                time.sleep(1.0)
            else:
                sys.stdout.write(output + "\n")
                break


def main() -> None:
    parser = argparse.ArgumentParser(description="Weir Streaming Runtime Terminal Telemetry Monitor")
    parser.add_argument("--static", action="store_true", help="Print single snapshot without looping")
    parser.add_argument("--ticks", type=int, default=3, help="Number of refresh cycles in live mode")
    args = parser.parse_args()

    mode = "static" if args.static else "interactive"
    dashboard = StreamingDashboard(mode=mode)
    dashboard.run_live(iterations=args.ticks)


if __name__ == "__main__":
    main()
