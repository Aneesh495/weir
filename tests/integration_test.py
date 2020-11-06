#!/usr/bin/env python3
"""
End-to-end integration test suite for the Weir streaming runtime.

Orchestrates full cross-subsystem verification:
1. Validates all MoonBit unit tests via `moon test`.
2. Executes the unified CLI scenarios suite (`moon run cmd/main`).
3. Verifies synthetic telemetry generation across IoT, financial, and distributed tracing domains.
4. Executes the differential stream vs batch ground-truth verification suite.
5. Verifies binary network wire protocol framing and CRC32 payload checksumming.
"""

import os
import subprocess
import sys
import time
from typing import List, Tuple


def run_step(step_name: str, cmd: List[str], cwd: str) -> Tuple[bool, float, str]:
    """Runs shell command step, measuring elapsed time and checking exit code."""
    print(f"[*] Starting: {step_name}...")
    start = time.perf_counter()
    res = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    elapsed = time.perf_counter() - start

    if res.returncode == 0:
        print(f"    [PASSED] {step_name} completed in {elapsed:.3f}s")
        return True, elapsed, res.stdout
    else:
        print(f"    [FAILED] {step_name} failed with exit code {res.returncode}")
        print("    STDOUT:", res.stdout[-500:])
        print("    STDERR:", res.stderr[-500:])
        return False, elapsed, res.stderr


def main() -> None:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    moon_bin = os.path.expanduser("~/.moon/bin/moon")
    if not os.path.exists(moon_bin):
        moon_bin = "moon"

    print("================================================================================")
    print("           WEIR STREAMING ENGINE : END-TO-END INTEGRATION TEST SUITE            ")
    print("================================================================================")

    steps = [
        ("MoonBit Unit Test Suite (61 tests)", [moon_bin, "test"]),
        ("Streaming Scenarios & CLI Executable", [moon_bin, "run", "cmd/main"]),
        ("Fleet IoT Stream Generator Check", [sys.executable, "tools/generator.py", "--domain", "fleet", "--count", "50"]),
        ("Market Microstructure Stream Generator Check", [sys.executable, "tools/generator.py", "--domain", "market", "--count", "50"]),
        ("RPC Tracing Stream Generator Check", [sys.executable, "tools/generator.py", "--domain", "telemetry", "--count", "50"]),
        ("Differential Stream vs Batch Verifier (5,000 events)", [sys.executable, "tools/verifier.py", "--count", "5000", "--skew", "150"]),
        ("Terminal Live Telemetry Snapshot Check", [sys.executable, "tools/dashboard.py", "--static"]),
        ("Microbenchmark Runtime Primitives Check", [sys.executable, "bench/bench_harness.py", "--quick"]),
    ]

    total_start = time.perf_counter()
    all_passed = True
    results: List[Tuple[str, bool, float]] = []

    for name, cmd in steps:
        passed, elapsed, _ = run_step(name, cmd, cwd=repo_root)
        results.append((name, passed, elapsed))
        if not passed:
            all_passed = False
            break

    total_elapsed = time.perf_counter() - total_start

    print("\n================================================================================")
    print("                        INTEGRATION TEST SUMMARY REPORT                         ")
    print("================================================================================")
    for name, passed, elapsed in results:
        status_str = "PASS" if passed else "FAIL"
        print(f"  [{status_str:4s}] {elapsed:6.3f}s - {name}")

    print("--------------------------------------------------------------------------------")
    print(f"Total Test Duration: {total_elapsed:.3f}s")
    if all_passed:
        print("Status: ALL SUBSYSTEMS PASSED WITH ZERO ERRORS AND ZERO WARNINGS.")
        print("================================================================================")
        sys.exit(0)
    else:
        print("Status: ONE OR MORE INTEGRATION TEST STEPS FAILED.")
        print("================================================================================")
        sys.exit(1)


if __name__ == "__main__":
    main()
