# weir

A high-throughput distributed stateful stream processing and complex event runtime in MoonBit with out-of-order event-time watermarking, log-structured state storage, Chandy-Lamport distributed snapshotting, and continuous streaming SQL.

```
                         weir streaming engine
               =========================================

      +-------------------+                  +-------------------+
      | Ingestion Sources |                  | Egress & Sinks    |
      | TCP / JSON / File |                  | Socket / Terminal |
      +---------+---------+                  +---------^---------+
                |                                      |
                v                                      |
      +-------------------+                  +---------+---------+
      | Ring Buffers &    |                  | Pipelined Window  |
      | Flow Controllers  |                  | Aggregations      |
      +---------+---------+                  +---------^---------+
                |                                      |
                v                                      |
      +-------------------+                  +---------+---------+
      | Event-Time Engine |                  | Stateful Operators|
      | Watermarks / Lag  +----------------->| & CEP Automata    |
      +-------------------+                  +----+---------+----+
                                                  |         |
                                     State Store  |         | Snapshots
                                                  v         v
                                          +-------+----+----+------+
                                          | LSM Tree   | Chandy-   |
                                          | WAL+SSTable| Lamport   |
                                          +------------+-----------+
```

## Features

- Event-time processing with monotonic, bounded-out-of-orderness, and punctuated watermark generators.
- Dynamic windowing: tumbling, hopping (sliding), dynamic session gap merging, and count-based triggers.
- Embedded Log-Structured Merge (LSM) state backend with Write-Ahead Logging (WAL), MemTable, immutable SSTables with sparse index blocks, Bloom filters, and leveled compaction.
- Fault tolerance through asynchronous Chandy-Lamport barrier alignment and distributed state snapshots.
- Complex Event Processing (CEP) engine powered by Non-deterministic Finite Automata (NFA) with Kleene star, negation, and temporal constraints.
- Continuous streaming SQL engine (WeirSQL) featuring recursive-descent query parsing, AST representation, and operator DAG translation.
- High-performance binary wire protocol with magic frame markers and CRC32 payload verification.
- Interactive terminal telemetry dashboard and built-in production scenarios (fleet IoT telemetry, financial market microstructure, and service mesh SLA monitoring).

## Architecture

Weir is structured into modular layers designed for high-throughput, low-latency stream computation:

- `types`: Core stream primitives, records, timestamps, watermarks, barriers, and schema definitions.
- `buffer`: Zero-copy circular ring buffers and flow-controlled channels with backpressure policies.
- `watermark`: Watermark extractors, partition alignment, late event detection, and side-output routing.
- `window`: Window assigners, triggers, and stateful accumulators.
- `state`: LSM state backend, write-ahead logs, SSTables, Bloom filters, and compaction workers.
- `checkpoint`: Chandy-Lamport barrier injection, channel alignment, snapshot serialization, and failure recovery.
- `operator`: Stream operators (Map, FlatMap, Filter, KeyBy, WindowAggregate, CoGroup, HashJoin, Sinks).
- `cep`: NFA compiler, state traversal engine, and pattern sequence matching.
- `sql`: WeirSQL lexer, parser, semantic analyzer, and physical plan generator.
- `dag`: Pipelined execution DAG, topological sorting, and cycle detection.
- `network`: Binary framing codec, TCP streaming server/client, and metrics exporter.
- `scenarios`: Production scenarios for IoT fleet telemetry, financial order flow, and SLA monitoring.
- `cmd/weir`: Unified CLI binary with query runner, benchmark suite, and validation tools.

## Quick Start

### Prerequisites

- MoonBit CLI (`moon` version 0.1.20261104 or later)
- Python 3.10+ (for synthetic telemetry generators and visual dashboard)

### Building and Testing

```bash
# Run the complete test suite
moon test

# Build all packages
moon build

# Run the CLI entry point
moon run cmd/main
```

## License

Licensed under the Apache License, Version 2.0.
