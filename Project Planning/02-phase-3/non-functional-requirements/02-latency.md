# Latency

## Requirement
The project brief expects near-real-time analytics with latency below 5 seconds.

## What This Means
The system should process live telemetry quickly enough that alerts and current-state outputs are still useful.

The main live-path latency measure will be:

- `end-to-end delay = output_time - event_time`

## How The Architecture Addresses It
- `Kafka` accepts events quickly and separates producers from consumers.
- `Spark Structured Streaming` processes events continuously instead of waiting for large batch windows.
- `Device and facility lookup data` is kept small so enrichment remains lightweight.
- `Redis` stores live operational state so the current view does not depend on heavy analytical queries.
- `S3 + Parquet` is used for historical analytics, not for the immediate alerting path.

## Design Principle
The system uses two paths:

- a fast live path for alerts and current state
- a slower batch path for reports and historical analysis

This keeps live processing focused and helps protect the latency target.

## How We Will Measure It
We will record:

- average latency
- p95 latency
- p99 latency if possible
- watermark delay or processing lag if available

We will test latency under:

- normal load
- burst load
- after scaling events
- during partial failure and recovery

## What We Can Honestly Claim
If the live path stays under 5 seconds in normal conditions and remains reasonable during spikes, we can claim near-real-time performance. If burst scenarios cross the threshold, we should report that as a trade-off instead of hiding it.
