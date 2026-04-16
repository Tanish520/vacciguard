# Archive Path Split Design

## Goal

Separate S3 archival writes from the hot alert path so Redis updates no longer wait behind slow or variable S3 work.

The goal is lower alert-path latency, especially p95 latency, without changing VacciGuard business logic.

## Problem Statement

The current optimized pipeline still performs alerting and archival work in one callback. Even after hot-path refactors, the live path can still inherit S3 variability because processed data, invalid records, and breach windows are written as part of the same execution flow.

That means:

- Redis waits for S3 completion
- p95 latency is exposed to S3 outliers
- the hot path and archive path compete for the same local Spark runtime when they live in the same process

We want the alert path to finish as soon as Redis is updated, while keeping the archive path durable and complete.

## Recommended Approach

Use a **separate archive job/container** for S3 writes.

In this design:

- the hot path service consumes Kafka, validates and classifies records, and writes Redis only
- the archive service consumes the same event stream in its own consumer group and writes S3 outputs
- both paths share the same transformation rules so business logic stays aligned

This gives us true runtime isolation instead of only code-path separation.

## Alternatives Considered

### 1. Two queries in the same service

One query handles Redis, the other handles S3.

Tradeoff:
- smaller change than a full split
- but both queries still share `local[*]`, one SparkSession, and one pod
- S3 jitter can still bleed into the hot path

### 2. Keep one query and offload S3 in-process

The hot callback would enqueue archive work to a background worker or thread.

Tradeoff:
- simplest to describe
- but still shares process resources
- harder to reason about correctness and backpressure

### 3. Separate archive job/container

This is the recommended option.

Tradeoff:
- more deployment pieces
- but the strongest isolation
- best chance of reliably improving p95 latency

## Architecture

### Hot Path Service

The hot path service is responsible for:

- reading Kafka
- validating records
- dropping duplicates
- enriching device data
- classifying breach state
- writing latest device state to Redis
- exposing hot-path metrics

It must not write S3.

### Archive Service

The archive service is responsible for:

- reading the same event stream in its own consumer group
- applying the same validation and classification rules
- writing processed records to S3
- writing invalid records to S3
- writing breach windows to S3
- exposing archive metrics

It must not write Redis.

### Shared Logic Boundary

Both services should share the same transformation helpers so that:

- schema changes are applied consistently
- validation rules stay aligned
- classification behavior stays identical

The split should happen at the side-effect boundary, not inside the business rules.

## Data Flow

### Hot Path

1. Kafka receives events.
2. Hot path consumes the events.
3. Records are validated, deduplicated, enriched, and classified.
4. Redis latest-state keys are updated.
5. The hot path returns.

### Archive Path

1. Kafka receives the same events.
2. Archive path consumes them in a separate consumer group.
3. Records are validated, deduplicated, enriched, and classified.
4. Processed, invalid, and breach-window outputs are written to S3.
5. The archive path returns.

## Operational Model

### Deployment

The optimized branch should carry two deployments:

- `stream-processor-hot`
- `stream-processor-archive`

Both can live in the `vacciguard-optimized` namespace.

### Runtime Expectations

- The hot path should have the smallest possible callback.
- The archive path can be slower as long as it stays durable and catches up.
- The archive path should never block alert visibility.

### Metrics

The hot path should expose:

- average latency
- p95 latency
- ingest-to-Redis p95
- consumer lag

The archive path should expose:

- processed events
- invalid events
- deduplicated events
- breach events
- output object counts

## Failure Handling

### If the archive job is down

- Redis alerts should still work.
- S3 lag can grow temporarily.
- The archive job should resume from Kafka offsets when it comes back.

### If the hot path is down

- alerts stop until recovery.
- archive processing may continue, but alerting is unavailable.

### If Kafka retention is too short

- the archive job can fall behind permanently.
- this is a capacity and retention risk that should be monitored.

## Migration Plan

1. Keep the current optimized path as the baseline reference.
2. Extract shared validation/classification helpers if needed.
3. Add a hot-path-only deployment that writes Redis and nothing else.
4. Add a separate archive deployment that writes S3 and nothing else.
5. Run both against the same workload and compare hot-path p95 to the current single-callback design.
6. Remove the remaining S3 writes from the hot path once the archive path is verified.

## Testing Strategy

We should verify:

- the hot path no longer writes S3
- the archive path no longer writes Redis
- both services can render their manifests cleanly
- both services can run in the optimized namespace
- the archive path still produces complete processed, invalid, and breach outputs
- the hot path still updates Redis correctly

Recommended checks:

```bash
kubectl kustomize infra/kubernetes/optimized
python3 -m unittest tests.stream.test_job -v
python3 -m unittest tests.evaluation.test_aws_native_evaluation_manifests -v
```

## Success Criteria

- Redis no longer waits behind S3 writes.
- Hot-path p95 latency is measurably lower than the current single-callback optimized run.
- S3 outputs remain complete and correct.
- The archive path can lag without affecting alert visibility.
- The architecture is clear enough that hot-path and archive changes can be tested independently.

