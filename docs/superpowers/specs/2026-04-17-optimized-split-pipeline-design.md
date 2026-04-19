# Optimized Split Pipeline Design

**Date:** `2026-04-17`
**Branch:** `baseline-spike-fix`
**Owner:** `Codex + Tanish`

---

## Goal

Replace the current single-process `optimized` pipeline with a split architecture that keeps the existing external outputs unchanged while materially improving spike-load latency. The target is to make the optimized pipeline capable of sustaining the evaluation spike workload with a realistic path to sub-5-second latency.

The design keeps these external contracts stable:

- same Redis device-status payload shape
- same S3 processed / invalid / breach output layout
- same evaluation-controller report fields
- same optimized pipeline target from the user’s point of view

---

## Problem Summary

The current pipeline has three structural issues under spike load:

1. The hot Redis-update path and the cold S3/archive path share one Spark application and compete for the same CPU.
2. The hot path still performs driver-heavy work, including latest-row collapse and Redis writes, which limits real-time throughput.
3. The evaluation/controller setup assumes a single stream-processor deployment, which prevents the hot path from being isolated and resourced independently.

This causes the spike workload to build Kafka backlog faster than the hot path can drain it, which makes latency explode even though the pipeline eventually processes the data.

---

## Recommended Approach

The optimized pipeline will become two cooperating services that consume the same Kafka input independently:

- `optimized-hot`
- `optimized-cold`

Both together form the single `optimized` pipeline target.

### Why this approach

- It directly isolates the SLA-critical Redis path from slow archive work.
- It preserves the current report/output workflow.
- It gives the hot path independent CPU, trigger cadence, checkpointing, and scaling behavior.
- It is easier to explain in the project report than deeper queue-topology changes.

### Alternatives considered

#### 1. Keep one service and optimize internals only

Pros:
- smaller code change
- less controller/manifests work

Cons:
- hot and cold still contend for CPU
- weaker confidence for sustained spike performance

#### 2. Add internal Kafka fan-out or extra topics

Pros:
- strongest end-to-end isolation

Cons:
- more moving parts
- more operational complexity
- harder to justify for the current project scope

This design chooses the middle path: separate services without adding extra internal topics.

---

## Architecture

### Optimized Hot Service

`optimized-hot` is the real-time path.

Responsibilities:

- consume Kafka telemetry
- validate records needed for real-time state
- deduplicate events
- remove or materially raise the hot-path Kafka intake cap so spike throughput is not artificially limited by `MAX_OFFSETS_PER_TRIGGER`
- compute latest state per device using a grouped latest-row reduction rather than a windowed `row_number()` sort
- compute breach status for live state
- write latest device state to Redis using partition-local Redis pipelines after latest-state reduction, not a driver-side `toLocalIterator()` loop
- publish SLA latency metrics

Non-responsibilities:

- S3 processed output
- invalid-output archival
- breach-window archival
- cumulative archive/report counts

### Optimized Cold Service

`optimized-cold` is the archival/reporting path.

Responsibilities:

- consume Kafka telemetry independently
- compute processed / invalid / breach-window outputs
- write archive outputs to S3
- publish archive-related counts and status
- support evaluation reporting that depends on cold outputs

Non-responsibilities:

- Redis status writes
- ownership of SLA latency metrics

### Shared Input Model

Both services read the same Kafka topic independently.

Each service has:

- its own checkpoint path
- its own deployment
- its own resource requests/limits
- its own readiness lifecycle

This keeps failure isolation strong while preserving the existing Kafka-based pipeline structure.

---

## Data Flow

### Hot Path

`Kafka -> optimized-hot -> validate/dedup/latest-state -> Redis -> latency metrics`

Key design intent:

- keep the hot path minimal
- avoid any S3 writes
- keep ownership of end-to-end latency in the hot service only
- tune hot intake explicitly for spike handling, including a higher or removed `MAX_OFFSETS_PER_TRIGGER` and a faster trigger cadence
- replace driver-heavy latest-state and Redis-write steps with distributed executor-side work where safe

### Cold Path

`Kafka -> optimized-cold -> classify/process/aggregate -> S3 -> archive/count metrics`

Key design intent:

- allow slower batching
- preserve existing archive outputs
- let cold work lag temporarily without breaking Redis freshness

### External Output Compatibility

The following must remain compatible with the existing evaluation/reporting flow:

- Redis key structure and payload fields
- S3 prefixes for processed, invalid, and breach-window outputs
- evaluation report schema and field names

---

## Deployment Design

### Kubernetes

The current single stream-processor deployment for the optimized target will be replaced by:

- one deployment for `optimized-hot`
- one deployment for `optimized-cold`

Both deployments will stay in the same namespace and continue using the existing config-map-driven environment model where practical.

### Compute Allocation

`optimized-hot` gets priority for compute:

- higher CPU request/limit
- faster trigger cadence
- lower-latency checkpointing where possible
- tuning focused on Kafka intake and Redis freshness

`optimized-cold` is tuned for throughput rather than immediacy:

- slower trigger cadence
- archive-oriented resources
- separate checkpoint root

### Infrastructure

Additional AWS compute is allowed if needed.

Priority order:

1. give more CPU to `optimized-hot`
2. separate hot/cold deployments
3. increase node capacity if hot still lacks headroom

This means infrastructure upgrades are treated as supporting work, not as a substitute for architecture fixes.

---

## Controller And Evaluation Wiring

The evaluation controller will continue to expose a single `optimized` pipeline target, but internally it must manage two services instead of one.

Controller changes required:

- restart both optimized deployments together
- wait for both optimized services to become ready
- collect latency metrics from the hot service only
- collect archive/count state from the cold service only
- merge hot-owned and cold-owned metrics into one unified optimized evaluation report without changing the external report schema
- continue generating one optimized report

The reporting flow must not require downstream documentation or dashboard changes.

---

## Metrics Ownership

### Hot-owned metrics

Only `optimized-hot` may own and publish:

- average end-to-end latency
- P95/P99 latency
- ingest-to-Redis latency
- hot-batch duration / hot throughput

### Cold-owned metrics

`optimized-cold` may publish:

- processed count
- invalid count
- deduplicated count
- breach count
- cold-batch duration
- archive progress signals

### Guardrail

Cold metrics must never overwrite hot latency metrics.

This is a strict design rule because earlier pipeline behavior showed how easily shared metric registries can corrupt the reported latency picture.

---

## Reliability And Failure Behavior

### If optimized-cold slows down

- S3/archive freshness may degrade
- Redis freshness should remain protected
- the optimized pipeline should still keep the real-time SLA path alive

### If optimized-hot slows down or restarts

- Redis freshness may dip temporarily
- archive processing may continue independently
- controller/reporting should surface the hot-path issue clearly

### Checkpoint Isolation

Hot and cold services must use separate checkpoint locations so:

- restarts do not corrupt the other path
- progress tracking remains independent
- reprocessing and recovery stay predictable

---

## Testing Strategy

The implementation must add or update tests for:

- optimized dual-deployment manifest wiring
- controller support for dual optimized services
- hot-path ownership of latency metrics
- cold-path preservation of processed / invalid / breach outputs
- report compatibility with existing field names

The work should also include verification at runtime:

- optimized normal-load evaluation
- optimized spike-load evaluation
- report comparison against previous output format expectations

---

## Success Criteria

The design is considered successful when:

1. the optimized pipeline still produces the same external outputs and report fields
2. normal-load latency remains under `5s`
3. spike-load latency is materially reduced and becomes a realistic sub-`5s` target
4. hot-path latency is no longer coupled to S3/archive batch work
5. controller and manifests support the split optimized architecture cleanly

---

## Scope Boundaries

Included:

- stream-processing architecture split
- optimized manifests and deployment changes
- controller changes for dual optimized services
- resource tuning and optional compute increase
- metrics ownership cleanup
- hot-path intake-cap changes for spike throughput
- replacement of driver-side Redis writes with partition-local Redis pipelines
- replacement of window-based latest-state selection in the hot path with a cheaper grouped latest-row strategy

Not included:

- redesigning the workload family
- changing report field names or output contracts
- introducing extra internal Kafka topics as part of this implementation

---

## Risks And Tradeoffs

### Main tradeoff

The design adds operational complexity by moving from one optimized deployment to two. That is acceptable because it directly removes the largest source of spike-latency contention.

### Main risks

- controller logic may still assume a single service in subtle places
- output compatibility could regress if hot/cold ownership boundaries are implemented carelessly
- more compute may improve performance but can blur whether the architecture or the hardware caused the gain

### Mitigation

- preserve external output contracts explicitly
- isolate metrics ownership by service
- validate report compatibility before claiming success
- document any infrastructure increase alongside the architecture change

---

## Final Recommendation

Proceed with the split optimized architecture:

- `optimized-hot` for Redis + SLA latency
- `optimized-cold` for S3 + archive/reporting

Keep the external pipeline name as `optimized`, keep outputs compatible, and allow targeted compute increases where the hot service needs them.
