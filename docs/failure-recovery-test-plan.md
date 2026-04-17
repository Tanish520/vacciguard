# Failure Recovery Test Plan

**Date:** 2026-04-17
**Pipeline target:** baseline
**Scenario:** failure-recovery
**Workload duration:** 10 minutes
**Failure injection point:** minute 3 (30% through the workload)
**Post-recovery observation window:** ~7 minutes

---

## Why This Test Exists

The professor's project specification explicitly requires the following (Section 5, Section 6, Phase 6 Step 6.2):

> "Break the system on purpose. Kill a container. Write down: what broke, how long recovery took."
> "Failure detection and auto-recovery."
> "Use metrics: latency, throughput, cost, recovery time."
> "≥ 99.9% pipeline uptime."

This test answers all four requirements with a single controlled experiment.

---

## What We Are Testing

We are testing whether the VacciGuard baseline pipeline can survive an abrupt stream-processor container crash mid-workload and automatically recover without human intervention, without data loss, and without a lasting latency impact.

The stream-processor is the most critical single component in the pipeline. It owns:
- Real-time Redis device state updates (hot query, every 2 seconds)
- S3 cold storage writes (cold query, every 30 seconds)
- The Prometheus metrics endpoint
- Breach detection state in Redis

If it dies, all of these stop. Kafka continues accepting events — it acts as the durability buffer, holding events until the pipeline recovers. Kubernetes detects the pod failure through its Deployment controller and automatically schedules a replacement pod.

---

## Pre-Test Success Criteria

Defined before running. Results will be judged against these thresholds only.

| Criterion | Target | How to measure |
|-----------|--------|----------------|
| Recovery time (pod-kill to both queries active) | **< 120 seconds** | Timestamp difference from pod logs |
| Data durability | **consumer_lag = 0 at run end** | Evaluation report JSON |
| S3 completeness | **≥ 60 processed S3 objects** | `aws s3 ls ... \| wc -l` |
| Post-recovery latency | **avg < 5s within first 3 hot batches** | Stream processor logs |
| Pipeline success | **pipeline_success = true** | Evaluation report JSON |
| Invalid rate | **~1.82% (matches normal run)** | Evaluation report JSON |

> **Note on processed_rate_pct:** The evaluation report will show a low processed_rate_pct (expected ~68%) due to a known metric artifact — the in-memory counter resets to zero when the pod is killed. This is not data loss. S3 object count and consumer_lag are the correct durability metrics. This is explained fully in the analysis section below.

---

## Failure Injection Strategy

### Why minute 3 of a 10-minute run

The failure is injected at minute 3 for one reason: it maximises the post-recovery observation window.

```
0 min ──── 3 min (T_kill) ──── 3:25 min (T_ready) ──── 10 min
  [pre-failure]   [25s dead]        [post-recovery: ~7 minutes]
```

7 minutes of post-recovery operation demonstrates that the pipeline returned to full steady-state, not just that it briefly restarted. This is more valuable than killing at the midpoint (which only gives ~5 minutes of post-recovery time) or killing late (which barely gives time to confirm recovery).

### Expected metric artifact

The in-memory `processed_events` counter lives only in the stream-processor container. When the pod is killed at minute 3, the counter (holding ~17,500 events) is destroyed. The new pod starts from zero. The evaluation controller reads only the new pod's counter.

```
Pre-kill events (0–3 min):     ~18,000 → counter destroyed, written to S3
Post-restart events (3–10 min): ~46,500 → counter = ~45,500, read by controller

Reported processed_rate: ~45,500 / ~66,000 total events ≈ 68%
True processed rate:     ~63,500 / ~66,000 total events ≈ 96%
```

The 109 S3 objects from the previous 12-minute run confirm this pattern — S3 contained the full run's data even though the counter showed only 52.97%. S3 object count is the ground truth, not the counter.

---

## What We Will Measure

### Phase 1: Pre-failure baseline (minute 0 to minute 3)

Confirm the pipeline is in steady state before inducing failure. Observe in Terminal 2 (stream-processor logs):

- Hot batch every ~2 seconds: `Batch N: wrote X latest device states to Redis`
- Cold batch every ~30 seconds: `Batch N summary valid=... processed=...`
- Avg latency in logs: expect 2.3–2.7s
- No error messages

### Phase 2: Failure injection and dead window (minute 3 to T_ready)

At exactly minute 3, run the kill command. Record every timestamp below.

| Variable | Definition | How to get it |
|----------|-----------|---------------|
| **T_kill** | Pod force-deleted | Clock time when you run the kill command |
| **T_pod_gone** | Old pod Terminating | `kubectl get pods -w` in Terminal 1 |
| **T_new_running** | New pod Running | `kubectl get pods -w` in Terminal 1 |
| **T_ready** | Both queries active | Terminal 2 logs: `Stream processor is running with 2 active queries` |
| **T_first_hot** | First Redis write | Terminal 2 logs: `Batch N: wrote X latest device states to Redis` |

**Recovery time = T_ready − T_kill**

### Phase 3: What broke during the dead window

These are the known impacts during the T_kill to T_ready interval. Fill in actual values from your timestamps.

| Component | Impact | Duration |
|-----------|--------|----------|
| Redis device state keys | Not updated — stale data served | T_kill to T_first_hot |
| Prometheus /metrics endpoint | Unreachable — HTTP server dead | T_kill to T_ready |
| S3 cold writes | Paused — at most one 30s batch missed | T_kill to ~T_ready + 30s |
| Hot-path breach detection | active_breaches set not updated | T_kill to T_first_hot |
| Kafka topic | Unaffected — events accumulated | Entire dead window |
| Replay producer | Unaffected — separate Kubernetes Job | Entire dead window |

Kafka buffered approximately `100 eps × [T_ready - T_kill in seconds]` events during the dead window.

### Phase 4: Post-recovery observation (T_ready to end of run)

Confirm full recovery within these bounds:

- Latency returns to <5s: **within first 3 hot batches (6 seconds after T_ready)**
- Consumer lag returns to 0: **within one cold trigger cycle (≤30 seconds after T_ready)**
- Cold batch resumes logging normally
- No Spark exception logs after T_ready

### Phase 5: Final validation

After the evaluation controller uploads the report, record:

| Metric | Expected | Actual |
|--------|----------|--------|
| `consumer_lag_records` | **0** | |
| `pipeline_success` | **true** | |
| `avg_end_to_end_latency_seconds` | 2.3–3.5s | |
| `p95_end_to_end_latency_seconds` | < 5.0s | |
| `processed_rate_pct` | ~68% (metric artifact) | |
| `invalid_rate_pct` | ~1.82% | |

Then run the S3 verification:

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/failure-recovery/<run-id>/processed/ \
  --recursive | wc -l
```

**Expected: ≥ 60 objects.**

Basis: previous 12-minute run produced 109 objects. A 10-minute run is 10/12 = 83% of that duration, so expected ≥ 90 objects. The minimum threshold of 60 accounts for the 3-minute pre-kill window being absent from S3 if checkpoints hadn't committed yet (conservative lower bound).

---

## Exact Commands

Open four terminal windows before starting.

**Terminal 1 — watch pods:**
```bash
kubectl -n vacciguard get pods -w
```

**Terminal 2 — follow stream-processor logs:**
```bash
kubectl -n vacciguard logs -f deployment/stream-processor
```

**Terminal 3 — run the evaluation:**
```bash
WORKLOAD_DURATION_MINUTES=10 \
bash scripts/run-aws-evaluation-controller.sh \
  baseline \
  failure-recovery \
  failure-recovery-$(date -u +%Y%m%dT%H%M%SZ)
```

**Terminal 4 — kill command (run at minute 3):**
```bash
kubectl -n vacciguard delete pod -l app=stream-processor --grace-period=0 --force
```

Start a stopwatch when Terminal 2 shows `Stream processor is running with 2 active queries`. At exactly 180 seconds, run the Terminal 4 command and note the clock time.

---

## Observation Log (fill in during the test)

```
T0  (stream ready):        ________________Z
T_kill (pod deleted):      ________________Z
T_pod_gone:                ________________Z
T_new_running:             ________________Z
T_ready (2 queries active):________________Z
T_first_hot_batch:         ________________Z

Recovery time  (T_ready - T_kill):         ___ seconds
Hot batch lag  (T_first_hot - T_ready):    ___ seconds
Dead window    (T_ready - T_kill):         ___ seconds
Kafka buffered (100 eps × dead window):    ___ events

Pre-failure avg latency  (last log before T_kill):    ___ s
Post-recovery avg latency (first 3 hot batches):      ___ s

Consumer lag at run end:       ___
S3 processed object count:     ___
```

---

## How Results Map to Professor's Requirements

| Requirement (from PDF) | How this test answers it |
|------------------------|--------------------------|
| "Kill a container" (Phase 6 Step 6.2) | Force-delete stream-processor pod at minute 3 |
| "What broke?" (Phase 6 Step 6.2) | Phase 3 table: Redis stale, metrics dark, S3 paused, breach detection halted |
| "How long recovery took?" (Phase 6 Step 6.2) | Recovery time = T_ready − T_kill, target < 120s |
| "Failure detection and auto-recovery" (Section 6) | Kubernetes Deployment `Recreate` strategy — no human intervention needed |
| "Recovery time metric" (Section 8) | Directly measured in seconds from pod logs |
| "≥ 99.9% uptime" (Section 5) | MTTR measured → MTTF threshold derived → see calculation below |
| "Trigger failure, show recovery" (Phase 9 Demo) | Live: kill pod, watch replacement appear, confirm latency normalises |

---

## 99.9% Uptime Derivation

This test measures MTTR (Mean Time To Recover). The 99.9% uptime claim is derived from MTTR as follows:

```
Uptime = MTTF / (MTTF + MTTR)

For ≥ 99.9% uptime:
  MTTF / (MTTF + MTTR) ≥ 0.999
  MTTF ≥ 999 × MTTR
```

Substituting MTTR = 25 seconds (from previous run; update with new measurement):
```
  MTTF ≥ 999 × 25 = 24,975 seconds ≈ 6.9 hours
```

**Interpretation:** As long as the stream-processor pod fails less than once every 6.9 hours, the pipeline meets the ≥ 99.9% uptime SLA. On EKS with stable workloads, pod failures are driven by hardware faults or application crashes — these occur on the order of days to weeks in practice, far exceeding the 6.9-hour threshold.

The 12-minute test run itself is not representative of steady-state uptime (failure was deliberately injected once in 12 minutes = artificially high failure rate). The correct metric for the SLA claim is MTTR, not uptime percentage during the test window.
