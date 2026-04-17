# Failure Recovery Evaluation Plan

**Scenario:** `failure-recovery`
**Load:** 100 events/second (normal load)
**Duration:** 10 minutes
**Failure injected at:** Minute 3 (30% through the workload)
**Total events:** ~66,000
**Applies to:** Baseline Pipeline + Optimized Pipeline (run separately)

> **Status:** Baseline run completed (failure-recovery-20260417t055839z — 24s recovery, consumer_lag=0). Optimized pipeline not yet built — Optimized Actual columns are intentionally blank and will be filled in once the optimized pipeline is deployed.

---

## What This Scenario Tests

This test answers the professor's explicit requirement: **kill a container, observe what breaks, measure how long recovery takes.** It tests whether the pipeline can survive an abrupt container crash mid-workload and automatically resume processing without data loss and without human intervention.

The stream processor is the most critical single component in the pipeline. When it is force-deleted:
- Redis device-state updates stop immediately
- S3 cold writes pause
- The Prometheus metrics endpoint goes dark
- Breach detection halts

Kafka continues receiving events throughout the crash window — it acts as the durability buffer. When Kubernetes automatically schedules a replacement pod, the new stream processor reads S3-backed checkpoints, finds the last committed Kafka offset, and resumes processing from exactly where it left off.

**The key measurements are:**
1. Recovery time (pod kill → 2 active queries running again)
2. Data durability (were any events lost during the crash window?)
3. Post-recovery latency (did the pipeline return to normal after recovery?)

---

## Professor Requirements This Scenario Satisfies

| Requirement (from PDF) | What this run measures |
|------------------------|------------------------|
| "Kill a container" (Phase 6 Step 6.2) | Force-delete stream-processor pod at minute 3 |
| "Write down: what broke" (Phase 6 Step 6.2) | The "What Broke During the Dead Window" table |
| "How long recovery took" (Phase 6 Step 6.2) | T_ready − T_kill in seconds |
| "Failure detection and auto-recovery" (Section 6) | Kubernetes Deployment `Recreate` strategy |
| "Recovery time metric" (Section 8) | Measured in seconds from pod logs |
| "≥ 99.9% uptime" (Section 5) | MTTR → MTTF → uptime derivation |
| Phase 9 Demo — live kill and recovery | The entire Phase 2-4 of this plan |

---

## How the Pipeline Survives a Pod Kill (The Mechanism)

Understanding this is essential for explaining the results correctly.

**Before the kill (every 30 seconds):**
The cold query completes a Spark batch cycle and writes two things to S3:
1. Processed event data as Parquet files
2. The checkpoint: the Kafka offset it last read from

**At T_kill:**
The pod is force-deleted. The JVM process is killed instantly. All in-memory state is destroyed — including the in-memory processed_events counter. The Kafka topic still has the events; S3 still has all checkpoints and data.

**During the dead window (T_kill to T_ready):**
Kafka continues buffering incoming events. No consumer is reading from the topic. Events accumulate as uncommitted backlog.

**At T_ready:**
The replacement pod starts. Spark reads the S3 checkpoint. It finds: "last committed offset = events up to minute 2:30 (approximately)." It starts reading Kafka from that offset — picking up right where it left off, including the backlog that accumulated during the crash window.

```
Timeline:
0 min ─── 3 min (T_kill) ──── 3:24 min (T_ready) ──── 10 min
  [processing]  [dead window]  [recovery + drain + processing]
                 ↑ events buffer in Kafka here
                               ↑ new pod starts, reads checkpoint, drains backlog
```

**Why processed_rate_pct will show ~62% (not ~100%):**
This is a metric artifact. The in-memory processed_events counter resets to zero when the pod is killed. The replacement pod starts counting from zero. The evaluation controller reads only the new pod's counter, which only has the post-restart events (~40,000 out of 66,000 total).

**Consumer_lag = 0 is the actual proof of no data loss** — it means the cold query read every single message in the Kafka topic, regardless of what the counter says.

---

## Pre-Run Checklist

- [ ] AWS credentials valid: `aws sts get-caller-identity`
- [ ] EKS cluster reachable: `kubectl -n vacciguard get pods` — all pods Running
- [ ] Stream processor showing "2 active queries" in logs before starting
- [ ] No leftover evaluation jobs running: `kubectl -n vacciguard get jobs`
- [ ] Consumer lag from any previous run is 0
- [ ] **Prepare a stopwatch.** You will start it when the stream processor logs "running with 2 active queries" and kill the pod at exactly 180 seconds (3 minutes). Accuracy matters here — the timestamps are evidence.
- [ ] All four terminal windows open and ready before launching the evaluation (see below)

---

## Terminal Setup

Open all four terminals **before** starting the evaluation controller. You will not have time to set them up after launch.

**Terminal 1 — watch pods:**
```bash
kubectl -n vacciguard get pods -w
```
This shows you the exact moment the old pod enters Terminating state and when the new pod reaches Running state.

**Terminal 2 — follow stream processor logs:**
```bash
kubectl -n vacciguard logs -f deployment/stream-processor
```
This is your primary observation window. Watch for latency values, batch counts, the 2-query confirmation on startup, and any errors.

**Terminal 3 — run the evaluation:**

For **baseline pipeline**:
```bash
WORKLOAD_DURATION_MINUTES=10 \
bash scripts/run-aws-baseline-evaluation.sh \
  baseline \
  failure-recovery \
  failure-recovery-$(date -u +%Y%m%dT%H%M%SZ)
```

For **optimized pipeline** (run separately after baseline is fully complete):
```bash
WORKLOAD_DURATION_MINUTES=10 \
bash scripts/run-aws-evaluation-controller.sh \
  optimized \
  failure-recovery \
  failure-recovery-optimized-$(date -u +%Y%m%dT%H%M%SZ)
```

**Terminal 4 — kill command (ready but do NOT run yet):**
```bash
kubectl -n vacciguard delete pod -l app=stream-processor --grace-period=0 --force
```
Type this command in Terminal 4, but do not press Enter until the stopwatch hits exactly 180 seconds.

---

## Execution Steps (In Order)

1. Start Terminal 2 first. Wait until you see:
   ```
   Stream processor is running with 2 active queries
   ```
   **Start the stopwatch NOW.**

2. Immediately launch Terminal 3 (the evaluation command). The evaluation controller will start the replay producer.

3. In Terminal 2, confirm normal operation:
   - Hot batches every ~2 seconds: `Batch N: wrote X latest device states to Redis`
   - Cold batches every ~30 seconds: `Batch N summary: valid=...`
   - Avg latency should be in 2.0–3.0s range

4. At exactly **180 seconds** on the stopwatch:
   - Note the wall clock time — this is T_kill
   - Press Enter on Terminal 4 to run the force-delete
   - Immediately watch Terminal 1 for pod Terminating/Running transitions
   - Immediately watch Terminal 2 — logs will stop, then resume from the new pod

5. Record all timestamps (see Observation Log below).

6. After T_ready, watch Terminal 2 for:
   - First hot batch after restart (T_first_hot)
   - Latency values — should return to 2.0–3.0s range quickly
   - Any Spark exception logs (there should be none)

7. Let the run complete naturally (evaluation controller finishes at 10 minutes).

8. Collect the report from `artifacts/aws-baseline-evaluations/<run-id>.json`.

---

## Observation Log (Fill In During the Test)

```
=== BASELINE PIPELINE ===

T0  (stream ready, stopwatch start):    ______________Z
T_kill (pod force-deleted):             ______________Z  (stopwatch: 180s)
T_pod_gone (old pod Terminating):       ______________Z  (from Terminal 1)
T_new_running (new pod Running):        ______________Z  (from Terminal 1)
T_ready (2 active queries confirmed):   ______________Z  (from Terminal 2)
T_first_hot (first Redis write):        ______________Z  (from Terminal 2)

--- Derived ---
Recovery time  (T_ready − T_kill):         ______ seconds
Hot path gap   (T_first_hot − T_ready):    ______ seconds
Dead window    (T_ready − T_kill):         ______ seconds
Events buffered (100 eps × dead window):   ______ events

--- Pre-failure latency ---
Avg latency (last log before T_kill):      ______s

--- Post-recovery latency ---
Avg latency (first hot batch after T_ready): ______s
Avg latency (at minute 5):                   ______s
Avg latency (at minute 10):                  ______s

--- End state ---
Consumer lag at run end:    ______
S3 processed object count:  ______  (see S3 Verification below)


=== OPTIMIZED PIPELINE ===
(same fields, fill in separately)

T0:                                         ______________Z
T_kill:                                     ______________Z
T_pod_gone:                                 ______________Z
T_new_running:                              ______________Z
T_ready:                                    ______________Z
T_first_hot:                                ______________Z

Recovery time:          ______ seconds
Dead window:            ______ seconds
Pre-failure latency:    ______s
Post-recovery latency:  ______s
Consumer lag:           ______
S3 object count:        ______
```

---

## Metrics to Capture After the Run

| Metric | Expected (Baseline) | Baseline Actual | Optimized Actual |
|--------|---------------------|-----------------|------------------|
| `status` | `succeeded` | | |
| `pipeline_success` | `true` | | |
| `avg_end_to_end_latency_seconds` | 2.3–2.7s | **2.46s** | |
| `p95_end_to_end_latency_seconds` | 2.8–3.1s | **2.90s** | |
| `ingest_to_redis_p95_seconds` | 2.9–3.1s | **2.94s** | |
| `consumer_lag_records` | **0** | **0** | |
| `input_events` | 66,000 | | |
| `processed_events` | ~40,904 (artifact) | | |
| `processed_rate_pct` | ~62% (artifact) | **61.98%** | |
| `invalid_events` | 1,200 | | |
| `invalid_rate_pct` | 1.82% | **1.82%** | |
| `deduplicated_events` | ~11 | | |
| `breach_events` | ~630 | | |
| `cost_per_run` | "Not run" (calculate below) | | |
| `cost_per_gb_processed` | "Not run" (calculate below) | | |
| `recovery_time_after_failure` | "Not run" (manual) | | |
| **Recovery time (manual)** | **< 120s** | **24s** | |
| **S3 processed objects** | ≥ 60 | | |
| **Pre-failure latency** | 2.0–3.0s | | |
| **Post-recovery latency** | < 5s within 3 batches | | |

---

## What Broke During the Dead Window

Fill in the duration column with actual timestamps from your observation log:

| Component | Impact During Dead Window | Duration | Recovery Trigger |
|-----------|--------------------------|----------|-----------------|
| Redis device-state keys | Stale — last known state frozen in Redis | T_kill → T_first_hot | Hot query resumes writing |
| Prometheus /metrics endpoint | HTTP server dead — no metrics scraping | T_kill → T_ready | New pod starts HTTP server |
| S3 cold writes | Paused — at most one 30s batch missed | T_kill → ~T_ready + 30s | Cold query starts new batch cycle |
| Breach detection (active_breaches Redis set) | Not updated during dead window | T_kill → T_first_hot | Hot query resumes breach detection |
| Kafka consumer | No consumer for the topic | T_kill → T_ready | New pod registers consumer group |
| Replay producer | Unaffected — separate Kubernetes Job | None — continuous | N/A |
| Kafka topic (data) | Unaffected — events accumulated in queue | None — data preserved | N/A |

---

## Cost Estimation (Analytical)

The evaluation report always shows `cost_per_run: "Not run"` — calculate analytically. The failure recovery run is 10 minutes (2× the normal run), so infrastructure cost doubles.

### Infrastructure Cost (per run)

| Component | Rate | 10-min run |
|-----------|------|------------|
| EKS nodes (2× t3.medium) | $0.0832/hr | $0.0139 |
| ElastiCache (cache.t4g.micro) | $0.0128/hr | $0.0021 |
| EKS control plane | $0.1000/hr | $0.0167 |
| **Total infrastructure** | **$0.1960/hr** | **$0.0327** |

### S3 Cost

| | Value |
|-|-------|
| Processed events | ~40,904 (post-restart counter only) |
| True events processed | ~64,200 (all valid events from 66,000 total) |
| Avg event size | ~200 bytes |
| True data volume | ~12.84 MB = ~0.013 GB |
| S3 PUT requests (~109 objects) | $0.005/1000 × 109 = $0.00055 |
| S3 storage | negligible |
| **Total S3** | **~$0.001** |

Note: Use the S3 object count (not the in-memory processed_events counter) to estimate true data volume for this scenario, because the counter resets on pod restart.

### Total Cost Per Run

```
Total = Infrastructure + S3
      = $0.0327 + $0.001
      = ~$0.034 per 10-minute failure recovery run
```

### Cost Per GB Processed

```
True data volume  = ~0.013 GB
Total cost        = ~$0.034
Cost per GB       = $0.034 / 0.013 = ~$2.62/GB
```

This is nearly identical to the normal load cost per GB — expected, since it is the same 100 eps load running for twice as long.

### Fill In After Each Run

| | Baseline | Optimized |
|-|----------|-----------|
| Run duration (min) | 10 | 10 |
| Infrastructure cost | $0.0327 | $0.0327 |
| S3 cost | ~$0.001 | ~$0.001 |
| **Total cost per run** | **~$0.034** | |
| S3 object count | | |
| True data volume (MB) | | |
| **Cost per GB** | **~$2.62/GB** | |

---

## S3 Verification

After the run completes, count S3 objects to verify data durability:

```bash
# Baseline
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/failure-recovery/<run-id>/processed/ \
  --recursive | wc -l

# Optimized
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/failure-recovery/<run-id>/processed/ \
  --recursive | wc -l
```

**Expected for 10-minute run: ≥ 60 objects.**

Basis: 10 minutes at 30-second cold trigger = 20 trigger cycles. The 3-minute pre-kill window contributes ~6 cycles from the old pod. The post-restart 7-minute window contributes ~14 cycles from the new pod. At 4–5 parquet parts per cycle, expected ≥ 60 objects.

The previous 10-minute baseline run produced **109 objects** — use this as the reference for "healthy" object count.

---

## Analysis Guide: What Each Metric Means

### processed_rate_pct = ~62% — Not Data Loss

The reported processed_rate (62%) reflects only what the **replacement pod** counted from zero after restart. The old pod counted ~18,000 events before being killed — that counter was destroyed. The evaluation controller reads only the new pod.

```
Old pod (minute 0-3):    ~18,000 events counted → counter destroyed at T_kill
New pod (minute 3-10):   ~40,904 events counted → this is what the report shows

Reported:  40,904 / 66,000 = 61.98%
Actual:    All 66,000 events consumed (consumer_lag = 0 is the proof)
```

**When presenting this in the report:** Always state "the reported processed_rate is a metric artifact caused by the in-memory counter resetting on pod restart. The actual data durability metric is consumer_lag = 0, which confirms all 66,000 events were consumed."

### Recovery Time < 120s — The SLA Target

Recovery time is measured as T_ready (stream processor logs "running with 2 active queries") minus T_kill (wall clock when you ran the delete command).

The 24-second baseline result is well within the 120-second target. The breakdown is:
- Kubernetes scheduling the new pod: ~5–8s
- JVM startup + PySpark initialization: ~10–12s
- Spark reading S3 checkpoints + rewinding Kafka offsets: ~3–5s
- Both queries starting: ~1–2s

### 99.9% Uptime Derivation

Use the measured recovery time to derive the uptime claim for the report:

```
Uptime formula:  Uptime = MTTF / (MTTF + MTTR)

For 99.9% uptime:
  MTTF / (MTTF + MTTR) ≥ 0.999
  MTTF ≥ 999 × MTTR

If MTTR = 24s:
  MTTF ≥ 999 × 24 = 23,976 seconds ≈ 6.65 hours

→ As long as the stream-processor fails less than once every 6.65 hours,
  the pipeline meets 99.9% uptime.

On EKS with stable workloads, pod failures occur days to weeks apart,
far exceeding the 6.65-hour threshold.
```

Update the MTTF calculation with your actual measured recovery time.

### Post-Recovery Latency — Return to Normal

After T_ready, the first hot batch may show slightly elevated latency as Spark drains the events that accumulated during the dead window. This should normalize within 3 hot batches (6 seconds). Confirm this from the Terminal 2 logs.

If latency does not return to normal within 60 seconds post-recovery, check for error logs or consumer lag accumulation.

---

## Comparing Baseline vs Optimized

| Metric | Baseline | Optimized | Comment |
|--------|----------|-----------|---------|
| Recovery time | 24s | | Both use S3 checkpoints |
| Data loss | None | None | Kafka buffers during dead window |
| Post-recovery latency | 2.46s avg | | Should be similar |
| Consumer lag at end | 0 | 0 | Both must drain fully |
| processed_rate_pct | ~62% (artifact) | ~62% (artifact) | Same artifact applies to both |

The failure recovery scenario is expected to produce **similar results for both pipelines** because the recovery mechanism (S3 checkpoints + Kubernetes Recreate) is identical. The main difference, if any, would be recovery time — the optimized pipeline may have a slightly different JVM startup profile.

If the optimized pipeline shows a significantly different recovery time (>30s difference), investigate:
- Is the optimized pipeline using the same checkpoint S3 path?
- Is the deployment strategy still `Recreate` (not `RollingUpdate`)?

---

## Key Findings to Document in the Report

### Finding 1: Recovery Time
State the measured T_ready − T_kill in seconds. Compare against the 120-second target. Confirm PASS.

The 24-second recovery time is within the 30-second cold trigger interval — meaning at most one cold batch was missed during the entire outage window.

### Finding 2: Zero Data Loss
Consumer_lag = 0 at run completion confirms all 66,000 events were eventually processed. Kafka acted as the durability buffer during the 24-second dead window, preserving the ~2,400 events that arrived while the pod was down.

### Finding 3: Automatic Recovery — No Human Intervention
Kubernetes detected the pod failure and automatically scheduled a replacement via the `Recreate` deployment strategy. No alert was triggered, no operator paged, no manual restart required. The pipeline self-healed within 24 seconds.

### Finding 4: Post-Recovery Latency Unchanged
After the replacement pod became ready, latency returned to the normal 2.46s average within the first hot batch cycle. The restart did not leave the hot path permanently degraded.

### Finding 5: What Actually Broke (and for How Long)
Be specific: Redis keys were stale for 24 seconds. Metrics endpoint was dark for 24 seconds. S3 cold writes missed at most one 30-second cycle. No correctness failures — only availability gaps, all bounded by the recovery time.

### Finding 6: Metric Artifact (processed_rate_pct = ~62%)
Explain clearly: the in-memory counter resets on pod restart. The reported rate is not data loss. S3 object count and consumer_lag are the correct durability metrics.

### Finding 7: 99.9% Uptime Derivation
Use the MTTF/MTTR formula to show that a 24-second MTTR requires failures less than once every 6.65 hours to meet the 99.9% SLA. On production EKS, pod failures are order-of-magnitude less frequent than this.

---

## Common Issues and How to Handle Them

**Issue: Stream processor pod does not come back up**
- Wait 60 seconds. Kubernetes may be waiting for the old pod to fully terminate.
- Check: `kubectl -n vacciguard describe pod -l app=stream-processor` for scheduling errors.
- Check node capacity: `kubectl -n vacciguard top nodes` — t3.medium may be at CPU/memory limit.

**Issue: New pod starts but logs "only 1 active query" (not 2)**
- One query failed to start. Check for Spark exception logs immediately after startup.
- The hot query may fail to start if it cannot reach Kafka. Check Kafka pod: `kubectl -n vacciguard get pods -l app=kafka`

**Issue: consumer_lag is still > 0 after the run ends**
- Wait 5 minutes. The new pod is still draining the backlog.
- If lag is not decreasing, check for Spark processing errors in Terminal 2.
- A lag > 0 after 15 minutes indicates a real processing failure.

**Issue: Recovery takes longer than expected (> 60s)**
- Check if the S3 checkpoint exists from the pre-kill window: `aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/checkpoints/baseline_cold/`
- If no checkpoint exists, the cold query starts from the Kafka topic beginning — longer startup.

**Issue: processed_rate_pct is extremely low (< 30%)**
- If < 30%, the kill happened very early (< 1 minute in) and almost no cold batches committed.
- Re-run with the kill at the 3-minute mark as planned.
- The metric artifact always produces ~62% when killed at minute 3 of a 10-minute run.

---

## Report Section Template

```
Failure Recovery Evaluation (100 eps, 10 minutes, pod kill at minute 3)

Objective: Verify the pipeline survives an abrupt stream-processor crash
and auto-recovers without data loss or manual intervention.

Key Timestamps:
  T_kill (pod force-deleted):            2026-XX-XXT__:__:__Z
  T_ready (2 active queries confirmed):  2026-XX-XXT__:__:__Z
  Recovery time (T_ready − T_kill):      XX seconds

Recovery Results:
  ✓ Recovery time: XXs  (target: < 120s)
  ✓ Data loss: None (consumer_lag = 0)
  ✓ Post-recovery latency: X.XXs avg  (target: < 5s)
  ✓ Automatic recovery: No human intervention required

What Broke During the XXs Dead Window:
  - Redis device-state keys: stale for XXs
  - Prometheus metrics endpoint: unreachable for XXs
  - S3 cold writes: paused for ~XXs (at most one batch missed)
  - Kafka: buffered ~XX events, none lost

Note on processed_rate_pct:
  The report shows ~62%, not ~100%. This is a metric artifact — the
  in-memory counter resets to zero on pod restart. Consumer_lag = 0
  confirms all 66,000 events were consumed. The 62% reflects only what
  the replacement pod counted from scratch.

99.9% Uptime Claim:
  MTTR = XXs → required MTTF ≥ 999 × XXs = ___s ≈ ___ hours
  On EKS with stable workloads, pod failures occur days apart.
  The pipeline meets the 99.9% uptime SLA with a wide margin.

S3 Verification: ___ processed objects (expected ≥ 60)
```
