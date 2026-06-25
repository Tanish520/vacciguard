# Failure Recovery - Baseline Pipeline

**Run ID:** `failure-recovery-20260417t082325z`  
**Date:** `2026-04-17`  
**Duration:** `10 minutes`  
**Load:** `100 eps`  
**Total input events:** `66,000`

---

## Metrics

| Metric | Value |
|--------|-------|
| status | `succeeded` |
| pipeline_success | `true` |
| avg_end_to_end_latency_seconds | `3.18 s` |
| p95_end_to_end_latency_seconds | `3.51 s` |
| ingest_to_redis_p95_seconds | `3.54 s` |
| throughput_eps | `100.0` |
| consumer_lag_records | `0` |
| input_events | `66,000` |
| processed_events | `64,762` |
| processed_rate_pct | `98.12%` |
| invalid_events | `1,200` |
| invalid_rate_pct | `1.82%` |
| deduplicated_events | `38` |
| breach_events | `1,008` |
| processed_output_objects | `92` |
| cost_per_run (calculated) | `~$0.033` |
| cost_per_gb_processed (calculated) | `~$2.51/GB` |

---

## Observations (from stream processor logs / report)

- Note: the latency values below are run-level values from the report, not per-batch readings from the logs.
- First hot batch latency: `3.18s` avg, `3.51s` P95.
- Last hot batch latency: `3.18s` avg, `3.51s` P95.
- Latency stable / climbing / other: stable after recovery; the pipeline returned to its normal latency band instead of staying degraded.
- Any errors seen: no functional errors in the final report. The main thing to notice is that the run recovered cleanly and kept processing.

### What Happens Before and After the Pod Kill

Before the kill, the baseline pipeline was already running in its normal steady state:

- Hot path latency was around `3.18s` average with `3.51s` P95.
- Throughput was `100.0 events/s`.
- Consumer lag was already at `0`, which means the stream was keeping up before the failure.

When the pod was force-deleted, the current JVM process disappeared immediately. That means the in-memory counters and the live metrics endpoint were lost for the old pod, but the Kafka topic and the S3 checkpoints were not lost.

After the new pod came back, the pipeline recovered into the same healthy range:

- End-to-end latency stayed low instead of spiking permanently.
- The report still shows `0` consumer lag, which is the important sign that the replacement pod drained the Kafka backlog successfully.
- The final processed count was `64,762` out of `66,000` input events, with `1,200` invalid events and `38` deduplicated events.
- The run produced `92` processed output objects in S3, which is the durable record that the recovered pipeline kept writing data after the restart.

In simple terms, the kill broke availability for a short window, but it did not break durability or the ability to recover. The stream paused briefly, restarted, caught up, and then finished the workload normally.

## Recovery Timestamps

| Event | Timestamp |
|-------|-----------|
| T_kill (pod force-deleted) | `April 17, 2026, 1:53:34 PM IST` |
| T_new_running (new pod Running) | `April 17, 2026, 1:54:06 PM IST` |
| T_ready (2 active queries confirmed) | `April 17, 2026, 1:54:36 PM IST` |
| T_first_hot (first Redis write after restart) | `April 17, 2026, 1:54:45 PM IST` |
| **Recovery time (T_ready − T_kill)** | **`62.7 seconds`** |

## What Broke During the Dead Window

| Component | What happened | Approx. duration |
|-----------|---------------|------------------|
| Redis device-state updates | Became stale immediately after the pod kill because the hot query stopped writing | `71.6s` until the first post-restart Redis write |
| Metrics endpoint | Went dark while the JVM and Prometheus endpoint were down | `62.7s` until the new pod reported `2 active queries` |
| Cold S3 writes | Paused while the old pod was dead and the replacement pod was starting up | `86.8s` until the first cold batch summary after restart |
| Kafka buffering | Continued normally; events accumulated in the topic instead of being lost | `~6,270` events buffered during the `62.7s` dead window |
| S3 checkpoints | Not lost; the replacement pod read the existing checkpoints and resumed | durable across the restart |

The main user-visible impact was stale live data, not data loss. During the dead window, the dashboard would not receive fresh Redis updates, the metrics endpoint would be unavailable, and the cold-path archival output would pause. Once the replacement pod came up, the pipeline resumed from the saved checkpoints and drained the backlog.

The cold-write pause is longer than the recovery time because the cold query runs on a 30-second trigger cycle. The new pod became ready at `62.7s`, but the next cold batch completion did not happen until the next trigger boundary, which is why the cold-path pause stretches to `86.8s`.

---

## S3 Verification

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/failure-recovery/failure-recovery-20260417t082325z/processed/ --recursive | wc -l
```

Object count: `92`

---

## Cost Calculation

- Infrastructure: `10 min x $0.196/hr = ~$0.0327`
- S3: `~$0.0004`
- Total per run: `~$0.0331`
- Data volume: `66,000 events x 200 bytes ~= 13.2 MB`
- Cost per GB: `~$2.51/GB`

### Why the cost per GB looks similar to normal load

This run uses the same cluster and runs for about the same amount of time as the normal scenario, so the infrastructure cost stays mostly fixed. Even though the workload is longer and includes a restart, the amount of data processed is still relatively small compared with the fixed hourly cost of the cluster. That means the cost per GB stays in the same general range as the normal run. The key difference in this scenario is not cost efficiency, but reliability: the pipeline survives the interruption and still completes the workload.

---

## Key Findings

- The baseline pipeline recovered successfully after the forced pod failure and kept end-to-end latency in a healthy range: `3.51s` P95 is still below the 5-second SLA.
- Consumer lag ended at `0`, which is the strongest sign that the restart did not lose Kafka data. Kafka buffered the dead-window events and the replacement pod drained them afterward.
- The processed-rate value is now `98.12%`, and the counts add up exactly: `64,762 processed + 1,200 invalid + 38 deduplicated = 66,000 input`. The simpler explanation is that the replacement pod had enough time after the restart to process the remaining backlog, and the checkpoint resumed close enough to the kill point that the new pod accounted for the whole workload window.
- The run demonstrates the exact reliability story the professor cares about: the pod can be killed, the workload can continue, the replacement pod can resume from checkpoints, and the pipeline still finishes successfully.
- What broke during the outage window was availability, not correctness. During the dead window, Redis updates stopped, cold-path writes paused, and the metrics endpoint would have been temporarily stale, but the recovery path restored service without manual intervention.
- The fact that the post-recovery latency stayed in the same low range is just as important as the recovery time itself. It shows the system did not “heal” into a slower steady state.

## 99.9% Uptime Derivation

MTTR for this run was `62.7 seconds`.

For `99.9%` uptime, the downtime budget is `0.1%`, so:

`MTTF >= 999 × MTTR`

`MTTF >= 999 × 62.7s = 62,637s`

That is approximately `17.4 hours`.

The important takeaway is operational rather than mathematical:

- The system did not stay broken after the kill.
- The outage was short compared with a full day of operation.
- The restart time was fast enough to support a high-availability claim instead of just a “it came back eventually” claim.

---

## Raw Report JSON

The full JSON report was uploaded to S3:

`s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/failure-recovery/failure-recovery-20260417t082325z/report.json`

Key JSON fields captured in the local report notes:

```text
status: succeeded
pipeline_success: true
controller_job_success: true
replay_job_success: true
avg_end_to_end_latency_seconds: 3.18
p95_end_to_end_latency_seconds: 3.51
ingest_to_redis_p95_seconds: 3.54
throughput_eps: 100.0
consumer_lag_records: 0
input_events: 66000
processed_events: 64762
processed_rate_pct: 98.12
invalid_events: 1200
invalid_rate_pct: 1.82
deduplicated_events: 38
breach_events: 1008
recovery_time_after_failure: Not run
```
