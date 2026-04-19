# AWS Baseline Evaluation baseline-failure-recovery-20260409t122500z

- Run ID: `baseline-failure-recovery-20260409t122500z`
- AWS account: `347038623570`
- Region: `ap-south-1`
- Cluster: `vacciguard-baseline-eks`
- Namespace: `vacciguard`
- Scenario: `failure-recovery`
- Workload family version: `evaluation-workload-v1`
- Workload file: `/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/data/workloads/evaluation/v1/failure-recovery.events.ndjson`
- Workload source: `s3`
- Workload size bytes: `16046100`
- Declared input events: `79200`
- Configured replay rate: `100.0`
- Fault model: `{"offset_minutes": 6, "type": "stream-processor-restart"}`
- Kafka topic: `vacciguard-eval-baseline-failure-recovery-20260409t122500z`
- S3 prefix: `s3://vacciguard-baseline-data/evaluations/baseline-failure-recovery-20260409t122500z/`
- Redis reset: `true`
- Report written at: `2026-04-09T07:12:07Z`

## Redis Reset

```text
cleared_device_status_keys=30
cleared_active_breaches=1
```

## Fault Injection

```text
completed:stream-processor-restart:delay=360s
```

## Evaluation Summary

| Metric | Baseline |
|---|---:|
| Workload family version | evaluation-workload-v1 |
| Scenario | failure-recovery |
| Configured replay rate | 100.00 events/s |
| Avg end-to-end latency | 11.82 s |
| P95 latency | 53.71 s |
| Throughput | 100.00 events/s |
| 10x spike success/failure | Not run |
| Recovery time after failure | Not run |
| Input events | 79200 |
| Processed events | 39741 |
| Invalid events | 1440 |
| Deduplicated events | 0 |
| Breach events | 563 |
| Cost per run | Not run |
| Cost per GB processed | Not run |

## Evaluation Metrics JSON

```json
{
  "avg_end_to_end_latency_seconds": 11.82,
  "breach_events": 563,
  "configured_events_per_second": 100.0,
  "cost_per_gb_processed": "Not run",
  "cost_per_run": "Not run",
  "deduplicated_events": 0,
  "input_events": 79200,
  "invalid_events": 1440,
  "p95_end_to_end_latency_seconds": 53.71,
  "processed_events": 39741,
  "recovery_time_after_failure": "Not run",
  "scenario": "failure-recovery",
  "spike_result": "Not run",
  "throughput_eps": 100.0,
  "workload_family_version": "evaluation-workload-v1"
}
```

## Replay Logs

```text
2026-04-09T07:09:52Z INFO Sent 69450/79200  actual 100.0 eps
2026-04-09T07:09:52Z INFO Sent 69500/79200  actual 100.0 eps
2026-04-09T07:09:53Z INFO Sent 69550/79200  actual 100.0 eps
2026-04-09T07:09:53Z INFO Sent 69600/79200  actual 100.0 eps
2026-04-09T07:09:54Z INFO Sent 69650/79200  actual 100.0 eps
2026-04-09T07:09:54Z INFO Sent 69700/79200  actual 100.0 eps
2026-04-09T07:09:55Z INFO Sent 69750/79200  actual 100.0 eps
2026-04-09T07:09:55Z INFO Sent 69800/79200  actual 100.0 eps
2026-04-09T07:09:56Z INFO Sent 69850/79200  actual 100.0 eps
2026-04-09T07:09:56Z INFO Sent 69900/79200  actual 100.0 eps
2026-04-09T07:09:57Z INFO Sent 69950/79200  actual 100.0 eps
2026-04-09T07:09:57Z INFO Sent 70000/79200  actual 100.0 eps
2026-04-09T07:09:58Z INFO Sent 70050/79200  actual 100.0 eps
2026-04-09T07:09:58Z INFO Sent 70100/79200  actual 100.0 eps
2026-04-09T07:09:59Z INFO Sent 70150/79200  actual 100.0 eps
2026-04-09T07:09:59Z INFO Sent 70200/79200  actual 100.0 eps
2026-04-09T07:10:00Z INFO Sent 70250/79200  actual 100.0 eps
2026-04-09T07:10:00Z INFO Sent 70300/79200  actual 100.0 eps
2026-04-09T07:10:01Z INFO Sent 70350/79200  actual 100.0 eps
2026-04-09T07:10:02Z INFO Sent 70400/79200  actual 99.9 eps
2026-04-09T07:10:02Z INFO Sent 70450/79200  actual 99.9 eps
2026-04-09T07:10:03Z INFO Sent 70500/79200  actual 99.9 eps
2026-04-09T07:10:03Z INFO Sent 70550/79200  actual 99.9 eps
2026-04-09T07:10:04Z INFO Sent 70600/79200  actual 99.9 eps
2026-04-09T07:10:04Z INFO Sent 70650/79200  actual 100.0 eps
2026-04-09T07:10:04Z INFO Sent 70700/79200  actual 100.0 eps
2026-04-09T07:10:05Z INFO Sent 70750/79200  actual 100.0 eps
2026-04-09T07:10:05Z INFO Sent 70800/79200  actual 100.0 eps
2026-04-09T07:10:06Z INFO Sent 70850/79200  actual 100.0 eps
2026-04-09T07:10:06Z INFO Sent 70900/79200  actual 100.0 eps
2026-04-09T07:10:07Z INFO Sent 70950/79200  actual 100.0 eps
2026-04-09T07:10:07Z INFO Sent 71000/79200  actual 100.0 eps
2026-04-09T07:10:08Z INFO Sent 71050/79200  actual 100.0 eps
2026-04-09T07:10:08Z INFO Sent 71100/79200  actual 100.0 eps
2026-04-09T07:10:09Z INFO Sent 71150/79200  actual 100.0 eps
2026-04-09T07:10:09Z INFO Sent 71200/79200  actual 100.0 eps
2026-04-09T07:10:10Z INFO Sent 71250/79200  actual 100.0 eps
2026-04-09T07:10:10Z INFO Sent 71300/79200  actual 100.0 eps
2026-04-09T07:10:11Z INFO Sent 71350/79200  actual 100.0 eps
2026-04-09T07:10:11Z INFO Sent 71400/79200  actual 100.0 eps
2026-04-09T07:10:12Z INFO Sent 71450/79200  actual 100.0 eps
2026-04-09T07:10:12Z INFO Sent 71500/79200  actual 100.0 eps
2026-04-09T07:10:13Z INFO Sent 71550/79200  actual 100.0 eps
2026-04-09T07:10:13Z INFO Sent 71600/79200  actual 100.0 eps
2026-04-09T07:10:14Z INFO Sent 71650/79200  actual 100.0 eps
2026-04-09T07:10:14Z INFO Sent 71700/79200  actual 100.0 eps
2026-04-09T07:10:15Z INFO Sent 71750/79200  actual 100.0 eps
2026-04-09T07:10:15Z INFO Sent 71800/79200  actual 100.0 eps
2026-04-09T07:10:16Z INFO Sent 71850/79200  actual 100.0 eps
2026-04-09T07:10:16Z INFO Sent 71900/79200  actual 100.0 eps
2026-04-09T07:10:17Z INFO Sent 71950/79200  actual 100.0 eps
2026-04-09T07:10:17Z INFO Sent 72000/79200  actual 100.0 eps
2026-04-09T07:10:18Z INFO Sent 72050/79200  actual 100.0 eps
2026-04-09T07:10:18Z INFO Sent 72100/79200  actual 100.0 eps
2026-04-09T07:10:19Z INFO Sent 72150/79200  actual 100.0 eps
2026-04-09T07:10:19Z INFO Sent 72200/79200  actual 100.0 eps
2026-04-09T07:10:20Z INFO Sent 72250/79200  actual 100.0 eps
2026-04-09T07:10:20Z INFO Sent 72300/79200  actual 100.0 eps
2026-04-09T07:10:21Z INFO Sent 72350/79200  actual 100.0 eps
2026-04-09T07:10:21Z INFO Sent 72400/79200  actual 100.0 eps
2026-04-09T07:10:22Z INFO Sent 72450/79200  actual 100.0 eps
2026-04-09T07:10:22Z INFO Sent 72500/79200  actual 100.0 eps
2026-04-09T07:10:23Z INFO Sent 72550/79200  actual 100.0 eps
2026-04-09T07:10:23Z INFO Sent 72600/79200  actual 100.0 eps
2026-04-09T07:10:24Z INFO Sent 72650/79200  actual 100.0 eps
2026-04-09T07:10:24Z INFO Sent 72700/79200  actual 100.0 eps
2026-04-09T07:10:25Z INFO Sent 72750/79200  actual 100.0 eps
2026-04-09T07:10:25Z INFO Sent 72800/79200  actual 100.0 eps
2026-04-09T07:10:26Z INFO Sent 72850/79200  actual 100.0 eps
2026-04-09T07:10:26Z INFO Sent 72900/79200  actual 100.0 eps
2026-04-09T07:10:27Z INFO Sent 72950/79200  actual 100.0 eps
2026-04-09T07:10:27Z INFO Sent 73000/79200  actual 100.0 eps
2026-04-09T07:10:28Z INFO Sent 73050/79200  actual 100.0 eps
2026-04-09T07:10:28Z INFO Sent 73100/79200  actual 100.0 eps
2026-04-09T07:10:29Z INFO Sent 73150/79200  actual 100.0 eps
2026-04-09T07:10:29Z INFO Sent 73200/79200  actual 100.0 eps
2026-04-09T07:10:30Z INFO Sent 73250/79200  actual 100.0 eps
2026-04-09T07:10:30Z INFO Sent 73300/79200  actual 100.0 eps
2026-04-09T07:10:31Z INFO Sent 73350/79200  actual 100.0 eps
2026-04-09T07:10:31Z INFO Sent 73400/79200  actual 100.0 eps
2026-04-09T07:10:32Z INFO Sent 73450/79200  actual 100.0 eps
2026-04-09T07:10:32Z INFO Sent 73500/79200  actual 100.0 eps
2026-04-09T07:10:33Z INFO Sent 73550/79200  actual 100.0 eps
2026-04-09T07:10:33Z INFO Sent 73600/79200  actual 100.0 eps
2026-04-09T07:10:34Z INFO Sent 73650/79200  actual 100.0 eps
2026-04-09T07:10:34Z INFO Sent 73700/79200  actual 100.0 eps
2026-04-09T07:10:35Z INFO Sent 73750/79200  actual 100.0 eps
2026-04-09T07:10:35Z INFO Sent 73800/79200  actual 100.0 eps
2026-04-09T07:10:36Z INFO Sent 73850/79200  actual 100.0 eps
2026-04-09T07:10:36Z INFO Sent 73900/79200  actual 100.0 eps
2026-04-09T07:10:37Z INFO Sent 73950/79200  actual 100.0 eps
2026-04-09T07:10:37Z INFO Sent 74000/79200  actual 100.0 eps
2026-04-09T07:10:38Z INFO Sent 74050/79200  actual 100.0 eps
2026-04-09T07:10:38Z INFO Sent 74100/79200  actual 100.0 eps
2026-04-09T07:10:39Z INFO Sent 74150/79200  actual 100.0 eps
2026-04-09T07:10:39Z INFO Sent 74200/79200  actual 100.0 eps
2026-04-09T07:10:40Z INFO Sent 74250/79200  actual 100.0 eps
2026-04-09T07:10:40Z INFO Sent 74300/79200  actual 100.0 eps
2026-04-09T07:10:41Z INFO Sent 74350/79200  actual 100.0 eps
2026-04-09T07:10:41Z INFO Sent 74400/79200  actual 100.0 eps
2026-04-09T07:10:42Z INFO Sent 74450/79200  actual 100.0 eps
2026-04-09T07:10:42Z INFO Sent 74500/79200  actual 100.0 eps
2026-04-09T07:10:43Z INFO Sent 74550/79200  actual 100.0 eps
2026-04-09T07:10:43Z INFO Sent 74600/79200  actual 100.0 eps
2026-04-09T07:10:44Z INFO Sent 74650/79200  actual 100.0 eps
2026-04-09T07:10:44Z INFO Sent 74700/79200  actual 100.0 eps
2026-04-09T07:10:45Z INFO Sent 74750/79200  actual 100.0 eps
2026-04-09T07:10:45Z INFO Sent 74800/79200  actual 100.0 eps
2026-04-09T07:10:46Z INFO Sent 74850/79200  actual 100.0 eps
2026-04-09T07:10:46Z INFO Sent 74900/79200  actual 100.0 eps
2026-04-09T07:10:47Z INFO Sent 74950/79200  actual 100.0 eps
2026-04-09T07:10:47Z INFO Sent 75000/79200  actual 100.0 eps
2026-04-09T07:10:48Z INFO Sent 75050/79200  actual 100.0 eps
2026-04-09T07:10:48Z INFO Sent 75100/79200  actual 100.0 eps
2026-04-09T07:10:49Z INFO Sent 75150/79200  actual 100.0 eps
2026-04-09T07:10:49Z INFO Sent 75200/79200  actual 100.0 eps
2026-04-09T07:10:50Z INFO Sent 75250/79200  actual 100.0 eps
2026-04-09T07:10:50Z INFO Sent 75300/79200  actual 100.0 eps
2026-04-09T07:10:51Z INFO Sent 75350/79200  actual 100.0 eps
2026-04-09T07:10:51Z INFO Sent 75400/79200  actual 100.0 eps
2026-04-09T07:10:52Z INFO Sent 75450/79200  actual 100.0 eps
2026-04-09T07:10:52Z INFO Sent 75500/79200  actual 100.0 eps
2026-04-09T07:10:53Z INFO Sent 75550/79200  actual 100.0 eps
2026-04-09T07:10:53Z INFO Sent 75600/79200  actual 100.0 eps
2026-04-09T07:10:54Z INFO Sent 75650/79200  actual 100.0 eps
2026-04-09T07:10:54Z INFO Sent 75700/79200  actual 100.0 eps
2026-04-09T07:10:55Z INFO Sent 75750/79200  actual 100.0 eps
2026-04-09T07:10:55Z INFO Sent 75800/79200  actual 100.0 eps
2026-04-09T07:10:56Z INFO Sent 75850/79200  actual 100.0 eps
2026-04-09T07:10:56Z INFO Sent 75900/79200  actual 100.0 eps
2026-04-09T07:10:57Z INFO Sent 75950/79200  actual 100.0 eps
2026-04-09T07:10:57Z INFO Sent 76000/79200  actual 100.0 eps
2026-04-09T07:10:58Z INFO Sent 76050/79200  actual 100.0 eps
2026-04-09T07:10:58Z INFO Sent 76100/79200  actual 100.0 eps
2026-04-09T07:10:59Z INFO Sent 76150/79200  actual 100.0 eps
2026-04-09T07:10:59Z INFO Sent 76200/79200  actual 100.0 eps
2026-04-09T07:11:00Z INFO Sent 76250/79200  actual 100.0 eps
2026-04-09T07:11:00Z INFO Sent 76300/79200  actual 100.0 eps
2026-04-09T07:11:01Z INFO Sent 76350/79200  actual 100.0 eps
2026-04-09T07:11:01Z INFO Sent 76400/79200  actual 100.0 eps
2026-04-09T07:11:02Z INFO Sent 76450/79200  actual 100.0 eps
2026-04-09T07:11:02Z INFO Sent 76500/79200  actual 100.0 eps
2026-04-09T07:11:03Z INFO Sent 76550/79200  actual 100.0 eps
2026-04-09T07:11:03Z INFO Sent 76600/79200  actual 100.0 eps
2026-04-09T07:11:04Z INFO Sent 76650/79200  actual 100.0 eps
2026-04-09T07:11:04Z INFO Sent 76700/79200  actual 100.0 eps
2026-04-09T07:11:05Z INFO Sent 76750/79200  actual 100.0 eps
2026-04-09T07:11:05Z INFO Sent 76800/79200  actual 100.0 eps
2026-04-09T07:11:06Z INFO Sent 76850/79200  actual 100.0 eps
2026-04-09T07:11:06Z INFO Sent 76900/79200  actual 100.0 eps
2026-04-09T07:11:07Z INFO Sent 76950/79200  actual 100.0 eps
2026-04-09T07:11:07Z INFO Sent 77000/79200  actual 100.0 eps
2026-04-09T07:11:08Z INFO Sent 77050/79200  actual 100.0 eps
2026-04-09T07:11:08Z INFO Sent 77100/79200  actual 100.0 eps
2026-04-09T07:11:09Z INFO Sent 77150/79200  actual 100.0 eps
2026-04-09T07:11:09Z INFO Sent 77200/79200  actual 100.0 eps
2026-04-09T07:11:10Z INFO Sent 77250/79200  actual 100.0 eps
2026-04-09T07:11:10Z INFO Sent 77300/79200  actual 100.0 eps
2026-04-09T07:11:11Z INFO Sent 77350/79200  actual 100.0 eps
2026-04-09T07:11:11Z INFO Sent 77400/79200  actual 100.0 eps
2026-04-09T07:11:12Z INFO Sent 77450/79200  actual 100.0 eps
2026-04-09T07:11:12Z INFO Sent 77500/79200  actual 100.0 eps
2026-04-09T07:11:13Z INFO Sent 77550/79200  actual 100.0 eps
2026-04-09T07:11:13Z INFO Sent 77600/79200  actual 100.0 eps
2026-04-09T07:11:14Z INFO Sent 77650/79200  actual 100.0 eps
2026-04-09T07:11:14Z INFO Sent 77700/79200  actual 100.0 eps
2026-04-09T07:11:15Z INFO Sent 77750/79200  actual 100.0 eps
2026-04-09T07:11:15Z INFO Sent 77800/79200  actual 100.0 eps
2026-04-09T07:11:16Z INFO Sent 77850/79200  actual 100.0 eps
2026-04-09T07:11:16Z INFO Sent 77900/79200  actual 100.0 eps
2026-04-09T07:11:17Z INFO Sent 77950/79200  actual 100.0 eps
2026-04-09T07:11:17Z INFO Sent 78000/79200  actual 100.0 eps
2026-04-09T07:11:18Z INFO Sent 78050/79200  actual 100.0 eps
2026-04-09T07:11:18Z INFO Sent 78100/79200  actual 100.0 eps
2026-04-09T07:11:19Z INFO Sent 78150/79200  actual 100.0 eps
2026-04-09T07:11:19Z INFO Sent 78200/79200  actual 100.0 eps
2026-04-09T07:11:20Z INFO Sent 78250/79200  actual 100.0 eps
2026-04-09T07:11:20Z INFO Sent 78300/79200  actual 100.0 eps
2026-04-09T07:11:21Z INFO Sent 78350/79200  actual 100.0 eps
2026-04-09T07:11:21Z INFO Sent 78400/79200  actual 100.0 eps
2026-04-09T07:11:22Z INFO Sent 78450/79200  actual 100.0 eps
2026-04-09T07:11:22Z INFO Sent 78500/79200  actual 100.0 eps
2026-04-09T07:11:23Z INFO Sent 78550/79200  actual 100.0 eps
2026-04-09T07:11:23Z INFO Sent 78600/79200  actual 100.0 eps
2026-04-09T07:11:24Z INFO Sent 78650/79200  actual 100.0 eps
2026-04-09T07:11:24Z INFO Sent 78700/79200  actual 100.0 eps
2026-04-09T07:11:25Z INFO Sent 78750/79200  actual 100.0 eps
2026-04-09T07:11:25Z INFO Sent 78800/79200  actual 100.0 eps
2026-04-09T07:11:26Z INFO Sent 78850/79200  actual 100.0 eps
2026-04-09T07:11:26Z INFO Sent 78900/79200  actual 100.0 eps
2026-04-09T07:11:27Z INFO Sent 78950/79200  actual 100.0 eps
2026-04-09T07:11:27Z INFO Sent 79000/79200  actual 100.0 eps
2026-04-09T07:11:28Z INFO Sent 79050/79200  actual 100.0 eps
2026-04-09T07:11:28Z INFO Sent 79100/79200  actual 100.0 eps
2026-04-09T07:11:29Z INFO Sent 79150/79200  actual 100.0 eps
2026-04-09T07:11:29Z INFO Sent 79200/79200  actual 100.0 eps
2026-04-09T07:11:29Z INFO Replay complete: 79200 events in 792.0s  avg 100.0 eps
2026-04-09T07:11:29Z INFO Closing the Kafka producer with 9223372036.0 secs timeout.
2026-04-09T07:11:29Z INFO <BrokerConnection node_id=1 host=kafka-0.kafka.vacciguard.svc.cluster.local:9092 <connected> [IPv4 ('172.31.29.100', 9092)]>: Closing connection. 
2026-04-09T07:11:29Z INFO Producer finished
```

## Stream Summary Logs

```text
2026-04-09T07:05:01Z INFO Stream processor is running with 2 active queries and metrics on port 9108
[Stage 30:=========>        (2 + 2) / 4][Stage 32:=========>        (2 + 0) / 4]                                                                                2026-04-09T07:05:28Z INFO Batch 64 summary valid=532 invalid=0 deduplicated=0 breach=0 processed=532 avg_e2e_latency_s=45.15 p95_e2e_latency_s=47.18
[Stage 40:=========>        (2 + 2) / 4][Stage 42:>                 (0 + 0) / 6][Stage 40:=============>    (3 + 1) / 4][Stage 42:>                 (0 + 1) / 6][Stage 40:=============>    (3 + 1) / 4][Stage 42:===>              (1 + 1) / 6][Stage 42:===================>                                      (2 + 1) / 6][Stage 42:=============================>                            (3 + 1) / 6][Stage 42:======================================>                   (4 + 2) / 6][Stage 42:================================================>         (5 + 1) / 6]                                                                                2026-04-09T07:05:35Z INFO Batch 22: wrote 30 latest device states to Redis
2026-04-09T07:05:39Z INFO Batch 65 summary valid=4635 invalid=0 deduplicated=0 breach=54 processed=4635 avg_e2e_latency_s=32.83 p95_e2e_latency_s=53.71
[Stage 101:========>        (2 + 2) / 4][Stage 104:>                (0 + 0) / 1]                                                                                2026-04-09T07:05:52Z INFO Batch 66 summary valid=1055 invalid=0 deduplicated=0 breach=33 processed=1055 avg_e2e_latency_s=15.77 p95_e2e_latency_s=20.06
[Stage 113:========>        (2 + 2) / 4][Stage 115:>                (0 + 0) / 6][Stage 113:============>    (3 + 1) / 4][Stage 115:===========>     (4 + 1) / 6][Stage 113:==========================================>              (3 + 1) / 4]                                                                                2026-04-09T07:05:58Z INFO Batch 23: wrote 30 latest device states to Redis
2026-04-09T07:05:59Z INFO Batch 67 summary valid=1293 invalid=0 deduplicated=0 breach=21 processed=1293 avg_e2e_latency_s=13.24 p95_e2e_latency_s=19.28
[Stage 168:====>            (1 + 2) / 4][Stage 171:>                (0 + 0) / 4][Stage 168:============>    (3 + 1) / 4][Stage 171:====>            (1 + 1) / 4]                                                                                2026-04-09T07:06:09Z INFO Batch 68 summary valid=700 invalid=0 deduplicated=0 breach=0 processed=700 avg_e2e_latency_s=11.85 p95_e2e_latency_s=14.86
2026-04-09T07:06:16Z INFO Batch 69 summary valid=947 invalid=0 deduplicated=0 breach=0 processed=947 avg_e2e_latency_s=11.71 p95_e2e_latency_s=16.15
2026-04-09T07:06:19Z INFO Batch 24: wrote 30 latest device states to Redis
2026-04-09T07:06:21Z INFO Batch 70 summary valid=786 invalid=0 deduplicated=0 breach=45 processed=786 avg_e2e_latency_s=8.21 p95_e2e_latency_s=12.12
2026-04-09T07:06:28Z INFO Batch 71 summary valid=470 invalid=0 deduplicated=0 breach=9 processed=470 avg_e2e_latency_s=8.71 p95_e2e_latency_s=10.88
[Stage 293:============================>                            (2 + 2) / 4][Stage 293:========>        (2 + 2) / 4][Stage 296:>                (0 + 0) / 4]                                                                                2026-04-09T07:06:34Z INFO Batch 72 summary valid=657 invalid=0 deduplicated=0 breach=0 processed=657 avg_e2e_latency_s=8.62 p95_e2e_latency_s=11.46
2026-04-09T07:06:34Z INFO Batch 25: wrote 30 latest device states to Redis
[Stage 316:>                (0 + 2) / 4][Stage 317:>                (0 + 0) / 6][Stage 316:====>            (1 + 2) / 4][Stage 317:>                (0 + 0) / 6][Stage 316:========>        (2 + 2) / 4][Stage 317:>                (0 + 0) / 6][Stage 316:============>    (3 + 1) / 4][Stage 317:>                (0 + 1) / 6][Stage 316:============>    (3 + 1) / 4][Stage 317:========>        (3 + 1) / 6][Stage 316:==========================================>              (3 + 1) / 4]                                                                                2026-04-09T07:06:42Z INFO Batch 73 summary valid=643 invalid=0 deduplicated=0 breach=0 processed=643 avg_e2e_latency_s=11.07 p95_e2e_latency_s=14.38
[Stage 345:========>        (2 + 2) / 4][Stage 347:>                (0 + 0) / 4]                                                                                2026-04-09T07:06:49Z INFO Batch 74 summary valid=790 invalid=0 deduplicated=0 breach=0 processed=790 avg_e2e_latency_s=11.09 p95_e2e_latency_s=14.58
2026-04-09T07:06:53Z INFO Batch 26: wrote 30 latest device states to Redis
2026-04-09T07:06:55Z INFO Batch 75 summary valid=736 invalid=0 deduplicated=0 breach=15 processed=736 avg_e2e_latency_s=8.71 p95_e2e_latency_s=11.84
2026-04-09T07:07:00Z INFO Batch 76 summary valid=538 invalid=0 deduplicated=0 breach=39 processed=538 avg_e2e_latency_s=8.04 p95_e2e_latency_s=10.53
[Stage 436:============================>                            (2 + 2) / 4][Stage 436:========>        (2 + 2) / 4][Stage 438:>                (0 + 0) / 6][Stage 436:============>    (3 + 1) / 4][Stage 438:=====>           (2 + 1) / 6]                                                                                2026-04-09T07:07:06Z INFO Batch 77 summary valid=561 invalid=0 deduplicated=0 breach=0 processed=561 avg_e2e_latency_s=8.82 p95_e2e_latency_s=11.81
2026-04-09T07:07:10Z INFO Batch 27: wrote 30 latest device states to Redis
2026-04-09T07:07:12Z INFO Batch 78 summary valid=620 invalid=0 deduplicated=0 breach=0 processed=620 avg_e2e_latency_s=8.05 p95_e2e_latency_s=10.97
2026-04-09T07:07:19Z INFO Batch 79 summary valid=561 invalid=0 deduplicated=0 breach=0 processed=561 avg_e2e_latency_s=9.37 p95_e2e_latency_s=12.15
2026-04-09T07:07:24Z INFO Batch 80 summary valid=675 invalid=0 deduplicated=0 breach=0 processed=675 avg_e2e_latency_s=8.58 p95_e2e_latency_s=11.55
2026-04-09T07:07:27Z INFO Batch 28: wrote 30 latest device states to Redis
2026-04-09T07:07:29Z INFO Batch 81 summary valid=545 invalid=0 deduplicated=0 breach=4 processed=545 avg_e2e_latency_s=7.20 p95_e2e_latency_s=9.28
[Stage 615:>                                                        (0 + 2) / 4][Stage 615:========>        (2 + 2) / 4][Stage 618:>                (0 + 0) / 4][Stage 618:>                                                        (0 + 2) / 4]                                                                                2026-04-09T07:07:36Z INFO Batch 82 summary valid=471 invalid=0 deduplicated=0 breach=47 processed=471 avg_e2e_latency_s=7.72 p95_e2e_latency_s=9.86
2026-04-09T07:07:41Z INFO Batch 83 summary valid=663 invalid=0 deduplicated=0 breach=3 processed=663 avg_e2e_latency_s=8.82 p95_e2e_latency_s=11.66
2026-04-09T07:07:44Z INFO Batch 29: wrote 30 latest device states to Redis
2026-04-09T07:07:46Z INFO Batch 84 summary valid=561 invalid=0 deduplicated=0 breach=0 processed=561 avg_e2e_latency_s=7.33 p95_e2e_latency_s=10.28
[Stage 709:>                                                        (0 + 2) / 4][Stage 709:>                (0 + 2) / 4][Stage 712:>                (0 + 0) / 4][Stage 709:========>        (2 + 2) / 4][Stage 712:>                (0 + 0) / 4]                                                                                2026-04-09T07:07:52Z INFO Batch 85 summary valid=505 invalid=0 deduplicated=0 breach=0 processed=505 avg_e2e_latency_s=7.11 p95_e2e_latency_s=9.39
2026-04-09T07:07:57Z INFO Batch 86 summary valid=587 invalid=0 deduplicated=0 breach=0 processed=587 avg_e2e_latency_s=7.90 p95_e2e_latency_s=10.63
2026-04-09T07:08:00Z INFO Batch 30: wrote 30 latest device states to Redis
2026-04-09T07:08:03Z INFO Batch 87 summary valid=505 invalid=0 deduplicated=0 breach=0 processed=505 avg_e2e_latency_s=7.58 p95_e2e_latency_s=9.78
2026-04-09T07:08:09Z INFO Batch 88 summary valid=556 invalid=0 deduplicated=0 breach=30 processed=556 avg_e2e_latency_s=8.20 p95_e2e_latency_s=10.70
2026-04-09T07:08:13Z INFO Batch 89 summary valid=592 invalid=0 deduplicated=0 breach=24 processed=592 avg_e2e_latency_s=7.46 p95_e2e_latency_s=9.70
2026-04-09T07:08:16Z INFO Batch 31: wrote 30 latest device states to Redis
2026-04-09T07:08:18Z INFO Batch 90 summary valid=451 invalid=0 deduplicated=0 breach=0 processed=451 avg_e2e_latency_s=6.55 p95_e2e_latency_s=9.00
[Stage 894:========>        (2 + 2) / 4][Stage 897:>                (0 + 0) / 4]                                                                                2026-04-09T07:08:23Z INFO Batch 91 summary valid=445 invalid=0 deduplicated=0 breach=0 processed=445 avg_e2e_latency_s=7.53 p95_e2e_latency_s=9.47
2026-04-09T07:08:28Z INFO Batch 92 summary valid=534 invalid=0 deduplicated=0 breach=0 processed=534 avg_e2e_latency_s=7.73 p95_e2e_latency_s=10.54
2026-04-09T07:08:31Z INFO Batch 32: wrote 30 latest device states to Redis
2026-04-09T07:08:33Z INFO Batch 93 summary valid=506 invalid=0 deduplicated=0 breach=0 processed=506 avg_e2e_latency_s=7.30 p95_e2e_latency_s=9.33
2026-04-09T07:08:38Z INFO Batch 94 summary valid=508 invalid=0 deduplicated=0 breach=0 processed=508 avg_e2e_latency_s=7.01 p95_e2e_latency_s=9.12
[Stage 1000:========>       (2 + 2) / 4][Stage 1002:>               (0 + 0) / 6]                                                                                2026-04-09T07:08:44Z INFO Batch 95 summary valid=455 invalid=0 deduplicated=0 breach=18 processed=455 avg_e2e_latency_s=8.28 p95_e2e_latency_s=10.20
2026-04-09T07:08:48Z INFO Batch 33: wrote 30 latest device states to Redis
2026-04-09T07:08:48Z INFO Batch 96 summary valid=628 invalid=0 deduplicated=0 breach=36 processed=628 avg_e2e_latency_s=7.46 p95_e2e_latency_s=9.78
2026-04-09T07:08:55Z INFO Batch 97 summary valid=436 invalid=0 deduplicated=0 breach=0 processed=436 avg_e2e_latency_s=8.34 p95_e2e_latency_s=10.01
[Stage 1094:============>   (3 + 1) / 4][Stage 1096:==>             (1 + 1) / 6][Stage 1094:============>   (3 + 1) / 4][Stage 1096:=====>          (2 + 1) / 6][Stage 1094:============>   (3 + 1) / 4][Stage 1096:==========>     (4 + 1) / 6]                                                                                2026-04-09T07:09:01Z INFO Batch 98 summary valid=615 invalid=0 deduplicated=0 breach=0 processed=615 avg_e2e_latency_s=9.01 p95_e2e_latency_s=11.92
2026-04-09T07:09:05Z INFO Batch 34: wrote 30 latest device states to Redis
2026-04-09T07:09:05Z INFO Batch 99 summary valid=592 invalid=0 deduplicated=0 breach=0 processed=592 avg_e2e_latency_s=7.72 p95_e2e_latency_s=10.66
2026-04-09T07:09:10Z INFO Batch 100 summary valid=471 invalid=0 deduplicated=0 breach=0 processed=471 avg_e2e_latency_s=7.36 p95_e2e_latency_s=9.60
[Stage 1188:============>   (3 + 1) / 4][Stage 1190:>               (0 + 1) / 6][Stage 1188:============>   (3 + 1) / 4][Stage 1190:========>       (3 + 1) / 6][Stage 1188:============>   (3 + 1) / 4][Stage 1190:==========>     (4 + 1) / 6]                                                                                2026-04-09T07:09:17Z INFO Batch 101 summary valid=492 invalid=0 deduplicated=0 breach=0 processed=492 avg_e2e_latency_s=8.85 p95_e2e_latency_s=10.91
2026-04-09T07:09:20Z INFO Batch 35: wrote 30 latest device states to Redis
2026-04-09T07:09:21Z INFO Batch 102 summary valid=629 invalid=0 deduplicated=0 breach=45 processed=629 avg_e2e_latency_s=7.45 p95_e2e_latency_s=10.14
2026-04-09T07:09:26Z INFO Batch 103 summary valid=438 invalid=0 deduplicated=0 breach=9 processed=438 avg_e2e_latency_s=7.12 p95_e2e_latency_s=9.13
[Stage 1282:==========================================>             (3 + 1) / 4][Stage 1282:============>   (3 + 1) / 4][Stage 1284:==>             (1 + 1) / 6][Stage 1282:============>   (3 + 1) / 4][Stage 1284:==========>     (4 + 1) / 6]                                                                                2026-04-09T07:09:30Z INFO Batch 104 summary valid=489 invalid=0 deduplicated=0 breach=0 processed=489 avg_e2e_latency_s=7.20 p95_e2e_latency_s=9.85
2026-04-09T07:09:33Z INFO Batch 36: wrote 30 latest device states to Redis
2026-04-09T07:09:35Z INFO Batch 105 summary valid=468 invalid=0 deduplicated=0 breach=0 processed=468 avg_e2e_latency_s=6.85 p95_e2e_latency_s=9.27
2026-04-09T07:09:40Z INFO Batch 106 summary valid=463 invalid=0 deduplicated=0 breach=0 processed=463 avg_e2e_latency_s=7.72 p95_e2e_latency_s=9.79
[Stage 1376:============>   (3 + 1) / 4][Stage 1378:==>             (1 + 1) / 6][Stage 1376:============>   (3 + 1) / 4][Stage 1378:=============>  (5 + 1) / 6]                                                                                2026-04-09T07:09:46Z INFO Batch 107 summary valid=551 invalid=0 deduplicated=0 breach=0 processed=551 avg_e2e_latency_s=7.99 p95_e2e_latency_s=10.16
2026-04-09T07:09:50Z INFO Batch 37: wrote 30 latest device states to Redis
2026-04-09T07:09:50Z INFO Batch 108 summary valid=548 invalid=0 deduplicated=0 breach=0 processed=548 avg_e2e_latency_s=6.72 p95_e2e_latency_s=9.35
[Stage 1442:>                                                       (0 + 2) / 4][Stage 1442:>               (0 + 2) / 4][Stage 1443:>               (0 + 0) / 6][Stage 1442:========>       (2 + 2) / 4][Stage 1443:>               (0 + 0) / 6]                                                                                2026-04-09T07:09:55Z INFO Batch 109 summary valid=395 invalid=0 deduplicated=0 breach=20 processed=395 avg_e2e_latency_s=7.11 p95_e2e_latency_s=8.48
[Stage 1470:============>   (3 + 1) / 4][Stage 1472:=====>          (2 + 1) / 6]                                                                                2026-04-09T07:10:01Z INFO Batch 110 summary valid=514 invalid=0 deduplicated=0 breach=34 processed=514 avg_e2e_latency_s=8.35 p95_e2e_latency_s=10.25
2026-04-09T07:10:05Z INFO Batch 38: wrote 30 latest device states to Redis
2026-04-09T07:10:06Z INFO Batch 111 summary valid=552 invalid=0 deduplicated=0 breach=0 processed=552 avg_e2e_latency_s=7.75 p95_e2e_latency_s=10.01
2026-04-09T07:10:11Z INFO Batch 112 summary valid=526 invalid=0 deduplicated=0 breach=0 processed=526 avg_e2e_latency_s=7.43 p95_e2e_latency_s=9.37
[Stage 1564:============>   (3 + 1) / 4][Stage 1566:>               (0 + 1) / 6][Stage 1564:============>   (3 + 1) / 4][Stage 1566:=====>          (2 + 1) / 6][Stage 1564:============>   (3 + 1) / 4][Stage 1566:==========>     (4 + 1) / 6]                                                                                2026-04-09T07:10:17Z INFO Batch 113 summary valid=510 invalid=0 deduplicated=0 breach=0 processed=510 avg_e2e_latency_s=8.39 p95_e2e_latency_s=10.19
2026-04-09T07:10:20Z INFO Batch 39: wrote 30 latest device states to Redis
2026-04-09T07:10:21Z INFO Batch 114 summary valid=584 invalid=0 deduplicated=0 breach=0 processed=584 avg_e2e_latency_s=7.03 p95_e2e_latency_s=9.31
2026-04-09T07:10:27Z INFO Batch 115 summary valid=410 invalid=0 deduplicated=0 breach=6 processed=410 avg_e2e_latency_s=7.70 p95_e2e_latency_s=9.95
[Stage 1658:============>   (3 + 1) / 4][Stage 1660:==>             (1 + 1) / 6][Stage 1658:============>   (3 + 1) / 4][Stage 1660:========>       (3 + 1) / 6]                                                                                2026-04-09T07:10:33Z INFO Batch 116 summary valid=562 invalid=0 deduplicated=0 breach=7 processed=562 avg_e2e_latency_s=8.84 p95_e2e_latency_s=10.96
2026-04-09T07:10:36Z INFO Batch 40: wrote 30 latest device states to Redis
2026-04-09T07:10:37Z INFO Batch 117 summary valid=606 invalid=0 deduplicated=0 breach=14 processed=606 avg_e2e_latency_s=6.95 p95_e2e_latency_s=9.90
2026-04-09T07:10:41Z INFO Batch 118 summary valid=390 invalid=0 deduplicated=0 breach=4 processed=390 avg_e2e_latency_s=6.91 p95_e2e_latency_s=8.83
2026-04-09T07:10:45Z INFO Batch 119 summary valid=494 invalid=0 deduplicated=0 breach=2 processed=494 avg_e2e_latency_s=5.86 p95_e2e_latency_s=8.20
                                                                                2026-04-09T07:10:51Z INFO Batch 120 summary valid=374 invalid=0 deduplicated=0 breach=5 processed=374 avg_e2e_latency_s=6.96 p95_e2e_latency_s=8.63
2026-04-09T07:10:55Z INFO Batch 121 summary valid=579 invalid=0 deduplicated=0 breach=6 processed=579 avg_e2e_latency_s=7.08 p95_e2e_latency_s=9.54
[Stage 1862:=====>          (1 + 2) / 3][Stage 1864:>               (0 + 0) / 4][Stage 1862:=====================================>                  (2 + 1) / 3]                                                                                2026-04-09T07:11:00Z INFO Batch 122 summary valid=434 invalid=0 deduplicated=0 breach=8 processed=434 avg_e2e_latency_s=7.02 p95_e2e_latency_s=8.54
2026-04-09T07:11:04Z INFO Batch 123 summary valid=488 invalid=0 deduplicated=0 breach=7 processed=488 avg_e2e_latency_s=6.41 p95_e2e_latency_s=8.53
                                                                                2026-04-09T07:11:09Z INFO Batch 124 summary valid=401 invalid=0 deduplicated=0 breach=2 processed=401 avg_e2e_latency_s=6.46 p95_e2e_latency_s=8.07
2026-04-09T07:11:14Z INFO Batch 125 summary valid=492 invalid=0 deduplicated=0 breach=7 processed=492 avg_e2e_latency_s=7.63 p95_e2e_latency_s=9.66
2026-04-09T07:11:18Z INFO Batch 126 summary valid=527 invalid=0 deduplicated=0 breach=9 processed=527 avg_e2e_latency_s=6.40 p95_e2e_latency_s=8.55
2026-04-09T07:11:28Z INFO Batch 127 summary valid=0 invalid=473 deduplicated=0 breach=0 processed=0 avg_e2e_latency_s=n/a p95_e2e_latency_s=n/a
[Stage 2056:========>       (3 + 2) / 6][Stage 2058:>               (0 + 0) / 6][Stage 2056:==========>     (4 + 2) / 6][Stage 2058:>               (0 + 0) / 6][Stage 2056:=============>  (5 + 1) / 6][Stage 2058:========>       (3 + 1) / 6]                                                                                2026-04-09T07:11:41Z INFO Batch 128 summary valid=0 invalid=871 deduplicated=0 breach=0 processed=0 avg_e2e_latency_s=n/a p95_e2e_latency_s=n/a
2026-04-09T07:11:49Z INFO Batch 129 summary valid=0 invalid=96 deduplicated=0 breach=0 processed=0 avg_e2e_latency_s=n/a p95_e2e_latency_s=n/a
```

## S3 Objects

```text
2026-04-09 12:40:34          0 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/_SUCCESS
2026-04-09 12:30:24        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-0cff3f48-bc63-4043-8dce-634ee9b12ad9-c000.json
2026-04-09 12:35:29        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-0e487b7c-152b-40e3-9464-21d8c5ea2653-c000.json
2026-04-09 12:37:57        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-157bb902-a3c5-414f-9c01-6744457b93df-c000.json
2026-04-09 12:30:59        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-198298ec-69aa-46f9-bcf8-36162b457875-c000.json
2026-04-09 12:30:07        206 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-21a4b096-494f-42e4-b8a1-4255d7d96962-c000.json
2026-04-09 12:35:53        778 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-2aa8e8fc-e1c1-4b4d-be69-fae34c3257d2-c000.json
2026-04-09 12:38:44       1516 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-2b3072c0-1a06-46f6-8e4d-b44fac206676-c000.json
2026-04-09 12:29:12        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-2d5faa19-dffa-4e2c-afc4-fcfa90c82fba-c000.json
2026-04-09 12:39:46        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-44786efe-da80-4e89-a2a9-31053de9613a-c000.json
2026-04-09 12:29:30        195 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-4762ba17-6c99-48f6-98a7-797202f45ea8-c000.json
2026-04-09 12:34:32        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-4b336c47-6f38-45a6-8f9e-6ecca6afade6-c000.json
2026-04-09 12:28:51        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-4efe3700-1b01-42e0-8772-74178bb62d77-c000.json
2026-04-09 12:34:17        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-52c679be-3dec-4e05-ac5e-8741b7de6e63-c000.json
2026-04-09 12:36:50        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-5331719e-1df4-424c-8cad-3e81c35465a3-c000.json
2026-04-09 12:32:25        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-5a7ae2c3-6058-4742-9b73-0d8cd4534610-c000.json
2026-04-09 12:33:13        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-6970d210-8b53-4e31-945d-5f821ae82cbf-c000.json
2026-04-09 12:32:06        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-6efe0857-46ca-4aca-9e80-af7216f27b36-c000.json
2026-04-09 12:37:40        778 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-7744f57d-2959-42f4-8c4f-9adeb8d1a10f-c000.json
2026-04-09 12:29:48        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-7b4a9e74-d5a0-4fd5-9244-00e6355bfbd9-c000.json
2026-04-09 12:39:01        811 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-7fad3ad6-49a4-4fe6-852e-f83f63a868ce-c000.json
2026-04-09 12:36:31        776 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-814def8f-08ce-4adf-bb7d-cd82971a331c-c000.json
2026-04-09 12:31:31        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-8713a679-610c-4943-b687-76d5f5df47ce-c000.json
2026-04-09 12:33:45        964 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-925ad965-07b7-4758-802b-7638f25d3248-c000.json
2026-04-09 12:40:33        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-9775b4c7-5469-4992-93e8-c1b65b1ccd6f-c000.json
2026-04-09 12:33:28        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-99ca2f39-41bf-46e6-96b6-932b3a95df93-c000.json
2026-04-09 12:31:16        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-9a834fbf-37ae-44c5-aaa6-7827999cb232-c000.json
2026-04-09 12:37:23        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-9edc7868-02cd-40c9-a1ea-1ad467405fe2-c000.json
2026-04-09 12:40:02        809 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-a13660d3-067e-4328-b2e4-8366ecbd386c-c000.json
2026-04-09 12:36:14        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-a532d5fb-eeb3-40ba-b25e-1852d376c5d0-c000.json
2026-04-09 12:38:27        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-a587aa6f-94f1-420c-954b-e822a74656c9-c000.json
2026-04-09 12:39:17        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-b28fe10b-dda6-4110-91cd-a01ff0a4a3c2-c000.json
2026-04-09 12:31:49        206 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-b31b6c66-3b76-4a9d-a2ac-6ebb08c235a5-c000.json
2026-04-09 12:30:43        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-bd82b3e6-0f39-48bf-b5b8-3311e2a1da49-c000.json
2026-04-09 12:32:57        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-c8388068-3b3a-414c-8d90-078c311c2261-c000.json
2026-04-09 12:39:31        811 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-dde91887-96f7-429f-93ca-081c44c870bb-c000.json
2026-04-09 12:37:06        776 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-e4d11ea8-fdfd-4ded-b47e-87aa070c0fde-c000.json
2026-04-09 12:38:13        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-e5f99e82-a54c-45b3-a68b-f3b101250f51-c000.json
2026-04-09 12:34:01        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-e838920c-46d9-4e49-a417-1956581e438f-c000.json
2026-04-09 12:40:17        811 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-e96b0071-41cf-4d7e-aec9-4fdac317a38a-c000.json
2026-04-09 12:32:42        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00000-ed4b162f-e01c-49ac-a816-41f787808f82-c000.json
2026-04-09 12:30:25        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-0cff3f48-bc63-4043-8dce-634ee9b12ad9-c000.json
2026-04-09 12:35:30        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-0e487b7c-152b-40e3-9464-21d8c5ea2653-c000.json
2026-04-09 12:37:57        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-157bb902-a3c5-414f-9c01-6744457b93df-c000.json
2026-04-09 12:30:59        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-198298ec-69aa-46f9-bcf8-36162b457875-c000.json
2026-04-09 12:30:07        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-21a4b096-494f-42e4-b8a1-4255d7d96962-c000.json
2026-04-09 12:35:54        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-2aa8e8fc-e1c1-4b4d-be69-fae34c3257d2-c000.json
2026-04-09 12:38:44        949 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-2b3072c0-1a06-46f6-8e4d-b44fac206676-c000.json
2026-04-09 12:29:12        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-2d5faa19-dffa-4e2c-afc4-fcfa90c82fba-c000.json
2026-04-09 12:39:47        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-44786efe-da80-4e89-a2a9-31053de9613a-c000.json
2026-04-09 12:29:31        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-4762ba17-6c99-48f6-98a7-797202f45ea8-c000.json
2026-04-09 12:34:33        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-4b336c47-6f38-45a6-8f9e-6ecca6afade6-c000.json
2026-04-09 12:28:51        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-4efe3700-1b01-42e0-8772-74178bb62d77-c000.json
2026-04-09 12:34:17        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-52c679be-3dec-4e05-ac5e-8741b7de6e63-c000.json
2026-04-09 12:36:50        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-5331719e-1df4-424c-8cad-3e81c35465a3-c000.json
2026-04-09 12:32:26        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-5a7ae2c3-6058-4742-9b73-0d8cd4534610-c000.json
2026-04-09 12:33:13        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-6970d210-8b53-4e31-945d-5f821ae82cbf-c000.json
2026-04-09 12:32:06        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-6efe0857-46ca-4aca-9e80-af7216f27b36-c000.json
2026-04-09 12:37:41        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-7744f57d-2959-42f4-8c4f-9adeb8d1a10f-c000.json
2026-04-09 12:29:48        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-7b4a9e74-d5a0-4fd5-9244-00e6355bfbd9-c000.json
2026-04-09 12:39:01        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-7fad3ad6-49a4-4fe6-852e-f83f63a868ce-c000.json
2026-04-09 12:36:32        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-814def8f-08ce-4adf-bb7d-cd82971a331c-c000.json
2026-04-09 12:31:32        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-8713a679-610c-4943-b687-76d5f5df47ce-c000.json
2026-04-09 12:33:45        566 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-925ad965-07b7-4758-802b-7638f25d3248-c000.json
2026-04-09 12:40:34        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-9775b4c7-5469-4992-93e8-c1b65b1ccd6f-c000.json
2026-04-09 12:33:29        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-99ca2f39-41bf-46e6-96b6-932b3a95df93-c000.json
2026-04-09 12:31:16        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-9a834fbf-37ae-44c5-aaa6-7827999cb232-c000.json
2026-04-09 12:37:24        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-9edc7868-02cd-40c9-a1ea-1ad467405fe2-c000.json
2026-04-09 12:40:02        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-a13660d3-067e-4328-b2e4-8366ecbd386c-c000.json
2026-04-09 12:36:14        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-a532d5fb-eeb3-40ba-b25e-1852d376c5d0-c000.json
2026-04-09 12:38:28        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-a587aa6f-94f1-420c-954b-e822a74656c9-c000.json
2026-04-09 12:39:17        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-b28fe10b-dda6-4110-91cd-a01ff0a4a3c2-c000.json
2026-04-09 12:31:49        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-b31b6c66-3b76-4a9d-a2ac-6ebb08c235a5-c000.json
2026-04-09 12:30:43        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-bd82b3e6-0f39-48bf-b5b8-3311e2a1da49-c000.json
2026-04-09 12:32:57        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-c8388068-3b3a-414c-8d90-078c311c2261-c000.json
2026-04-09 12:39:31        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-dde91887-96f7-429f-93ca-081c44c870bb-c000.json
2026-04-09 12:37:07        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-e4d11ea8-fdfd-4ded-b47e-87aa070c0fde-c000.json
2026-04-09 12:38:13        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-e5f99e82-a54c-45b3-a68b-f3b101250f51-c000.json
2026-04-09 12:34:01        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-e838920c-46d9-4e49-a417-1956581e438f-c000.json
2026-04-09 12:40:18        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-e96b0071-41cf-4d7e-aec9-4fdac317a38a-c000.json
2026-04-09 12:32:43        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00001-ed4b162f-e01c-49ac-a816-41f787808f82-c000.json
2026-04-09 12:30:25        570 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-0cff3f48-bc63-4043-8dce-634ee9b12ad9-c000.json
2026-04-09 12:35:30        764 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-0e487b7c-152b-40e3-9464-21d8c5ea2653-c000.json
2026-04-09 12:37:57        776 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-157bb902-a3c5-414f-9c01-6744457b93df-c000.json
2026-04-09 12:30:59        570 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-198298ec-69aa-46f9-bcf8-36162b457875-c000.json
2026-04-09 12:30:07        586 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-21a4b096-494f-42e4-b8a1-4255d7d96962-c000.json
2026-04-09 12:35:54        778 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-2aa8e8fc-e1c1-4b4d-be69-fae34c3257d2-c000.json
2026-04-09 12:38:45        756 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-2b3072c0-1a06-46f6-8e4d-b44fac206676-c000.json
2026-04-09 12:29:13        570 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-2d5faa19-dffa-4e2c-afc4-fcfa90c82fba-c000.json
2026-04-09 12:29:31        587 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-4762ba17-6c99-48f6-98a7-797202f45ea8-c000.json
2026-04-09 12:34:33        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-4b336c47-6f38-45a6-8f9e-6ecca6afade6-c000.json
2026-04-09 12:28:52        587 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-4efe3700-1b01-42e0-8772-74178bb62d77-c000.json
2026-04-09 12:34:18        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-52c679be-3dec-4e05-ac5e-8741b7de6e63-c000.json
2026-04-09 12:36:51        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-5331719e-1df4-424c-8cad-3e81c35465a3-c000.json
2026-04-09 12:32:26        571 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-5a7ae2c3-6058-4742-9b73-0d8cd4534610-c000.json
2026-04-09 12:33:14        570 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-6970d210-8b53-4e31-945d-5f821ae82cbf-c000.json
2026-04-09 12:32:07        570 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-6efe0857-46ca-4aca-9e80-af7216f27b36-c000.json
2026-04-09 12:37:41        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-7744f57d-2959-42f4-8c4f-9adeb8d1a10f-c000.json
2026-04-09 12:29:49        570 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-7b4a9e74-d5a0-4fd5-9244-00e6355bfbd9-c000.json
2026-04-09 12:36:32        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-814def8f-08ce-4adf-bb7d-cd82971a331c-c000.json
2026-04-09 12:31:32        570 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-8713a679-610c-4943-b687-76d5f5df47ce-c000.json
2026-04-09 12:33:46       1341 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-925ad965-07b7-4758-802b-7638f25d3248-c000.json
2026-04-09 12:33:29        587 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-99ca2f39-41bf-46e6-96b6-932b3a95df93-c000.json
2026-04-09 12:31:17        587 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-9a834fbf-37ae-44c5-aaa6-7827999cb232-c000.json
2026-04-09 12:37:24        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-9edc7868-02cd-40c9-a1ea-1ad467405fe2-c000.json
2026-04-09 12:36:15        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-a532d5fb-eeb3-40ba-b25e-1852d376c5d0-c000.json
2026-04-09 12:38:28        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-a587aa6f-94f1-420c-954b-e822a74656c9-c000.json
2026-04-09 12:31:50        587 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-b31b6c66-3b76-4a9d-a2ac-6ebb08c235a5-c000.json
2026-04-09 12:30:43        587 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-bd82b3e6-0f39-48bf-b5b8-3311e2a1da49-c000.json
2026-04-09 12:32:58        587 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-c8388068-3b3a-414c-8d90-078c311c2261-c000.json
2026-04-09 12:37:07        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-e4d11ea8-fdfd-4ded-b47e-87aa070c0fde-c000.json
2026-04-09 12:38:13        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-e5f99e82-a54c-45b3-a68b-f3b101250f51-c000.json
2026-04-09 12:34:02        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-e838920c-46d9-4e49-a417-1956581e438f-c000.json
2026-04-09 12:32:43        570 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00002-ed4b162f-e01c-49ac-a816-41f787808f82-c000.json
2026-04-09 12:30:26        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-0cff3f48-bc63-4043-8dce-634ee9b12ad9-c000.json
2026-04-09 12:35:31        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-0e487b7c-152b-40e3-9464-21d8c5ea2653-c000.json
2026-04-09 12:37:58        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-157bb902-a3c5-414f-9c01-6744457b93df-c000.json
2026-04-09 12:31:00        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-198298ec-69aa-46f9-bcf8-36162b457875-c000.json
2026-04-09 12:30:08        776 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-21a4b096-494f-42e4-b8a1-4255d7d96962-c000.json
2026-04-09 12:35:54        208 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-2aa8e8fc-e1c1-4b4d-be69-fae34c3257d2-c000.json
2026-04-09 12:38:45        569 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-2b3072c0-1a06-46f6-8e4d-b44fac206676-c000.json
2026-04-09 12:29:13        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-2d5faa19-dffa-4e2c-afc4-fcfa90c82fba-c000.json
2026-04-09 12:39:47        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-44786efe-da80-4e89-a2a9-31053de9613a-c000.json
2026-04-09 12:29:32        765 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-4762ba17-6c99-48f6-98a7-797202f45ea8-c000.json
2026-04-09 12:34:34        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-4b336c47-6f38-45a6-8f9e-6ecca6afade6-c000.json
2026-04-09 12:28:52        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-4efe3700-1b01-42e0-8772-74178bb62d77-c000.json
2026-04-09 12:34:18        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-52c679be-3dec-4e05-ac5e-8741b7de6e63-c000.json
2026-04-09 12:36:51        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-5331719e-1df4-424c-8cad-3e81c35465a3-c000.json
2026-04-09 12:32:26        776 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-5a7ae2c3-6058-4742-9b73-0d8cd4534610-c000.json
2026-04-09 12:33:14        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-6970d210-8b53-4e31-945d-5f821ae82cbf-c000.json
2026-04-09 12:32:07        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-6efe0857-46ca-4aca-9e80-af7216f27b36-c000.json
2026-04-09 12:37:41        208 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-7744f57d-2959-42f4-8c4f-9adeb8d1a10f-c000.json
2026-04-09 12:29:49        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-7b4a9e74-d5a0-4fd5-9244-00e6355bfbd9-c000.json
2026-04-09 12:39:01        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-7fad3ad6-49a4-4fe6-852e-f83f63a868ce-c000.json
2026-04-09 12:36:33        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-814def8f-08ce-4adf-bb7d-cd82971a331c-c000.json
2026-04-09 12:31:32        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-8713a679-610c-4943-b687-76d5f5df47ce-c000.json
2026-04-09 12:33:46        958 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-925ad965-07b7-4758-802b-7638f25d3248-c000.json
2026-04-09 12:40:34        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-9775b4c7-5469-4992-93e8-c1b65b1ccd6f-c000.json
2026-04-09 12:33:30        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-99ca2f39-41bf-46e6-96b6-932b3a95df93-c000.json
2026-04-09 12:31:17        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-9a834fbf-37ae-44c5-aaa6-7827999cb232-c000.json
2026-04-09 12:37:24        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-9edc7868-02cd-40c9-a1ea-1ad467405fe2-c000.json
2026-04-09 12:40:02        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-a13660d3-067e-4328-b2e4-8366ecbd386c-c000.json
2026-04-09 12:36:15        192 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-a532d5fb-eeb3-40ba-b25e-1852d376c5d0-c000.json
2026-04-09 12:38:29        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-a587aa6f-94f1-420c-954b-e822a74656c9-c000.json
2026-04-09 12:39:17        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-b28fe10b-dda6-4110-91cd-a01ff0a4a3c2-c000.json
2026-04-09 12:31:50        776 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-b31b6c66-3b76-4a9d-a2ac-6ebb08c235a5-c000.json
2026-04-09 12:30:44        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-bd82b3e6-0f39-48bf-b5b8-3311e2a1da49-c000.json
2026-04-09 12:32:58        777 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-c8388068-3b3a-414c-8d90-078c311c2261-c000.json
2026-04-09 12:39:32        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-dde91887-96f7-429f-93ca-081c44c870bb-c000.json
2026-04-09 12:37:08        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-e4d11ea8-fdfd-4ded-b47e-87aa070c0fde-c000.json
2026-04-09 12:38:14        190 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-e5f99e82-a54c-45b3-a68b-f3b101250f51-c000.json
2026-04-09 12:34:02        207 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-e838920c-46d9-4e49-a417-1956581e438f-c000.json
2026-04-09 12:40:18        380 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-e96b0071-41cf-4d7e-aec9-4fdac317a38a-c000.json
2026-04-09 12:32:43        760 evaluations/baseline-failure-recovery-20260409t122500z/breach_windows/part-00003-ed4b162f-e01c-49ac-a816-41f787808f82-c000.json
2026-04-09 12:39:11         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/100
2026-04-09 12:39:18         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/101
2026-04-09 12:39:22         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/102
2026-04-09 12:39:27         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/103
2026-04-09 12:39:32         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/104
2026-04-09 12:39:36         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/105
2026-04-09 12:39:42         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/106
2026-04-09 12:39:47         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/107
2026-04-09 12:39:51         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/108
2026-04-09 12:39:56         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/109
2026-04-09 12:40:02         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/110
2026-04-09 12:40:07         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/111
2026-04-09 12:40:12         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/112
2026-04-09 12:40:18         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/113
2026-04-09 12:40:22         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/114
2026-04-09 12:40:28         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/115
2026-04-09 12:40:34         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/116
2026-04-09 12:40:38         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/117
2026-04-09 12:40:43         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/118
2026-04-09 12:40:47         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/119
2026-04-09 12:40:52         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/120
2026-04-09 12:40:57         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/121
2026-04-09 12:41:02         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/122
2026-04-09 12:41:06         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/123
2026-04-09 12:41:10         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/124
2026-04-09 12:41:16         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/125
2026-04-09 12:41:20         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/126
2026-04-09 12:41:29         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/127
2026-04-09 12:41:43         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/128
2026-04-09 12:41:50         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/129
2026-04-09 12:31:39         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/29
2026-04-09 12:31:44         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/30
2026-04-09 12:31:50         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/31
2026-04-09 12:31:55         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/32
2026-04-09 12:32:01         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/33
2026-04-09 12:32:08         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/34
2026-04-09 12:32:13         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/35
2026-04-09 12:32:19         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/36
2026-04-09 12:32:26         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/37
2026-04-09 12:32:31         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/38
2026-04-09 12:32:37         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/39
2026-04-09 12:32:43         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/40
2026-04-09 12:32:48         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/41
2026-04-09 12:32:53         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/42
2026-04-09 12:32:58         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/43
2026-04-09 12:33:03         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/44
2026-04-09 12:33:08         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/45
2026-04-09 12:33:14         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/46
2026-04-09 12:33:19         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/47
2026-04-09 12:33:24         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/48
2026-04-09 12:33:30         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/49
2026-04-09 12:33:34         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/50
2026-04-09 12:33:40         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/51
2026-04-09 12:33:47         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/52
2026-04-09 12:33:51         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/53
2026-04-09 12:33:56         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/54
2026-04-09 12:34:02         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/55
2026-04-09 12:34:07         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/56
2026-04-09 12:34:13         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/57
2026-04-09 12:34:18         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/58
2026-04-09 12:34:23         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/59
2026-04-09 12:34:28         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/60
2026-04-09 12:34:34         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/61
2026-04-09 12:34:38         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/62
2026-04-09 12:34:44         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/63
2026-04-09 12:35:30         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/64
2026-04-09 12:35:40         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/65
2026-04-09 12:35:53         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/66
2026-04-09 12:36:01         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/67
2026-04-09 12:36:10         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/68
2026-04-09 12:36:18         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/69
2026-04-09 12:36:22         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/70
2026-04-09 12:36:29         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/71
2026-04-09 12:36:36         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/72
2026-04-09 12:36:43         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/73
2026-04-09 12:36:51         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/74
2026-04-09 12:36:56         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/75
2026-04-09 12:37:02         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/76
2026-04-09 12:37:08         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/77
2026-04-09 12:37:14         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/78
2026-04-09 12:37:20         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/79
2026-04-09 12:37:26         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/80
2026-04-09 12:37:30         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/81
2026-04-09 12:37:37         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/82
2026-04-09 12:37:43         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/83
2026-04-09 12:37:48         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/84
2026-04-09 12:37:54         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/85
2026-04-09 12:37:59         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/86
2026-04-09 12:38:04         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/87
2026-04-09 12:38:10         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/88
2026-04-09 12:38:15         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/89
2026-04-09 12:38:19         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/90
2026-04-09 12:38:24         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/91
2026-04-09 12:38:30         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/92
2026-04-09 12:38:35         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/93
2026-04-09 12:38:39         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/94
2026-04-09 12:38:45         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/95
2026-04-09 12:38:50         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/96
2026-04-09 12:38:56         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/97
2026-04-09 12:39:02         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/98
2026-04-09 12:39:07         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/commits/99
2026-04-09 12:28:13         45 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/metadata
2026-04-09 12:39:07        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/100
2026-04-09 12:39:12        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/101
2026-04-09 12:39:18        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/102
2026-04-09 12:39:23        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/103
2026-04-09 12:39:27        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/104
2026-04-09 12:39:32        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/105
2026-04-09 12:39:37        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/106
2026-04-09 12:39:42        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/107
2026-04-09 12:39:48        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/108
2026-04-09 12:39:52        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/109
2026-04-09 12:39:57        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/110
2026-04-09 12:40:03        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/111
2026-04-09 12:40:08        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/112
2026-04-09 12:40:13        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/113
2026-04-09 12:40:19        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/114
2026-04-09 12:40:23        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/115
2026-04-09 12:40:28        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/116
2026-04-09 12:40:34        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/117
2026-04-09 12:40:38        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/118
2026-04-09 12:40:43        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/119
2026-04-09 12:40:47        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/120
2026-04-09 12:40:53        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/121
2026-04-09 12:40:57        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/122
2026-04-09 12:41:02        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/123
2026-04-09 12:41:06        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/124
2026-04-09 12:41:11        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/125
2026-04-09 12:41:16        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/126
2026-04-09 12:41:21        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/127
2026-04-09 12:41:30        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/128
2026-04-09 12:41:43        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/129
2026-04-09 12:31:34        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/29
2026-04-09 12:31:40        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/30
2026-04-09 12:31:44        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/31
2026-04-09 12:31:51        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/32
2026-04-09 12:31:55        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/33
2026-04-09 12:32:01        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/34
2026-04-09 12:32:08        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/35
2026-04-09 12:32:13        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/36
2026-04-09 12:32:19        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/37
2026-04-09 12:32:27        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/38
2026-04-09 12:32:32        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/39
2026-04-09 12:32:37        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/40
2026-04-09 12:32:44        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/41
2026-04-09 12:32:48        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/42
2026-04-09 12:32:53        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/43
2026-04-09 12:32:59        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/44
2026-04-09 12:33:03        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/45
2026-04-09 12:33:09        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/46
2026-04-09 12:33:15        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/47
2026-04-09 12:33:19        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/48
2026-04-09 12:33:25        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/49
2026-04-09 12:33:30        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/50
2026-04-09 12:33:35        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/51
2026-04-09 12:33:40        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/52
2026-04-09 12:33:47        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/53
2026-04-09 12:33:52        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/54
2026-04-09 12:33:57        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/55
2026-04-09 12:34:02        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/56
2026-04-09 12:34:08        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/57
2026-04-09 12:34:14        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/58
2026-04-09 12:34:19        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/59
2026-04-09 12:34:24        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/60
2026-04-09 12:34:28        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/61
2026-04-09 12:34:34        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/62
2026-04-09 12:34:39        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/63
2026-04-09 12:34:44        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/64
2026-04-09 12:35:30        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/65
2026-04-09 12:35:41        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/66
2026-04-09 12:35:54        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/67
2026-04-09 12:36:01        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/68
2026-04-09 12:36:10        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/69
2026-04-09 12:36:18        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/70
2026-04-09 12:36:23        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/71
2026-04-09 12:36:29        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/72
2026-04-09 12:36:36        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/73
2026-04-09 12:36:44        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/74
2026-04-09 12:36:51        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/75
2026-04-09 12:36:57        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/76
2026-04-09 12:37:02        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/77
2026-04-09 12:37:08        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/78
2026-04-09 12:37:14        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/79
2026-04-09 12:37:21        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/80
2026-04-09 12:37:26        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/81
2026-04-09 12:37:31        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/82
2026-04-09 12:37:38        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/83
2026-04-09 12:37:43        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/84
2026-04-09 12:37:48        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/85
2026-04-09 12:37:54        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/86
2026-04-09 12:37:59        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/87
2026-04-09 12:38:05        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/88
2026-04-09 12:38:11        757 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/89
2026-04-09 12:38:15        758 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/90
2026-04-09 12:38:20        761 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/91
2026-04-09 12:38:25        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/92
2026-04-09 12:38:30        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/93
2026-04-09 12:38:35        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/94
2026-04-09 12:38:40        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/95
2026-04-09 12:38:46        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/96
2026-04-09 12:38:50        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/97
2026-04-09 12:38:56        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/98
2026-04-09 12:39:02        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/offsets/99
2026-04-09 12:28:14        104 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/classified_side_effects/sources/0/0
2026-04-09 12:28:34         29 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/0
2026-04-09 12:28:58         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/1
2026-04-09 12:31:37         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/10
2026-04-09 12:31:55         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/11
2026-04-09 12:32:12         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/12
2026-04-09 12:32:31         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/13
2026-04-09 12:32:47         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/14
2026-04-09 12:33:02         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/15
2026-04-09 12:33:18         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/16
2026-04-09 12:33:34         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/17
2026-04-09 12:33:50         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/18
2026-04-09 12:34:06         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/19
2026-04-09 12:29:17         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/2
2026-04-09 12:34:22         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/20
2026-04-09 12:34:38         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/21
2026-04-09 12:35:37         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/22
2026-04-09 12:36:00         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/23
2026-04-09 12:36:20         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/24
2026-04-09 12:36:36         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/25
2026-04-09 12:36:54         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/26
2026-04-09 12:37:11         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/27
2026-04-09 12:37:28         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/28
2026-04-09 12:37:45         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/29
2026-04-09 12:29:35         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/3
2026-04-09 12:38:01         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/30
2026-04-09 12:38:17         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/31
2026-04-09 12:38:32         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/32
2026-04-09 12:38:49         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/33
2026-04-09 12:39:06         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/34
2026-04-09 12:39:21         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/35
2026-04-09 12:39:35         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/36
2026-04-09 12:39:51         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/37
2026-04-09 12:40:07         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/38
2026-04-09 12:40:22         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/39
2026-04-09 12:29:53         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/4
2026-04-09 12:40:37         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/40
2026-04-09 12:40:45         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/41
2026-04-09 12:40:53         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/42
2026-04-09 12:41:02         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/43
2026-04-09 12:41:10         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/44
2026-04-09 12:41:19         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/45
2026-04-09 12:41:29         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/46
2026-04-09 12:41:39         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/47
2026-04-09 12:41:49         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/48
2026-04-09 12:30:12         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/5
2026-04-09 12:30:30         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/6
2026-04-09 12:30:48         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/7
2026-04-09 12:31:05         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/8
2026-04-09 12:31:20         41 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/commits/9
2026-04-09 12:28:11         45 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/metadata
2026-04-09 12:28:14        739 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/0
2026-04-09 12:28:34        751 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/1
2026-04-09 12:31:21        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/10
2026-04-09 12:31:37        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/11
2026-04-09 12:31:55        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/12
2026-04-09 12:32:13        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/13
2026-04-09 12:32:32        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/14
2026-04-09 12:32:47        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/15
2026-04-09 12:33:02        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/16
2026-04-09 12:33:18        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/17
2026-04-09 12:33:34        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/18
2026-04-09 12:33:50        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/19
2026-04-09 12:28:58        763 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/2
2026-04-09 12:34:07        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/20
2026-04-09 12:34:22        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/21
2026-04-09 12:34:38        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/22
2026-04-09 12:35:38        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/23
2026-04-09 12:36:00        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/24
2026-04-09 12:36:20        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/25
2026-04-09 12:36:36        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/26
2026-04-09 12:36:55        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/27
2026-04-09 12:37:12        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/28
2026-04-09 12:37:29        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/29
2026-04-09 12:29:18        765 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/3
2026-04-09 12:37:46        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/30
2026-04-09 12:38:02        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/31
2026-04-09 12:38:18        771 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/32
2026-04-09 12:38:33        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/33
2026-04-09 12:38:50        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/34
2026-04-09 12:39:06        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/35
2026-04-09 12:39:22        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/36
2026-04-09 12:39:35        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/37
2026-04-09 12:39:51        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/38
2026-04-09 12:40:07        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/39
2026-04-09 12:29:36        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/4
2026-04-09 12:40:22        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/40
2026-04-09 12:40:38        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/41
2026-04-09 12:40:45        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/42
2026-04-09 12:40:53        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/43
2026-04-09 12:41:02        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/44
2026-04-09 12:41:11        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/45
2026-04-09 12:41:20        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/46
2026-04-09 12:41:29        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/47
2026-04-09 12:41:39        775 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/48
2026-04-09 12:29:54        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/5
2026-04-09 12:30:13        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/6
2026-04-09 12:30:31        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/7
2026-04-09 12:30:48        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/8
2026-04-09 12:31:05        769 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/offsets/9
2026-04-09 12:28:14        104 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/sources/0/0
2026-04-09 12:31:18       2453 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.10.delta.40f926bd-30a2-4fc7-9a7e-f71ab91ad0a0.TID1632.tmp
2026-04-09 12:31:07       2453 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.10.delta.a7e5fe44-552f-407d-8b73-2d12eae2d9a4.TID1517.tmp
2026-04-09 12:31:23       2411 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.11.delta.64172953-162f-4275-81eb-15e574e8c105.TID1710.tmp
2026-04-09 12:31:33       2411 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.11.delta.ac81fcc9-922d-4f87-898a-eb41fa598cc2.TID1816.tmp
2026-04-09 12:31:51       2302 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.12.delta.9c408f2f-1d16-4e8a-919d-4e78816293f7.TID2022.tmp
2026-04-09 12:31:39       2302 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.12.delta.f75c5721-e6ba-4533-bfe9-85398dd535be.TID1898.tmp
2026-04-09 12:31:56       2961 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.13.delta.bc28e7d9-c783-494e-a5c3-084b5c68cad9.TID2093.tmp
2026-04-09 12:32:08       2961 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.13.delta.e8910b41-6ea7-43cc-9248-c819df7e16a5.TID2212.tmp
2026-04-09 12:32:28       2655 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.14.delta.6e2dd2b2-7542-41b9-bc22-d3f8bd559f9b.TID2407.tmp
2026-04-09 12:32:14       2655 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.14.delta.7e3c561a-4a5f-481f-a494-a8d009144e96.TID2283.tmp
2026-04-09 12:32:45       2933 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.15.delta.6b956ca3-5f9f-40ae-b823-1bf79208a119.TID2597.tmp
2026-04-09 12:32:33       2933 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.15.delta.9074d8cd-4339-4d80-9ec3-9dab3b9ec9f2.TID2479.tmp
2026-04-09 12:32:48       2531 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.16.delta.5be2f1e9-fece-4484-ba4a-25a2526a208f.TID2660.tmp
2026-04-09 12:32:59       2531 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.16.delta.e32e6e53-6c60-488f-9669-db6ee1752c79.TID2783.tmp
2026-04-09 12:33:15       2261 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.17.delta.ad6cfcf1-b67d-4bdf-8b10-afd8c23c742f.TID2972.tmp
2026-04-09 12:33:04       2261 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.17.delta.e6375c93-1599-4eb8-8e13-a5c132a93ab4.TID2850.tmp
2026-04-09 12:33:30       2212 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.18.delta.3dd28770-664d-4ff9-a684-e25968e2d020.TID3162.tmp
2026-04-09 12:33:19       2212 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.18.delta.afc6ce62-6e5c-4db1-8151-4511e61631e1.TID3040.tmp
2026-04-09 12:33:47       2248 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.19.delta.d4971a6f-64d4-4c93-a0b4-21687e853c6d.TID3352.tmp
2026-04-09 12:33:36       2248 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.19.delta.efb0073e-23b4-4426-9fa1-8dacae571b96.TID3233.tmp
2026-04-09 12:28:38       2311 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.2.delta.65744e4e-891e-4ec3-84ef-360a1a0ad200.TID92.tmp
2026-04-09 12:28:54       2311 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.2.delta.d9d81eb0-321e-4642-8a59-13cfe6fd9b4f.TID179.tmp
2026-04-09 12:34:03       2322 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.20.delta.0ada01ca-edc1-45d6-909c-68024d0686c5.TID3542.tmp
2026-04-09 12:33:51       2322 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.20.delta.9875fb99-1f4b-4911-a0c9-ec0c32d7032c.TID3420.tmp
2026-04-09 12:34:07       2489 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.21.delta.11f29b03-6c1d-4d7c-94c0-727c484178c7.TID3610.tmp
2026-04-09 12:34:19       2489 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.21.delta.7a6ebd24-e887-42a8-95e7-81900f5daf0c.TID3732.tmp
2026-04-09 12:34:35       2323 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.22.delta.609783c7-137a-49a4-a9f2-abfa9e8b3717.TID3922.tmp
2026-04-09 12:34:23       2323 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.22.delta.7c92e410-332f-4771-a5d2-2341b3d4f833.TID3800.tmp
2026-04-09 12:35:33       2344 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.23.delta.0f0f5885-66ea-4fe2-aa67-36a7505a924f.TID82.tmp
2026-04-09 12:34:39       2344 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.23.delta.4198f30b-1492-449b-967c-3056f0a4fe32.TID3991.tmp
2026-04-09 12:35:15       2344 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.23.delta.b3fc7601-8bf2-4e19-9715-7e038d3e443c.TID10.tmp
2026-04-09 12:35:41       9827 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.24.delta.6c6ae272-ffbf-4d80-9257-195b6e1881ee.TID145.tmp
2026-04-09 12:35:56       9827 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.24.delta.9edb296f-3dc9-41e6-bac6-7f4b21dafb0c.TID221.tmp
2026-04-09 12:36:16       3222 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.25.delta.d75c5491-6c12-45e4-a6ea-b6cb796644ce.TID377.tmp
2026-04-09 12:36:22       3241 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.26.delta.b4d2e5d4-99be-4a68-bc4f-792e1c970d26.TID473.tmp
2026-04-09 12:36:34       3241 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.26.delta.bb182c00-5b47-4a46-829a-35865c29462c.TID573.tmp
2026-04-09 12:36:52       2476 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.27.delta.07e0954b-c943-458c-aeb1-6562aa7e6826.TID734.tmp
2026-04-09 12:36:39       2476 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.27.delta.23315236-fe51-4db0-8907-dfb6eb1737d1.TID619.tmp
2026-04-09 12:37:08       2698 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.28.delta.34833e41-cc8b-4165-9776-2b8ab973d14a.TID923.tmp
2026-04-09 12:36:56       2698 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.28.delta.bd228a43-2855-47c0-a176-e80c6abbfdb9.TID801.tmp
2026-04-09 12:37:26       2442 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.29.delta.63292eee-8801-4d9a-831b-84071aac2415.TID1112.tmp
2026-04-09 12:37:14       2442 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.29.delta.d9315b7b-e06d-4088-93bb-844fe4fbdae3.TID991.tmp
2026-04-09 12:29:01       3389 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.3.delta.8763ceee-bf41-4ee3-ad4e-37330b94434d.TID245.tmp
2026-04-09 12:29:14       3389 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.3.delta.f1b561d9-b548-458b-8842-5df57dc73b9a.TID337.tmp
2026-04-09 12:37:31       2251 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.30.delta.0e5de905-f8d7-44b7-b7a9-2746764687ba.TID1181.tmp
2026-04-09 12:37:42       2251 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.30.delta.2ef34aca-58ac-48e6-969c-54179f611d63.TID1293.tmp
2026-04-09 12:37:47       2650 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.31.delta.a52e2f2a-e384-4682-9433-e0175990c8e3.TID1370.tmp
2026-04-09 12:37:59       2650 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.31.delta.cdad4560-52a9-4d10-b3f1-ad5c1ae734a8.TID1493.tmp
2026-04-09 12:38:15       2398 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.32.delta.2a9737a4-15dc-43c7-baaf-34734214bba0.TID1682.tmp
2026-04-09 12:38:04       2398 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.32.delta.5b4a5317-e068-4251-812f-e8ad9ddc4dac.TID1561.tmp
2026-04-09 12:38:19       2234 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.33.delta.92b3a511-213b-40ac-99bd-8ce8b79f049d.TID1751.tmp
2026-04-09 12:38:30       2234 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.33.delta.d2f5066f-a090-43a4-978b-430701b76a1f.TID1873.tmp
2026-04-09 12:38:46       2243 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.34.delta.2239e0d5-3ed3-4a5c-8238-d724a5d6431c.TID2063.tmp
2026-04-09 12:38:34       2243 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.34.delta.473d071f-9ecb-48d7-b51f-957bf390f72a.TID1941.tmp
2026-04-09 12:38:51       2586 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.35.delta.37a22d95-850e-4272-b597-eabfe887c4bd.TID2132.tmp
2026-04-09 12:39:03       2586 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.35.delta.6c460c32-da8d-4884-8cdc-c9d14b53b9f7.TID2253.tmp
2026-04-09 12:39:18       2456 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.36.delta.5f336993-1a38-4ede-8320-b2c2a81c963b.TID2443.tmp
2026-04-09 12:39:08       2456 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.36.delta.ff9f7da9-7270-4918-b2cf-3d9cd2cc5d43.TID2322.tmp
2026-04-09 12:39:32       2400 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.37.delta.66de7859-c080-4112-9145-ffccc0cc1d00.TID2633.tmp
2026-04-09 12:39:23       2400 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.37.delta.dd4dc9ff-8148-4513-9c91-0f1c50119223.TID2512.tmp
2026-04-09 12:39:49       2028 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.38.delta.1442316d-6f60-490e-965b-41f1c9924d33.TID2828.tmp
2026-04-09 12:39:37       2028 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.38.delta.15b6eee1-d5aa-40c3-b15b-cabae66e054a.TID2701.tmp
2026-04-09 12:40:03       2357 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.39.delta.0a8e3f0e-8ea9-493a-8405-dea23b6c80e2.TID3013.tmp
2026-04-09 12:39:53       2357 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.39.delta.e6743c30-8830-4cd0-9b65-fcaf4fbe13e1.TID2895.tmp
2026-04-09 12:29:32       2997 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.4.delta.a0a8d810-3c90-4fff-8056-493efd135b5d.TID502.tmp
2026-04-09 12:29:20       2997 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.4.delta.cd56b7f7-4f25-4ca4-8562-36daa45661b0.TID389.tmp
2026-04-09 12:40:19       2592 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.40.delta.17bd9736-b561-415d-a7f9-666d5002f483.TID3203.tmp
2026-04-09 12:40:08       2592 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.40.delta.96d79a3f-88ae-493c-8b28-4826533038da.TID3082.tmp
2026-04-09 12:40:35       1631 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.41.delta.272d02e3-38e7-406e-ab1e-e788796694a6.TID3393.tmp
2026-04-09 12:40:23       1631 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.41.delta.c6035393-130a-4a38-8e12-6f03e05569ea.TID3272.tmp
2026-04-09 12:29:38       3026 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.5.delta.3b7423d2-3896-4601-a90d-8ae9ffdf2f72.TID570.tmp
2026-04-09 12:29:50       3026 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.5.delta.c9aae7dc-3761-423c-a756-4e271f1593dd.TID686.tmp
2026-04-09 12:30:09       2581 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.6.delta.ca19c9da-0232-4080-8c41-45f74b445a92.TID877.tmp
2026-04-09 12:29:55       2581 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.6.delta.dba3a81a-db7b-46ef-8b52-b7f332ed2f14.TID760.tmp
2026-04-09 12:30:27       2932 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.7.delta.386f63eb-5f31-4398-86f9-18c4bc1d8547.TID1056.tmp
2026-04-09 12:30:14       2932 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.7.delta.3ffc3495-635c-44df-aef5-db61f2d14f66.TID950.tmp
2026-04-09 12:30:45       2935 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.8.delta.0a76c645-5236-44e8-83f3-e6846ee9fb2c.TID1246.tmp
2026-04-09 12:30:33       2935 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.8.delta.2c0d42b8-1fe2-48b5-b821-c22023595a83.TID1140.tmp
2026-04-09 12:30:50       2436 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.9.delta.09593112-89b5-4d2b-93d9-b8f92cd96cbc.TID1330.tmp
2026-04-09 12:31:01       2436 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/.9.delta.0f09a94b-0782-46d1-8256-ae6051805f61.TID1436.tmp
2026-04-09 12:28:32         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/1.delta
2026-04-09 12:31:18       2453 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/10.delta
2026-04-09 12:31:34       2411 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/11.delta
2026-04-09 12:31:52       2302 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/12.delta
2026-04-09 12:32:09       2961 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/13.delta
2026-04-09 12:32:29       2655 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/14.delta
2026-04-09 12:32:23      40061 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/14.snapshot
2026-04-09 12:32:45       2933 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/15.delta
2026-04-09 12:33:00       2531 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/16.delta
2026-04-09 12:33:16       2261 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/17.delta
2026-04-09 12:33:31       2212 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/18.delta
2026-04-09 12:33:48       2248 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/19.delta
2026-04-09 12:28:56       2311 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/2.delta
2026-04-09 12:34:04       2322 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/20.delta
2026-04-09 12:34:20       2489 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/21.delta
2026-04-09 12:34:36       2323 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/22.delta
2026-04-09 12:35:34       2344 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/23.delta
2026-04-09 12:35:57       9827 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/24.delta
2026-04-09 12:36:17       3222 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/25.delta
2026-04-09 12:36:13      81278 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/25.snapshot
2026-04-09 12:36:34       3241 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/26.delta
2026-04-09 12:36:53       2476 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/27.delta
2026-04-09 12:37:09       2698 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/28.delta
2026-04-09 12:37:26       2442 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/29.delta
2026-04-09 12:29:15       3389 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/3.delta
2026-04-09 12:37:43       2251 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/30.delta
2026-04-09 12:37:59       2650 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/31.delta
2026-04-09 12:38:15       2398 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/32.delta
2026-04-09 12:38:30       2234 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/33.delta
2026-04-09 12:38:47       2243 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/34.delta
2026-04-09 12:39:04       2586 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/35.delta
2026-04-09 12:39:19       2456 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/36.delta
2026-04-09 12:39:14     114740 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/36.snapshot
2026-04-09 12:39:33       2400 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/37.delta
2026-04-09 12:39:50       2028 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/38.delta
2026-04-09 12:40:04       2357 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/39.delta
2026-04-09 12:29:33       2997 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/4.delta
2026-04-09 12:40:20       2592 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/40.delta
2026-04-09 12:40:36       1631 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/41.delta
2026-04-09 12:40:43         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/42.delta
2026-04-09 12:40:51         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/43.delta
2026-04-09 12:40:59         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/44.delta
2026-04-09 12:41:08         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/45.delta
2026-04-09 12:41:17         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/46.delta
2026-04-09 12:41:26         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/47.delta
2026-04-09 12:41:37         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/48.delta
2026-04-09 12:41:45         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/49.delta
2026-04-09 12:29:51       3026 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/5.delta
2026-04-09 12:30:10       2581 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/6.delta
2026-04-09 12:30:28       2932 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/7.delta
2026-04-09 12:30:46       2935 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/8.delta
2026-04-09 12:31:02       2436 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/9.delta
2026-04-09 12:28:22        203 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/0/_metadata/schema
2026-04-09 12:28:33         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/1.delta
2026-04-09 12:31:19       2503 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/10.delta
2026-04-09 12:31:34       2416 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/11.delta
2026-04-09 12:31:52       2389 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/12.delta
2026-04-09 12:32:10       2851 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/13.delta
2026-04-09 12:32:29       2485 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/14.delta
2026-04-09 12:32:22      39805 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/14.snapshot
2026-04-09 12:32:45       2960 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/15.delta
2026-04-09 12:33:00       2203 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/16.delta
2026-04-09 12:33:16       2221 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/17.delta
2026-04-09 12:33:32       2296 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/18.delta
2026-04-09 12:33:48       2168 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/19.delta
2026-04-09 12:28:56       2370 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/2.delta
2026-04-09 12:34:04       2301 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/20.delta
2026-04-09 12:34:20       2478 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/21.delta
2026-04-09 12:34:36       2482 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/22.delta
2026-04-09 12:35:34       2443 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/23.delta
2026-04-09 12:35:57       9302 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/24.delta
2026-04-09 12:36:18       3503 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/25.delta
2026-04-09 12:36:14      80685 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/25.snapshot
2026-04-09 12:36:34       3206 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/26.delta
2026-04-09 12:36:53       2249 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/27.delta
2026-04-09 12:37:09       3102 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/28.delta
2026-04-09 12:37:26       2546 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/29.delta
2026-04-09 12:29:15       3547 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/3.delta
2026-04-09 12:37:43       2616 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/30.delta
2026-04-09 12:38:00       2334 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/31.delta
2026-04-09 12:38:15       2302 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/32.delta
2026-04-09 12:38:30       2366 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/33.delta
2026-04-09 12:38:48       2374 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/34.delta
2026-04-09 12:39:04       2549 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/35.delta
2026-04-09 12:39:20       2610 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/36.delta
2026-04-09 12:39:14     114918 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/36.snapshot
2026-04-09 12:39:33       2135 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/37.delta
2026-04-09 12:39:50       1968 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/38.delta
2026-04-09 12:40:05       2443 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/39.delta
2026-04-09 12:29:33       2755 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/4.delta
2026-04-09 12:40:20       2495 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/40.delta
2026-04-09 12:40:36       1663 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/41.delta
2026-04-09 12:40:44         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/42.delta
2026-04-09 12:40:52         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/43.delta
2026-04-09 12:41:00         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/44.delta
2026-04-09 12:41:09         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/45.delta
2026-04-09 12:41:18         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/46.delta
2026-04-09 12:41:27         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/47.delta
2026-04-09 12:41:38         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/48.delta
2026-04-09 12:41:47         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/49.delta
2026-04-09 12:29:51       2657 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/5.delta
2026-04-09 12:30:10       2646 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/6.delta
2026-04-09 12:30:28       2988 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/7.delta
2026-04-09 12:30:46       2827 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/8.delta
2026-04-09 12:31:03       2572 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/1/9.delta
2026-04-09 12:34:47       2274 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/.23.delta.17d82fe6-13a1-4e8f-b960-c5761e796ead.TID4059.tmp
2026-04-09 12:28:33         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/1.delta
2026-04-09 12:31:19       2437 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/10.delta
2026-04-09 12:31:35       2321 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/11.delta
2026-04-09 12:31:53       2424 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/12.delta
2026-04-09 12:32:10       2630 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/13.delta
2026-04-09 12:32:30       2792 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/14.delta
2026-04-09 12:32:22      39571 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/14.snapshot
2026-04-09 12:32:46       2933 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/15.delta
2026-04-09 12:33:00       2332 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/16.delta
2026-04-09 12:33:17       2269 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/17.delta
2026-04-09 12:33:32       2592 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/18.delta
2026-04-09 12:33:48       2651 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/19.delta
2026-04-09 12:28:56       2278 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/2.delta
2026-04-09 12:34:05       2324 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/20.delta
2026-04-09 12:34:21       2466 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/21.delta
2026-04-09 12:34:37       2220 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/22.delta
2026-04-09 12:35:35       2274 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/23.delta
2026-04-09 12:35:58       9326 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/24.delta
2026-04-09 12:36:18       3482 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/25.delta
2026-04-09 12:36:14      80940 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/25.snapshot
2026-04-09 12:36:35       2962 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/26.delta
2026-04-09 12:36:53       2367 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/27.delta
2026-04-09 12:37:10       2937 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/28.delta
2026-04-09 12:37:27       2556 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/29.delta
2026-04-09 12:29:16       3717 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/3.delta
2026-04-09 12:37:44       2616 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/30.delta
2026-04-09 12:38:00       2282 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/31.delta
2026-04-09 12:38:16       2590 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/32.delta
2026-04-09 12:38:31       2362 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/33.delta
2026-04-09 12:38:48       2172 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/34.delta
2026-04-09 12:39:04       2337 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/35.delta
2026-04-09 12:39:20       2445 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/36.delta
2026-04-09 12:39:15     114264 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/36.snapshot
2026-04-09 12:39:33       2298 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/37.delta
2026-04-09 12:39:50       1983 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/38.delta
2026-04-09 12:40:05       2375 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/39.delta
2026-04-09 12:29:34       2839 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/4.delta
2026-04-09 12:40:21       2526 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/40.delta
2026-04-09 12:40:36       1589 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/41.delta
2026-04-09 12:40:44         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/42.delta
2026-04-09 12:40:52         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/43.delta
2026-04-09 12:41:00         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/44.delta
2026-04-09 12:41:09         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/45.delta
2026-04-09 12:41:18         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/46.delta
2026-04-09 12:41:27         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/47.delta
2026-04-09 12:41:38         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/48.delta
2026-04-09 12:41:48         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/49.delta
2026-04-09 12:29:51       2563 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/5.delta
2026-04-09 12:30:11       2543 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/6.delta
2026-04-09 12:30:29       2812 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/7.delta
2026-04-09 12:30:46       2607 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/8.delta
2026-04-09 12:31:03       2610 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/2/9.delta
2026-04-09 12:34:47       2347 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/.23.delta.28204e26-c603-4249-abe1-ce4d55993c26.TID4060.tmp
2026-04-09 12:28:33         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/1.delta
2026-04-09 12:31:20       2393 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/10.delta
2026-04-09 12:31:35       2488 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/11.delta
2026-04-09 12:31:53       2454 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/12.delta
2026-04-09 12:32:11       2443 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/13.delta
2026-04-09 12:32:31       2922 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/14.delta
2026-04-09 12:32:24      41059 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/14.snapshot
2026-04-09 12:32:46       2634 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/15.delta
2026-04-09 12:33:01       2190 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/16.delta
2026-04-09 12:33:17       2129 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/17.delta
2026-04-09 12:33:33       2387 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/18.delta
2026-04-09 12:33:49       2248 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/19.delta
2026-04-09 12:28:56       2376 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/2.delta
2026-04-09 12:34:05       2502 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/20.delta
2026-04-09 12:34:21       2289 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/21.delta
2026-04-09 12:34:37       2399 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/22.delta
2026-04-09 12:35:35       2347 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/23.delta
2026-04-09 12:35:58       9371 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/24.delta
2026-04-09 12:36:19       3540 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/25.delta
2026-04-09 12:36:13      81557 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/25.snapshot
2026-04-09 12:36:35       2961 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/26.delta
2026-04-09 12:36:53       2259 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/27.delta
2026-04-09 12:37:10       3013 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/28.delta
2026-04-09 12:37:27       2384 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/29.delta
2026-04-09 12:29:16       3821 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/3.delta
2026-04-09 12:37:44       2504 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/30.delta
2026-04-09 12:38:00       2659 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/31.delta
2026-04-09 12:38:16       2489 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/32.delta
2026-04-09 12:38:31       2286 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/33.delta
2026-04-09 12:38:48       2333 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/34.delta
2026-04-09 12:39:05       2595 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/35.delta
2026-04-09 12:39:21       2328 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/36.delta
2026-04-09 12:39:13     115125 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/36.snapshot
2026-04-09 12:39:33       2317 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/37.delta
2026-04-09 12:39:50       2057 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/38.delta
2026-04-09 12:40:05       2509 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/39.delta
2026-04-09 12:29:34       2970 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/4.delta
2026-04-09 12:40:21       2300 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/40.delta
2026-04-09 12:40:36       1820 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/41.delta
2026-04-09 12:40:45         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/42.delta
2026-04-09 12:40:53         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/43.delta
2026-04-09 12:41:01         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/44.delta
2026-04-09 12:41:10         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/45.delta
2026-04-09 12:41:19         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/46.delta
2026-04-09 12:41:28         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/47.delta
2026-04-09 12:41:39         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/48.delta
2026-04-09 12:41:48         46 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/49.delta
2026-04-09 12:29:52       2661 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/5.delta
2026-04-09 12:30:11       2884 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/6.delta
2026-04-09 12:30:29       3255 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/7.delta
2026-04-09 12:30:47       2780 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/8.delta
2026-04-09 12:31:03       2798 evaluations/baseline-failure-recovery-20260409t122500z/checkpoints/processed_side_effects/state/0/3/9.delta
2026-04-09 12:41:49          0 evaluations/baseline-failure-recovery-20260409t122500z/invalid/_SUCCESS
2026-04-09 12:41:35      98875 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00000-05248af0-8d31-40c9-aace-9f559e02f5d0-c000.json
2026-04-09 12:41:24      50441 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00000-9d746e84-e3a7-430d-814e-eddec9af68f1-c000.json
2026-04-09 12:41:47      10087 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00000-e9a24af6-9d2b-4836-b5f6-db9b674aa86a-c000.json
2026-04-09 12:41:35     114989 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00001-05248af0-8d31-40c9-aace-9f559e02f5d0-c000.json
2026-04-09 12:41:25      48424 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00001-9d746e84-e3a7-430d-814e-eddec9af68f1-c000.json
2026-04-09 12:41:47      10085 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00001-e9a24af6-9d2b-4836-b5f6-db9b674aa86a-c000.json
2026-04-09 12:41:36     102222 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00002-05248af0-8d31-40c9-aace-9f559e02f5d0-c000.json
2026-04-09 12:41:25      54493 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00002-9d746e84-e3a7-430d-814e-eddec9af68f1-c000.json
2026-04-09 12:41:48       7403 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00002-e9a24af6-9d2b-4836-b5f6-db9b674aa86a-c000.json
2026-04-09 12:41:36      90811 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00003-05248af0-8d31-40c9-aace-9f559e02f5d0-c000.json
2026-04-09 12:41:26      50443 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00003-9d746e84-e3a7-430d-814e-eddec9af68f1-c000.json
2026-04-09 12:41:48      11433 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00003-e9a24af6-9d2b-4836-b5f6-db9b674aa86a-c000.json
2026-04-09 12:41:36      93479 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00004-05248af0-8d31-40c9-aace-9f559e02f5d0-c000.json
2026-04-09 12:41:26      61179 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00004-9d746e84-e3a7-430d-814e-eddec9af68f1-c000.json
2026-04-09 12:41:48      10095 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00004-e9a24af6-9d2b-4836-b5f6-db9b674aa86a-c000.json
2026-04-09 12:41:37      85393 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00005-05248af0-8d31-40c9-aace-9f559e02f5d0-c000.json
2026-04-09 12:41:26      53115 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00005-9d746e84-e3a7-430d-814e-eddec9af68f1-c000.json
2026-04-09 12:41:49      15467 evaluations/baseline-failure-recovery-20260409t122500z/invalid/part-00005-e9a24af6-9d2b-4836-b5f6-db9b674aa86a-c000.json
2026-04-09 12:40:28          0 evaluations/baseline-failure-recovery-20260409t122500z/processed/_SUCCESS
2026-04-09 12:39:39      28640 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-0742c7dc-2249-471a-b3c8-5be883786050-c000.snappy.parquet
2026-04-09 12:29:58      34072 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-09123f7e-f9d2-42f6-8495-6c4928d74dc4-c000.snappy.parquet
2026-04-09 12:31:59      36166 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-0c660af2-8f8f-410b-b550-97079c9d28b0-c000.snappy.parquet
2026-04-09 12:38:37      30614 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-112c07b2-8fcb-4644-b30f-34fb9a51ee5b-c000.snappy.parquet
2026-04-09 12:38:21      30989 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-182a3046-dcd4-4ae5-aac0-a89026f61b2c-c000.snappy.parquet
2026-04-09 12:29:04      41193 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-19f3cabc-cbe7-4f1c-86af-0680edbc4740-c000.snappy.parquet
2026-04-09 12:29:41      37203 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-1d093afb-1ad5-4432-9261-a9e6b99dcca3-c000.snappy.parquet
2026-04-09 12:33:54      31499 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-2fdd62e3-671b-4746-8057-2e83e114982e-c000.snappy.parquet
2026-04-09 12:34:42      31933 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-31314620-62f3-4580-b16f-32ee39a038df-c000.snappy.parquet
2026-04-09 12:36:59      34486 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-3262c937-ce18-4e2d-be43-df445d289da9-c000.snappy.parquet
2026-04-09 12:38:54      34066 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-38bb54d4-22a1-4471-9207-9d94e8fdeea0-c000.snappy.parquet
2026-04-09 12:34:26      31661 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-3b636e9b-c46d-4b49-a1b3-a4f06b702e6c-c000.snappy.parquet
2026-04-09 12:31:25      32318 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-3e44e6d0-3844-4310-8d90-83d0f7ed9d21-c000.snappy.parquet
2026-04-09 12:30:17      35544 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-3f049dd3-a43c-4877-9c98-4994533538dd-c000.snappy.parquet
2026-04-09 12:33:22      30871 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-41217488-0828-4058-a6dc-af11d2126d24-c000.snappy.parquet
2026-04-09 12:30:35      36452 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-466977f0-b7e5-4c2e-9100-370296c016a5-c000.snappy.parquet
2026-04-09 12:31:09      33195 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-4f945e30-ab3b-48cd-81f0-7255526c5b59-c000.snappy.parquet
2026-04-09 12:39:10      33173 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-515abdc1-9e93-4763-82ff-4d22a9b88370-c000.snappy.parquet
2026-04-09 12:38:06      32336 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-5283f78d-2b14-4f5f-a083-b58d058a81cb-c000.snappy.parquet
2026-04-09 12:31:41      31472 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-5b326f7a-4c61-488b-a34a-6b4fd9af8ee8-c000.snappy.parquet
2026-04-09 12:37:16      32688 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-75bcc75f-599e-43f8-bd2d-bc918f71fdf3-c000.snappy.parquet
2026-04-09 12:30:53      32648 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-7d9f6452-79a2-47f9-99f2-ac9559021432-c000.snappy.parquet
2026-04-09 12:35:21      31933 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-8218878e-a368-4a01-89fd-a915c6dbd14e-c000.snappy.parquet
2026-04-09 12:32:36      36451 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-8d60cde7-8461-49d7-808e-1fa575d7daa8-c000.snappy.parquet
2026-04-09 12:37:50      34821 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-8f3c4b10-a86a-441a-95f1-07e1bc25613a-c000.snappy.parquet
2026-04-09 12:32:17      34978 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-955607d7-6175-4ea2-82e7-71e43e4a4d25-c000.snappy.parquet
2026-04-09 12:36:25      39785 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-95e729e5-e8ba-4a66-a239-5f624862d25c-c000.snappy.parquet
2026-04-09 12:37:33      31172 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-96090da6-fddc-44af-b967-60cf8ff84b68-c000.snappy.parquet
2026-04-09 12:39:55      31860 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-97f51667-f82b-476c-9a3b-366c94272905-c000.snappy.parquet
2026-04-09 12:33:07      30791 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-a4f5e67f-6cb4-494a-b10d-bdcd62e0c601-c000.snappy.parquet
2026-04-09 12:39:26      32574 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-ab23ef45-3a1b-4f81-9c97-61c6dfb9758a-c000.snappy.parquet
2026-04-09 12:28:43      31190 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-b51c49cc-32fc-4a4f-bad9-8e9685114404-c000.snappy.parquet
2026-04-09 12:32:51      33637 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-b717792a-d497-4f65-a944-2fbae246aeec-c000.snappy.parquet
2026-04-09 12:33:38      31235 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-c5eeb000-369b-4b09-ae2f-cfb74f79af1a-c000.snappy.parquet
2026-04-09 12:34:10      33469 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-c7d59170-6605-4b33-8303-d448c9396b21-c000.snappy.parquet
2026-04-09 12:36:06      39533 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-c83ce222-6add-4c2b-a71b-7c5cc7438fe1-c000.snappy.parquet
2026-04-09 12:40:11      33359 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-d38b81ae-db9c-4038-9bf3-b2546487088c-c000.snappy.parquet
2026-04-09 12:29:23      37472 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-d9206a82-1fca-4d48-9134-f90811a9f5c6-c000.snappy.parquet
2026-04-09 12:35:45      96373 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-d94479ca-ea25-485f-a5bd-f2c13b6c2026-c000.snappy.parquet
2026-04-09 12:40:27      25097 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-ed5d36cf-7298-4f06-83c4-2894d1cc6baa-c000.snappy.parquet
2026-04-09 12:36:42      33078 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00000-f4dbcf41-23ff-40e7-8ee9-e774d9410a57-c000.snappy.parquet
2026-04-09 12:39:40      27848 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-0742c7dc-2249-471a-b3c8-5be883786050-c000.snappy.parquet
2026-04-09 12:29:59      34490 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-09123f7e-f9d2-42f6-8495-6c4928d74dc4-c000.snappy.parquet
2026-04-09 12:31:59      35739 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-0c660af2-8f8f-410b-b550-97079c9d28b0-c000.snappy.parquet
2026-04-09 12:38:37      31271 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-112c07b2-8fcb-4644-b30f-34fb9a51ee5b-c000.snappy.parquet
2026-04-09 12:38:22      32112 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-182a3046-dcd4-4ae5-aac0-a89026f61b2c-c000.snappy.parquet
2026-04-09 12:29:05      42646 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-19f3cabc-cbe7-4f1c-86af-0680edbc4740-c000.snappy.parquet
2026-04-09 12:29:41      34837 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-1d093afb-1ad5-4432-9261-a9e6b99dcca3-c000.snappy.parquet
2026-04-09 12:33:55      31378 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-2fdd62e3-671b-4746-8057-2e83e114982e-c000.snappy.parquet
2026-04-09 12:34:43      32931 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-31314620-62f3-4580-b16f-32ee39a038df-c000.snappy.parquet
2026-04-09 12:37:00      37191 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-3262c937-ce18-4e2d-be43-df445d289da9-c000.snappy.parquet
2026-04-09 12:38:55      34175 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-38bb54d4-22a1-4471-9207-9d94e8fdeea0-c000.snappy.parquet
2026-04-09 12:34:26      33076 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-3b636e9b-c46d-4b49-a1b3-a4f06b702e6c-c000.snappy.parquet
2026-04-09 12:31:25      32213 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-3e44e6d0-3844-4310-8d90-83d0f7ed9d21-c000.snappy.parquet
2026-04-09 12:30:18      35888 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-3f049dd3-a43c-4877-9c98-4994533538dd-c000.snappy.parquet
2026-04-09 12:33:22      31654 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-41217488-0828-4058-a6dc-af11d2126d24-c000.snappy.parquet
2026-04-09 12:30:36      35651 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-466977f0-b7e5-4c2e-9100-370296c016a5-c000.snappy.parquet
2026-04-09 12:31:10      33863 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-4f945e30-ab3b-48cd-81f0-7255526c5b59-c000.snappy.parquet
2026-04-09 12:39:10      34529 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-515abdc1-9e93-4763-82ff-4d22a9b88370-c000.snappy.parquet
2026-04-09 12:38:07      31078 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-5283f78d-2b14-4f5f-a083-b58d058a81cb-c000.snappy.parquet
2026-04-09 12:31:42      32441 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-5b326f7a-4c61-488b-a34a-6b4fd9af8ee8-c000.snappy.parquet
2026-04-09 12:37:17      33541 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-75bcc75f-599e-43f8-bd2d-bc918f71fdf3-c000.snappy.parquet
2026-04-09 12:30:53      33959 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-7d9f6452-79a2-47f9-99f2-ac9559021432-c000.snappy.parquet
2026-04-09 12:35:21      32931 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-8218878e-a368-4a01-89fd-a915c6dbd14e-c000.snappy.parquet
2026-04-09 12:32:36      36725 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-8d60cde7-8461-49d7-808e-1fa575d7daa8-c000.snappy.parquet
2026-04-09 12:37:50      31998 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-8f3c4b10-a86a-441a-95f1-07e1bc25613a-c000.snappy.parquet
2026-04-09 12:32:18      33400 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-955607d7-6175-4ea2-82e7-71e43e4a4d25-c000.snappy.parquet
2026-04-09 12:36:25      39230 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-95e729e5-e8ba-4a66-a239-5f624862d25c-c000.snappy.parquet
2026-04-09 12:37:34      34500 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-96090da6-fddc-44af-b967-60cf8ff84b68-c000.snappy.parquet
2026-04-09 12:39:56      32745 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-97f51667-f82b-476c-9a3b-366c94272905-c000.snappy.parquet
2026-04-09 12:33:07      30567 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-a4f5e67f-6cb4-494a-b10d-bdcd62e0c601-c000.snappy.parquet
2026-04-09 12:39:26      30016 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-ab23ef45-3a1b-4f81-9c97-61c6dfb9758a-c000.snappy.parquet
2026-04-09 12:28:44      31967 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-b51c49cc-32fc-4a4f-bad9-8e9685114404-c000.snappy.parquet
2026-04-09 12:32:52      30694 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-b717792a-d497-4f65-a944-2fbae246aeec-c000.snappy.parquet
2026-04-09 12:33:39      30562 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-c5eeb000-369b-4b09-ae2f-cfb74f79af1a-c000.snappy.parquet
2026-04-09 12:34:10      33427 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-c7d59170-6605-4b33-8303-d448c9396b21-c000.snappy.parquet
2026-04-09 12:36:07      41994 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-c83ce222-6add-4c2b-a71b-7c5cc7438fe1-c000.snappy.parquet
2026-04-09 12:40:11      32353 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-d38b81ae-db9c-4038-9bf3-b2546487088c-c000.snappy.parquet
2026-04-09 12:29:24      35596 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-d9206a82-1fca-4d48-9134-f90811a9f5c6-c000.snappy.parquet
2026-04-09 12:35:45      92500 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-d94479ca-ea25-485f-a5bd-f2c13b6c2026-c000.snappy.parquet
2026-04-09 12:40:27      25347 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-ed5d36cf-7298-4f06-83c4-2894d1cc6baa-c000.snappy.parquet
2026-04-09 12:36:43      31190 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00001-f4dbcf41-23ff-40e7-8ee9-e774d9410a57-c000.snappy.parquet
2026-04-09 12:39:40      27928 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-0742c7dc-2249-471a-b3c8-5be883786050-c000.snappy.parquet
2026-04-09 12:29:59      33983 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-09123f7e-f9d2-42f6-8495-6c4928d74dc4-c000.snappy.parquet
2026-04-09 12:32:00      34129 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-0c660af2-8f8f-410b-b550-97079c9d28b0-c000.snappy.parquet
2026-04-09 12:38:38      30382 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-112c07b2-8fcb-4644-b30f-34fb9a51ee5b-c000.snappy.parquet
2026-04-09 12:38:22      32358 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-182a3046-dcd4-4ae5-aac0-a89026f61b2c-c000.snappy.parquet
2026-04-09 12:29:05      43946 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-19f3cabc-cbe7-4f1c-86af-0680edbc4740-c000.snappy.parquet
2026-04-09 12:29:42      34225 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-1d093afb-1ad5-4432-9261-a9e6b99dcca3-c000.snappy.parquet
2026-04-09 12:33:55      31755 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-2fdd62e3-671b-4746-8057-2e83e114982e-c000.snappy.parquet
2026-04-09 12:34:43      31299 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-31314620-62f3-4580-b16f-32ee39a038df-c000.snappy.parquet
2026-04-09 12:37:00      36072 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-3262c937-ce18-4e2d-be43-df445d289da9-c000.snappy.parquet
2026-04-09 12:38:55      32064 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-38bb54d4-22a1-4471-9207-9d94e8fdeea0-c000.snappy.parquet
2026-04-09 12:34:27      30538 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-3b636e9b-c46d-4b49-a1b3-a4f06b702e6c-c000.snappy.parquet
2026-04-09 12:31:26      31451 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-3e44e6d0-3844-4310-8d90-83d0f7ed9d21-c000.snappy.parquet
2026-04-09 12:30:18      34921 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-3f049dd3-a43c-4877-9c98-4994533538dd-c000.snappy.parquet
2026-04-09 12:33:23      34680 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-41217488-0828-4058-a6dc-af11d2126d24-c000.snappy.parquet
2026-04-09 12:30:36      34441 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-466977f0-b7e5-4c2e-9100-370296c016a5-c000.snappy.parquet
2026-04-09 12:31:10      33135 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-4f945e30-ab3b-48cd-81f0-7255526c5b59-c000.snappy.parquet
2026-04-09 12:39:11      33137 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-515abdc1-9e93-4763-82ff-4d22a9b88370-c000.snappy.parquet
2026-04-09 12:38:07      33893 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-5283f78d-2b14-4f5f-a083-b58d058a81cb-c000.snappy.parquet
2026-04-09 12:31:42      32600 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-5b326f7a-4c61-488b-a34a-6b4fd9af8ee8-c000.snappy.parquet
2026-04-09 12:37:17      34123 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-75bcc75f-599e-43f8-bd2d-bc918f71fdf3-c000.snappy.parquet
2026-04-09 12:30:53      34525 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-7d9f6452-79a2-47f9-99f2-ac9559021432-c000.snappy.parquet
2026-04-09 12:35:22      31299 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-8218878e-a368-4a01-89fd-a915c6dbd14e-c000.snappy.parquet
2026-04-09 12:32:37      36494 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-8d60cde7-8461-49d7-808e-1fa575d7daa8-c000.snappy.parquet
2026-04-09 12:37:51      31280 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-8f3c4b10-a86a-441a-95f1-07e1bc25613a-c000.snappy.parquet
2026-04-09 12:32:18      35498 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-955607d7-6175-4ea2-82e7-71e43e4a4d25-c000.snappy.parquet
2026-04-09 12:36:26      37032 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-95e729e5-e8ba-4a66-a239-5f624862d25c-c000.snappy.parquet
2026-04-09 12:37:34      34732 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-96090da6-fddc-44af-b967-60cf8ff84b68-c000.snappy.parquet
2026-04-09 12:39:56      31822 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-97f51667-f82b-476c-9a3b-366c94272905-c000.snappy.parquet
2026-04-09 12:33:08      30791 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-a4f5e67f-6cb4-494a-b10d-bdcd62e0c601-c000.snappy.parquet
2026-04-09 12:39:26      31509 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-ab23ef45-3a1b-4f81-9c97-61c6dfb9758a-c000.snappy.parquet
2026-04-09 12:28:44      31352 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-b51c49cc-32fc-4a4f-bad9-8e9685114404-c000.snappy.parquet
2026-04-09 12:32:52      31767 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-b717792a-d497-4f65-a944-2fbae246aeec-c000.snappy.parquet
2026-04-09 12:33:39      34803 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-c5eeb000-369b-4b09-ae2f-cfb74f79af1a-c000.snappy.parquet
2026-04-09 12:34:11      33260 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-c7d59170-6605-4b33-8303-d448c9396b21-c000.snappy.parquet
2026-04-09 12:36:07      42125 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-c83ce222-6add-4c2b-a71b-7c5cc7438fe1-c000.snappy.parquet
2026-04-09 12:40:12      32272 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-d38b81ae-db9c-4038-9bf3-b2546487088c-c000.snappy.parquet
2026-04-09 12:29:24      36021 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-d9206a82-1fca-4d48-9134-f90811a9f5c6-c000.snappy.parquet
2026-04-09 12:35:46      92601 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-d94479ca-ea25-485f-a5bd-f2c13b6c2026-c000.snappy.parquet
2026-04-09 12:40:27      24632 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-ed5d36cf-7298-4f06-83c4-2894d1cc6baa-c000.snappy.parquet
2026-04-09 12:36:43      31940 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00002-f4dbcf41-23ff-40e7-8ee9-e774d9410a57-c000.snappy.parquet
2026-04-09 12:39:41      28882 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-0742c7dc-2249-471a-b3c8-5be883786050-c000.snappy.parquet
2026-04-09 12:29:59      35939 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-09123f7e-f9d2-42f6-8495-6c4928d74dc4-c000.snappy.parquet
2026-04-09 12:32:00      32400 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-0c660af2-8f8f-410b-b550-97079c9d28b0-c000.snappy.parquet
2026-04-09 12:38:38      31615 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-112c07b2-8fcb-4644-b30f-34fb9a51ee5b-c000.snappy.parquet
2026-04-09 12:38:22      31432 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-182a3046-dcd4-4ae5-aac0-a89026f61b2c-c000.snappy.parquet
2026-04-09 12:29:06      44672 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-19f3cabc-cbe7-4f1c-86af-0680edbc4740-c000.snappy.parquet
2026-04-09 12:29:42      34855 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-1d093afb-1ad5-4432-9261-a9e6b99dcca3-c000.snappy.parquet
2026-04-09 12:33:55      33039 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-2fdd62e3-671b-4746-8057-2e83e114982e-c000.snappy.parquet
2026-04-09 12:34:43      31791 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-31314620-62f3-4580-b16f-32ee39a038df-c000.snappy.parquet
2026-04-09 12:37:00      36567 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-3262c937-ce18-4e2d-be43-df445d289da9-c000.snappy.parquet
2026-04-09 12:38:56      34411 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-38bb54d4-22a1-4471-9207-9d94e8fdeea0-c000.snappy.parquet
2026-04-09 12:34:27      32263 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-3b636e9b-c46d-4b49-a1b3-a4f06b702e6c-c000.snappy.parquet
2026-04-09 12:31:26      32837 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-3e44e6d0-3844-4310-8d90-83d0f7ed9d21-c000.snappy.parquet
2026-04-09 12:30:19      38319 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-3f049dd3-a43c-4877-9c98-4994533538dd-c000.snappy.parquet
2026-04-09 12:33:23      32518 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-41217488-0828-4058-a6dc-af11d2126d24-c000.snappy.parquet
2026-04-09 12:30:37      35389 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-466977f0-b7e5-4c2e-9100-370296c016a5-c000.snappy.parquet
2026-04-09 12:31:10      32646 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-4f945e30-ab3b-48cd-81f0-7255526c5b59-c000.snappy.parquet
2026-04-09 12:39:11      31824 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-515abdc1-9e93-4763-82ff-4d22a9b88370-c000.snappy.parquet
2026-04-09 12:38:07      33037 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-5283f78d-2b14-4f5f-a083-b58d058a81cb-c000.snappy.parquet
2026-04-09 12:31:43      32836 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-5b326f7a-4c61-488b-a34a-6b4fd9af8ee8-c000.snappy.parquet
2026-04-09 12:37:18      32149 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-75bcc75f-599e-43f8-bd2d-bc918f71fdf3-c000.snappy.parquet
2026-04-09 12:30:54      35188 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-7d9f6452-79a2-47f9-99f2-ac9559021432-c000.snappy.parquet
2026-04-09 12:35:22      31791 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-8218878e-a368-4a01-89fd-a915c6dbd14e-c000.snappy.parquet
2026-04-09 12:32:37      34893 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-8d60cde7-8461-49d7-808e-1fa575d7daa8-c000.snappy.parquet
2026-04-09 12:37:51      34812 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-8f3c4b10-a86a-441a-95f1-07e1bc25613a-c000.snappy.parquet
2026-04-09 12:32:19      36191 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-955607d7-6175-4ea2-82e7-71e43e4a4d25-c000.snappy.parquet
2026-04-09 12:36:26      36938 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-95e729e5-e8ba-4a66-a239-5f624862d25c-c000.snappy.parquet
2026-04-09 12:37:34      33886 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-96090da6-fddc-44af-b967-60cf8ff84b68-c000.snappy.parquet
2026-04-09 12:39:56      33377 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-97f51667-f82b-476c-9a3b-366c94272905-c000.snappy.parquet
2026-04-09 12:33:08      29874 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-a4f5e67f-6cb4-494a-b10d-bdcd62e0c601-c000.snappy.parquet
2026-04-09 12:39:27      31618 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-ab23ef45-3a1b-4f81-9c97-61c6dfb9758a-c000.snappy.parquet
2026-04-09 12:28:45      31946 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-b51c49cc-32fc-4a4f-bad9-8e9685114404-c000.snappy.parquet
2026-04-09 12:32:52      30256 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-b717792a-d497-4f65-a944-2fbae246aeec-c000.snappy.parquet
2026-04-09 12:33:39      31028 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-c5eeb000-369b-4b09-ae2f-cfb74f79af1a-c000.snappy.parquet
2026-04-09 12:34:11      31644 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-c7d59170-6605-4b33-8303-d448c9396b21-c000.snappy.parquet
2026-04-09 12:36:07      42665 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-c83ce222-6add-4c2b-a71b-7c5cc7438fe1-c000.snappy.parquet
2026-04-09 12:40:12      30446 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-d38b81ae-db9c-4038-9bf3-b2546487088c-c000.snappy.parquet
2026-04-09 12:29:25      37264 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-d9206a82-1fca-4d48-9134-f90811a9f5c6-c000.snappy.parquet
2026-04-09 12:35:46      93141 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-d94479ca-ea25-485f-a5bd-f2c13b6c2026-c000.snappy.parquet
2026-04-09 12:40:28      26976 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-ed5d36cf-7298-4f06-83c4-2894d1cc6baa-c000.snappy.parquet
2026-04-09 12:36:43      30944 evaluations/baseline-failure-recovery-20260409t122500z/processed/part-00003-f4dbcf41-23ff-40e7-8ee9-e774d9410a57-c000.snappy.parquet
2026-04-09 12:27:23   16046100 evaluations/baseline-failure-recovery-20260409t122500z/workloads/failure-recovery.events.ndjson
```

## Cluster Snapshot

```text
NAME                                   READY   STATUS      RESTARTS   AGE
pod/kafka-0                            1/1     Running     0          50m
pod/replay-producer-tv5xp              0/1     Completed   0          13m
pod/stream-processor-bb5655984-8l97m   1/1     Running     0          7m20s

NAME            TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
service/kafka   ClusterIP   None         <none>        9092/TCP   32h

NAME                               READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/stream-processor   1/1     1            1           32h

NAME                                          DESIRED   CURRENT   READY   AGE
replicaset.apps/stream-processor-579c8d5596   0         0         0       8h
replicaset.apps/stream-processor-5868d9d9d9   0         0         0       14m
replicaset.apps/stream-processor-59b8959fdc   0         0         0       52m
replicaset.apps/stream-processor-5bcc79b58c   0         0         0       24m
replicaset.apps/stream-processor-5fbc9bb6d9   0         0         0       42m
replicaset.apps/stream-processor-65948df994   0         0         0       10h
replicaset.apps/stream-processor-65964b7b84   0         0         0       8h
replicaset.apps/stream-processor-65fd575586   0         0         0       8h
replicaset.apps/stream-processor-7998b5dbcd   0         0         0       8h
replicaset.apps/stream-processor-866bd8567d   0         0         0       48m
replicaset.apps/stream-processor-bb5655984    1         1         1       7m21s

NAME                     READY   AGE
statefulset.apps/kafka   1/1     32h

NAME                                            STATUS      COMPLETIONS   DURATION   AGE
job.batch/evaluation-controller                 Suspended   0/1                      8h
job.batch/evaluation-controller-smoke-run-001   Failed      0/1           8h         8h
job.batch/evaluation-controller-smoke-run-002   Complete    1/1           9s         8h
job.batch/replay-producer                       Complete    1/1           13m        13m
```

## Important Findings

- Replay completed successfully at the configured `100.0 eps` rate for the full `79,200`-event failure-recovery workload.
- The planned fault was actually injected and completed: `completed:stream-processor-restart:delay=360s`.
- The pipeline still completed the scenario and produced `39,741` processed events, `1,440` invalid events, and `563` breach events after the forced restart.
- Recovery was functionally successful, but performance degraded sharply: average end-to-end latency was `11.82 s` and p95 latency reached `53.71 s`.
- Live stream logs continued to show repeated `falling behind` warnings during and after recovery, so this scenario demonstrates resilience more clearly than low-latency recovery.
- Output evidence was present across all major sinks: `165` processed objects, `19` invalid objects, and `154` breach-window objects were written under the run prefix.
