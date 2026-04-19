# AWS Baseline Evaluation baseline-normal-20260409t120000z

- Run ID: `baseline-normal-20260409t120000z`
- AWS account: `347038623570`
- Region: `ap-south-1`
- Cluster: `vacciguard-baseline-eks`
- Namespace: `vacciguard`
- Scenario: `normal`
- Workload family version: `evaluation-workload-v1`
- Workload file: `/Users/tanishgupta/Downloads/vacciguard/.worktrees/tanish_pipeline/data/workloads/evaluation/v1/normal.events.ndjson`
- Workload source: `s3`
- Workload size bytes: `15254191`
- Declared input events: `79200`
- Configured replay rate: `100.0`
- Fault model: `none`
- Kafka topic: `vacciguard-eval-baseline-normal-20260409t120000z`
- S3 prefix: `s3://vacciguard-baseline-data/evaluations/baseline-normal-20260409t120000z/`
- Redis reset: `true`
- Report written at: `2026-04-09T06:46:37Z`

## Redis Reset

```text
cleared_device_status_keys=0
cleared_active_breaches=1
```

## Fault Injection

```text
not-configured
```

## Evaluation Summary

| Metric | Baseline |
|---|---:|
| Workload family version | evaluation-workload-v1 |
| Scenario | normal |
| Configured replay rate | 100.00 events/s |
| Avg end-to-end latency | 7.60 s |
| P95 latency | 13.79 s |
| Throughput | 100.00 events/s |
| 10x spike success/failure | Not run |
| Recovery time after failure | Not run |
| Input events | 79200 |
| Processed events | 32610 |
| Invalid events | 1440 |
| Deduplicated events | 1 |
| Breach events | 457 |
| Cost per run | Not run |
| Cost per GB processed | Not run |

## Evaluation Metrics JSON

```json
{
  "avg_end_to_end_latency_seconds": 7.6,
  "breach_events": 457,
  "configured_events_per_second": 100.0,
  "cost_per_gb_processed": "Not run",
  "cost_per_run": "Not run",
  "deduplicated_events": 1,
  "input_events": 79200,
  "invalid_events": 1440,
  "p95_end_to_end_latency_seconds": 13.79,
  "processed_events": 32610,
  "recovery_time_after_failure": "Not run",
  "scenario": "normal",
  "spike_result": "Not run",
  "throughput_eps": 100.0,
  "workload_family_version": "evaluation-workload-v1"
}
```

## Replay Logs

```text
2026-04-09T06:41:45Z INFO Sent 69450/79200  actual 100.0 eps
2026-04-09T06:41:46Z INFO Sent 69500/79200  actual 100.0 eps
2026-04-09T06:41:46Z INFO Sent 69550/79200  actual 100.0 eps
2026-04-09T06:41:47Z INFO Sent 69600/79200  actual 100.0 eps
2026-04-09T06:41:47Z INFO Sent 69650/79200  actual 100.0 eps
2026-04-09T06:41:48Z INFO Sent 69700/79200  actual 100.0 eps
2026-04-09T06:41:48Z INFO Sent 69750/79200  actual 100.0 eps
2026-04-09T06:41:49Z INFO Sent 69800/79200  actual 100.0 eps
2026-04-09T06:41:49Z INFO Sent 69850/79200  actual 100.0 eps
2026-04-09T06:41:50Z INFO Sent 69900/79200  actual 100.0 eps
2026-04-09T06:41:50Z INFO Sent 69950/79200  actual 100.0 eps
2026-04-09T06:41:51Z INFO Sent 70000/79200  actual 100.0 eps
2026-04-09T06:41:51Z INFO Sent 70050/79200  actual 100.0 eps
2026-04-09T06:41:52Z INFO Sent 70100/79200  actual 100.0 eps
2026-04-09T06:41:52Z INFO Sent 70150/79200  actual 100.0 eps
2026-04-09T06:41:53Z INFO Sent 70200/79200  actual 100.0 eps
2026-04-09T06:41:53Z INFO Sent 70250/79200  actual 100.0 eps
2026-04-09T06:41:54Z INFO Sent 70300/79200  actual 100.0 eps
2026-04-09T06:41:54Z INFO Sent 70350/79200  actual 100.0 eps
2026-04-09T06:41:55Z INFO Sent 70400/79200  actual 100.0 eps
2026-04-09T06:41:55Z INFO Sent 70450/79200  actual 100.0 eps
2026-04-09T06:41:56Z INFO Sent 70500/79200  actual 100.0 eps
2026-04-09T06:41:56Z INFO Sent 70550/79200  actual 100.0 eps
2026-04-09T06:41:57Z INFO Sent 70600/79200  actual 100.0 eps
2026-04-09T06:41:57Z INFO Sent 70650/79200  actual 100.0 eps
2026-04-09T06:41:58Z INFO Sent 70700/79200  actual 100.0 eps
2026-04-09T06:41:58Z INFO Sent 70750/79200  actual 100.0 eps
2026-04-09T06:41:59Z INFO Sent 70800/79200  actual 100.0 eps
2026-04-09T06:41:59Z INFO Sent 70850/79200  actual 100.0 eps
2026-04-09T06:42:00Z INFO Sent 70900/79200  actual 100.0 eps
2026-04-09T06:42:00Z INFO Sent 70950/79200  actual 100.0 eps
2026-04-09T06:42:01Z INFO Sent 71000/79200  actual 100.0 eps
2026-04-09T06:42:01Z INFO Sent 71050/79200  actual 100.0 eps
2026-04-09T06:42:02Z INFO Sent 71100/79200  actual 100.0 eps
2026-04-09T06:42:03Z INFO Sent 71150/79200  actual 100.0 eps
2026-04-09T06:42:03Z INFO Sent 71200/79200  actual 100.0 eps
2026-04-09T06:42:04Z INFO Sent 71250/79200  actual 100.0 eps
2026-04-09T06:42:04Z INFO Sent 71300/79200  actual 100.0 eps
2026-04-09T06:42:04Z INFO Sent 71350/79200  actual 100.0 eps
2026-04-09T06:42:05Z INFO Sent 71400/79200  actual 100.0 eps
2026-04-09T06:42:05Z INFO Sent 71450/79200  actual 100.0 eps
2026-04-09T06:42:06Z INFO Sent 71500/79200  actual 100.0 eps
2026-04-09T06:42:06Z INFO Sent 71550/79200  actual 100.0 eps
2026-04-09T06:42:07Z INFO Sent 71600/79200  actual 100.0 eps
2026-04-09T06:42:07Z INFO Sent 71650/79200  actual 100.0 eps
2026-04-09T06:42:08Z INFO Sent 71700/79200  actual 100.0 eps
2026-04-09T06:42:08Z INFO Sent 71750/79200  actual 100.0 eps
2026-04-09T06:42:09Z INFO Sent 71800/79200  actual 100.0 eps
2026-04-09T06:42:09Z INFO Sent 71850/79200  actual 100.0 eps
2026-04-09T06:42:10Z INFO Sent 71900/79200  actual 100.0 eps
2026-04-09T06:42:10Z INFO Sent 71950/79200  actual 100.0 eps
2026-04-09T06:42:11Z INFO Sent 72000/79200  actual 100.0 eps
2026-04-09T06:42:11Z INFO Sent 72050/79200  actual 100.0 eps
2026-04-09T06:42:12Z INFO Sent 72100/79200  actual 100.0 eps
2026-04-09T06:42:12Z INFO Sent 72150/79200  actual 100.0 eps
2026-04-09T06:42:13Z INFO Sent 72200/79200  actual 100.0 eps
2026-04-09T06:42:13Z INFO Sent 72250/79200  actual 100.0 eps
2026-04-09T06:42:14Z INFO Sent 72300/79200  actual 100.0 eps
2026-04-09T06:42:14Z INFO Sent 72350/79200  actual 100.0 eps
2026-04-09T06:42:15Z INFO Sent 72400/79200  actual 100.0 eps
2026-04-09T06:42:15Z INFO Sent 72450/79200  actual 100.0 eps
2026-04-09T06:42:16Z INFO Sent 72500/79200  actual 100.0 eps
2026-04-09T06:42:16Z INFO Sent 72550/79200  actual 100.0 eps
2026-04-09T06:42:17Z INFO Sent 72600/79200  actual 100.0 eps
2026-04-09T06:42:17Z INFO Sent 72650/79200  actual 100.0 eps
2026-04-09T06:42:18Z INFO Sent 72700/79200  actual 100.0 eps
2026-04-09T06:42:18Z INFO Sent 72750/79200  actual 100.0 eps
2026-04-09T06:42:19Z INFO Sent 72800/79200  actual 100.0 eps
2026-04-09T06:42:19Z INFO Sent 72850/79200  actual 100.0 eps
2026-04-09T06:42:20Z INFO Sent 72900/79200  actual 100.0 eps
2026-04-09T06:42:20Z INFO Sent 72950/79200  actual 100.0 eps
2026-04-09T06:42:21Z INFO Sent 73000/79200  actual 100.0 eps
2026-04-09T06:42:21Z INFO Sent 73050/79200  actual 100.0 eps
2026-04-09T06:42:22Z INFO Sent 73100/79200  actual 100.0 eps
2026-04-09T06:42:22Z INFO Sent 73150/79200  actual 100.0 eps
2026-04-09T06:42:23Z INFO Sent 73200/79200  actual 100.0 eps
2026-04-09T06:42:23Z INFO Sent 73250/79200  actual 100.0 eps
2026-04-09T06:42:24Z INFO Sent 73300/79200  actual 100.0 eps
2026-04-09T06:42:24Z INFO Sent 73350/79200  actual 100.0 eps
2026-04-09T06:42:25Z INFO Sent 73400/79200  actual 100.0 eps
2026-04-09T06:42:25Z INFO Sent 73450/79200  actual 100.0 eps
2026-04-09T06:42:26Z INFO Sent 73500/79200  actual 100.0 eps
2026-04-09T06:42:26Z INFO Sent 73550/79200  actual 100.0 eps
2026-04-09T06:42:27Z INFO Sent 73600/79200  actual 100.0 eps
2026-04-09T06:42:27Z INFO Sent 73650/79200  actual 100.0 eps
2026-04-09T06:42:28Z INFO Sent 73700/79200  actual 100.0 eps
2026-04-09T06:42:28Z INFO Sent 73750/79200  actual 100.0 eps
2026-04-09T06:42:29Z INFO Sent 73800/79200  actual 100.0 eps
2026-04-09T06:42:29Z INFO Sent 73850/79200  actual 100.0 eps
2026-04-09T06:42:30Z INFO Sent 73900/79200  actual 100.0 eps
2026-04-09T06:42:30Z INFO Sent 73950/79200  actual 100.0 eps
2026-04-09T06:42:31Z INFO Sent 74000/79200  actual 100.0 eps
2026-04-09T06:42:31Z INFO Sent 74050/79200  actual 100.0 eps
2026-04-09T06:42:32Z INFO Sent 74100/79200  actual 100.0 eps
2026-04-09T06:42:32Z INFO Sent 74150/79200  actual 100.0 eps
2026-04-09T06:42:33Z INFO Sent 74200/79200  actual 100.0 eps
2026-04-09T06:42:33Z INFO Sent 74250/79200  actual 100.0 eps
2026-04-09T06:42:34Z INFO Sent 74300/79200  actual 100.0 eps
2026-04-09T06:42:34Z INFO Sent 74350/79200  actual 100.0 eps
2026-04-09T06:42:35Z INFO Sent 74400/79200  actual 100.0 eps
2026-04-09T06:42:35Z INFO Sent 74450/79200  actual 100.0 eps
2026-04-09T06:42:36Z INFO Sent 74500/79200  actual 100.0 eps
2026-04-09T06:42:36Z INFO Sent 74550/79200  actual 100.0 eps
2026-04-09T06:42:37Z INFO Sent 74600/79200  actual 100.0 eps
2026-04-09T06:42:37Z INFO Sent 74650/79200  actual 100.0 eps
2026-04-09T06:42:38Z INFO Sent 74700/79200  actual 100.0 eps
2026-04-09T06:42:38Z INFO Sent 74750/79200  actual 100.0 eps
2026-04-09T06:42:39Z INFO Sent 74800/79200  actual 100.0 eps
2026-04-09T06:42:39Z INFO Sent 74850/79200  actual 100.0 eps
2026-04-09T06:42:40Z INFO Sent 74900/79200  actual 100.0 eps
2026-04-09T06:42:40Z INFO Sent 74950/79200  actual 100.0 eps
2026-04-09T06:42:41Z INFO Sent 75000/79200  actual 100.0 eps
2026-04-09T06:42:41Z INFO Sent 75050/79200  actual 100.0 eps
2026-04-09T06:42:42Z INFO Sent 75100/79200  actual 100.0 eps
2026-04-09T06:42:42Z INFO Sent 75150/79200  actual 100.0 eps
2026-04-09T06:42:43Z INFO Sent 75200/79200  actual 100.0 eps
2026-04-09T06:42:43Z INFO Sent 75250/79200  actual 100.0 eps
2026-04-09T06:42:44Z INFO Sent 75300/79200  actual 100.0 eps
2026-04-09T06:42:44Z INFO Sent 75350/79200  actual 100.0 eps
2026-04-09T06:42:45Z INFO Sent 75400/79200  actual 100.0 eps
2026-04-09T06:42:45Z INFO Sent 75450/79200  actual 100.0 eps
2026-04-09T06:42:46Z INFO Sent 75500/79200  actual 100.0 eps
2026-04-09T06:42:46Z INFO Sent 75550/79200  actual 100.0 eps
2026-04-09T06:42:47Z INFO Sent 75600/79200  actual 100.0 eps
2026-04-09T06:42:47Z INFO Sent 75650/79200  actual 100.0 eps
2026-04-09T06:42:48Z INFO Sent 75700/79200  actual 100.0 eps
2026-04-09T06:42:48Z INFO Sent 75750/79200  actual 100.0 eps
2026-04-09T06:42:49Z INFO Sent 75800/79200  actual 100.0 eps
2026-04-09T06:42:49Z INFO Sent 75850/79200  actual 100.0 eps
2026-04-09T06:42:50Z INFO Sent 75900/79200  actual 100.0 eps
2026-04-09T06:42:50Z INFO Sent 75950/79200  actual 100.0 eps
2026-04-09T06:42:51Z INFO Sent 76000/79200  actual 100.0 eps
2026-04-09T06:42:51Z INFO Sent 76050/79200  actual 100.0 eps
2026-04-09T06:42:52Z INFO Sent 76100/79200  actual 100.0 eps
2026-04-09T06:42:52Z INFO Sent 76150/79200  actual 100.0 eps
2026-04-09T06:42:53Z INFO Sent 76200/79200  actual 100.0 eps
2026-04-09T06:42:53Z INFO Sent 76250/79200  actual 100.0 eps
2026-04-09T06:42:54Z INFO Sent 76300/79200  actual 100.0 eps
2026-04-09T06:42:54Z INFO Sent 76350/79200  actual 100.0 eps
2026-04-09T06:42:55Z INFO Sent 76400/79200  actual 100.0 eps
2026-04-09T06:42:55Z INFO Sent 76450/79200  actual 100.0 eps
2026-04-09T06:42:56Z INFO Sent 76500/79200  actual 100.0 eps
2026-04-09T06:42:56Z INFO Sent 76550/79200  actual 100.0 eps
2026-04-09T06:42:57Z INFO Sent 76600/79200  actual 100.0 eps
2026-04-09T06:42:57Z INFO Sent 76650/79200  actual 100.0 eps
2026-04-09T06:42:58Z INFO Sent 76700/79200  actual 100.0 eps
2026-04-09T06:42:58Z INFO Sent 76750/79200  actual 100.0 eps
2026-04-09T06:42:59Z INFO Sent 76800/79200  actual 100.0 eps
2026-04-09T06:42:59Z INFO Sent 76850/79200  actual 100.0 eps
2026-04-09T06:43:00Z INFO Sent 76900/79200  actual 100.0 eps
2026-04-09T06:43:00Z INFO Sent 76950/79200  actual 100.0 eps
2026-04-09T06:43:01Z INFO Sent 77000/79200  actual 100.0 eps
2026-04-09T06:43:02Z INFO Sent 77050/79200  actual 100.0 eps
2026-04-09T06:43:02Z INFO Sent 77100/79200  actual 100.0 eps
2026-04-09T06:43:02Z INFO Sent 77150/79200  actual 100.0 eps
2026-04-09T06:43:03Z INFO Sent 77200/79200  actual 100.0 eps
2026-04-09T06:43:03Z INFO Sent 77250/79200  actual 100.0 eps
2026-04-09T06:43:04Z INFO Sent 77300/79200  actual 100.0 eps
2026-04-09T06:43:04Z INFO Sent 77350/79200  actual 100.0 eps
2026-04-09T06:43:05Z INFO Sent 77400/79200  actual 100.0 eps
2026-04-09T06:43:05Z INFO Sent 77450/79200  actual 100.0 eps
2026-04-09T06:43:06Z INFO Sent 77500/79200  actual 100.0 eps
2026-04-09T06:43:06Z INFO Sent 77550/79200  actual 100.0 eps
2026-04-09T06:43:07Z INFO Sent 77600/79200  actual 100.0 eps
2026-04-09T06:43:07Z INFO Sent 77650/79200  actual 100.0 eps
2026-04-09T06:43:08Z INFO Sent 77700/79200  actual 100.0 eps
2026-04-09T06:43:08Z INFO Sent 77750/79200  actual 100.0 eps
2026-04-09T06:43:09Z INFO Sent 77800/79200  actual 100.0 eps
2026-04-09T06:43:09Z INFO Sent 77850/79200  actual 100.0 eps
2026-04-09T06:43:10Z INFO Sent 77900/79200  actual 100.0 eps
2026-04-09T06:43:10Z INFO Sent 77950/79200  actual 100.0 eps
2026-04-09T06:43:11Z INFO Sent 78000/79200  actual 100.0 eps
2026-04-09T06:43:11Z INFO Sent 78050/79200  actual 100.0 eps
2026-04-09T06:43:12Z INFO Sent 78100/79200  actual 100.0 eps
2026-04-09T06:43:12Z INFO Sent 78150/79200  actual 100.0 eps
2026-04-09T06:43:13Z INFO Sent 78200/79200  actual 100.0 eps
2026-04-09T06:43:13Z INFO Sent 78250/79200  actual 100.0 eps
2026-04-09T06:43:14Z INFO Sent 78300/79200  actual 100.0 eps
2026-04-09T06:43:14Z INFO Sent 78350/79200  actual 100.0 eps
2026-04-09T06:43:15Z INFO Sent 78400/79200  actual 100.0 eps
2026-04-09T06:43:15Z INFO Sent 78450/79200  actual 100.0 eps
2026-04-09T06:43:16Z INFO Sent 78500/79200  actual 100.0 eps
2026-04-09T06:43:16Z INFO Sent 78550/79200  actual 100.0 eps
2026-04-09T06:43:17Z INFO Sent 78600/79200  actual 100.0 eps
2026-04-09T06:43:17Z INFO Sent 78650/79200  actual 100.0 eps
2026-04-09T06:43:18Z INFO Sent 78700/79200  actual 100.0 eps
2026-04-09T06:43:18Z INFO Sent 78750/79200  actual 100.0 eps
2026-04-09T06:43:19Z INFO Sent 78800/79200  actual 100.0 eps
2026-04-09T06:43:19Z INFO Sent 78850/79200  actual 100.0 eps
2026-04-09T06:43:20Z INFO Sent 78900/79200  actual 100.0 eps
2026-04-09T06:43:20Z INFO Sent 78950/79200  actual 100.0 eps
2026-04-09T06:43:21Z INFO Sent 79000/79200  actual 100.0 eps
2026-04-09T06:43:21Z INFO Sent 79050/79200  actual 100.0 eps
2026-04-09T06:43:22Z INFO Sent 79100/79200  actual 100.0 eps
2026-04-09T06:43:22Z INFO Sent 79150/79200  actual 100.0 eps
2026-04-09T06:43:23Z INFO Sent 79200/79200  actual 100.0 eps
2026-04-09T06:43:23Z INFO Replay complete: 79200 events in 792.0s  avg 100.0 eps
2026-04-09T06:43:23Z INFO Closing the Kafka producer with 9223372036.0 secs timeout.
2026-04-09T06:43:23Z INFO <BrokerConnection node_id=1 host=kafka-0.kafka.vacciguard.svc.cluster.local:9092 <connected> [IPv4 ('172.31.29.100', 9092)]>: Closing connection. 
2026-04-09T06:43:23Z INFO Producer finished
```

## Stream Summary Logs

```text
[Stage 2468:========>       (2 + 2) / 4][Stage 2470:>               (0 + 0) / 6][Stage 2468:============>   (3 + 1) / 4][Stage 2470:>               (0 + 1) / 6]                                                                                2026-04-09T06:37:52Z INFO Batch 78 summary valid=508 invalid=0 deduplicated=0 breach=0 processed=508 avg_e2e_latency_s=7.70 p95_e2e_latency_s=9.71
2026-04-09T06:37:55Z INFO Batch 27: wrote 30 latest device states to Redis
2026-04-09T06:37:56Z INFO Batch 79 summary valid=525 invalid=0 deduplicated=0 breach=0 processed=525 avg_e2e_latency_s=6.47 p95_e2e_latency_s=8.66
2026-04-09T06:38:01Z INFO Batch 80 summary valid=387 invalid=0 deduplicated=0 breach=0 processed=387 avg_e2e_latency_s=6.80 p95_e2e_latency_s=8.53
[Stage 2562:========>       (2 + 2) / 4][Stage 2564:>               (0 + 0) / 6]                                                                                2026-04-09T06:38:07Z INFO Batch 81 summary valid=482 invalid=0 deduplicated=0 breach=0 processed=482 avg_e2e_latency_s=7.82 p95_e2e_latency_s=9.89
[Stage 2592:============================>                           (2 + 2) / 4]                                                                                2026-04-09T06:38:10Z INFO Batch 28: wrote 30 latest device states to Redis
2026-04-09T06:38:10Z INFO Batch 82 summary valid=558 invalid=0 deduplicated=0 breach=9 processed=558 avg_e2e_latency_s=6.28 p95_e2e_latency_s=8.59
[Stage 2630:>               (0 + 2) / 4][Stage 2631:>               (0 + 0) / 6][Stage 2630:========>       (2 + 2) / 4][Stage 2631:>               (0 + 0) / 6]                                                                                2026-04-09T06:38:15Z INFO Batch 83 summary valid=357 invalid=0 deduplicated=0 breach=36 processed=357 avg_e2e_latency_s=6.99 p95_e2e_latency_s=8.83
[Stage 2659:========>       (2 + 2) / 4][Stage 2661:>               (0 + 0) / 4]                                                                                2026-04-09T06:38:21Z INFO Batch 84 summary valid=509 invalid=0 deduplicated=0 breach=9 processed=509 avg_e2e_latency_s=8.13 p95_e2e_latency_s=10.32
[Stage 2686:========>       (2 + 2) / 4][Stage 2688:>               (0 + 0) / 6][Stage 2686:============>   (3 + 1) / 4][Stage 2688:==>             (1 + 1) / 6]                                                                                2026-04-09T06:38:25Z INFO Batch 29: wrote 30 latest device states to Redis
2026-04-09T06:38:25Z INFO Batch 85 summary valid=549 invalid=0 deduplicated=0 breach=0 processed=549 avg_e2e_latency_s=6.80 p95_e2e_latency_s=9.28
2026-04-09T06:38:30Z INFO Batch 86 summary valid=398 invalid=0 deduplicated=0 breach=0 processed=398 avg_e2e_latency_s=7.37 p95_e2e_latency_s=9.59
[Stage 2750:========>       (2 + 2) / 4][Stage 2752:>               (0 + 0) / 6]                                                                                2026-04-09T06:38:36Z INFO Batch 87 summary valid=541 invalid=0 deduplicated=0 breach=0 processed=541 avg_e2e_latency_s=8.05 p95_e2e_latency_s=10.94
2026-04-09T06:38:39Z INFO Batch 30: wrote 30 latest device states to Redis
2026-04-09T06:38:40Z INFO Batch 88 summary valid=527 invalid=0 deduplicated=0 breach=0 processed=527 avg_e2e_latency_s=7.18 p95_e2e_latency_s=9.44
2026-04-09T06:38:45Z INFO Batch 89 summary valid=460 invalid=0 deduplicated=0 breach=0 processed=460 avg_e2e_latency_s=7.48 p95_e2e_latency_s=9.64
[Stage 2844:========>       (2 + 2) / 4][Stage 2846:>               (0 + 0) / 6]                                                                                2026-04-09T06:38:51Z INFO Batch 90 summary valid=525 invalid=0 deduplicated=0 breach=36 processed=525 avg_e2e_latency_s=8.02 p95_e2e_latency_s=10.14
2026-04-09T06:38:55Z INFO Batch 31: wrote 30 latest device states to Redis
2026-04-09T06:38:55Z INFO Batch 91 summary valid=537 invalid=0 deduplicated=0 breach=18 processed=537 avg_e2e_latency_s=6.89 p95_e2e_latency_s=9.31
2026-04-09T06:39:00Z INFO Batch 92 summary valid=415 invalid=0 deduplicated=0 breach=0 processed=415 avg_e2e_latency_s=7.37 p95_e2e_latency_s=9.54
[Stage 2938:========>       (2 + 2) / 4][Stage 2940:=====>          (2 + 0) / 6]                                                                                2026-04-09T06:39:07Z INFO Batch 93 summary valid=530 invalid=0 deduplicated=0 breach=0 processed=530 avg_e2e_latency_s=9.71 p95_e2e_latency_s=11.60
2026-04-09T06:39:10Z INFO Batch 32: wrote 30 latest device states to Redis
2026-04-09T06:39:11Z INFO Batch 94 summary valid=713 invalid=0 deduplicated=0 breach=0 processed=713 avg_e2e_latency_s=7.44 p95_e2e_latency_s=10.63
2026-04-09T06:39:16Z INFO Batch 95 summary valid=427 invalid=0 deduplicated=0 breach=0 processed=427 avg_e2e_latency_s=6.98 p95_e2e_latency_s=8.81
[Stage 3032:========>       (2 + 2) / 4][Stage 3034:>               (0 + 0) / 6]                                                                                2026-04-09T06:39:23Z INFO Batch 96 summary valid=486 invalid=0 deduplicated=0 breach=0 processed=486 avg_e2e_latency_s=8.40 p95_e2e_latency_s=10.77
[Stage 3059:==========================================>             (3 + 1) / 4]                                                                                2026-04-09T06:39:27Z INFO Batch 33: wrote 30 latest device states to Redis
2026-04-09T06:39:27Z INFO Batch 97 summary valid=620 invalid=0 deduplicated=0 breach=48 processed=620 avg_e2e_latency_s=7.48 p95_e2e_latency_s=10.39
2026-04-09T06:39:33Z INFO Batch 98 summary valid=492 invalid=0 deduplicated=0 breach=6 processed=492 avg_e2e_latency_s=7.70 p95_e2e_latency_s=10.17
[Stage 3126:========>       (2 + 2) / 4][Stage 3128:>               (0 + 0) / 6][Stage 3126:============>   (3 + 1) / 4][Stage 3128:==>             (1 + 1) / 6]                                                                                2026-04-09T06:39:39Z INFO Batch 99 summary valid=526 invalid=0 deduplicated=0 breach=0 processed=526 avg_e2e_latency_s=9.25 p95_e2e_latency_s=11.81
2026-04-09T06:39:44Z INFO Batch 34: wrote 30 latest device states to Redis
2026-04-09T06:39:44Z INFO Batch 100 summary valid=674 invalid=0 deduplicated=0 breach=0 processed=674 avg_e2e_latency_s=7.56 p95_e2e_latency_s=10.13
2026-04-09T06:39:49Z INFO Batch 101 summary valid=457 invalid=0 deduplicated=0 breach=0 processed=457 avg_e2e_latency_s=7.57 p95_e2e_latency_s=9.76
[Stage 3220:========>       (2 + 2) / 4][Stage 3222:>               (0 + 0) / 6][Stage 3220:================(4 + 0) / 4][Stage 3222:>               (0 + 2) / 6]                                                                                2026-04-09T06:39:56Z INFO Batch 102 summary valid=524 invalid=0 deduplicated=0 breach=0 processed=524 avg_e2e_latency_s=9.65 p95_e2e_latency_s=11.78
[Stage 3250:==============================================>         (5 + 1) / 6]                                                                                2026-04-09T06:40:00Z INFO Batch 35: wrote 30 latest device states to Redis
2026-04-09T06:40:01Z INFO Batch 103 summary valid=702 invalid=0 deduplicated=0 breach=26 processed=702 avg_e2e_latency_s=7.96 p95_e2e_latency_s=11.22
2026-04-09T06:40:07Z INFO Batch 104 summary valid=477 invalid=0 deduplicated=0 breach=28 processed=477 avg_e2e_latency_s=7.99 p95_e2e_latency_s=10.13
[Stage 3314:========>       (2 + 2) / 4][Stage 3316:>               (0 + 0) / 6]                                                                                2026-04-09T06:40:13Z INFO Batch 105 summary valid=551 invalid=0 deduplicated=0 breach=0 processed=551 avg_e2e_latency_s=8.90 p95_e2e_latency_s=11.22
2026-04-09T06:40:17Z INFO Batch 36: wrote 30 latest device states to Redis
2026-04-09T06:40:17Z INFO Batch 106 summary valid=611 invalid=0 deduplicated=0 breach=0 processed=611 avg_e2e_latency_s=7.69 p95_e2e_latency_s=10.78
2026-04-09T06:40:24Z INFO Batch 107 summary valid=491 invalid=0 deduplicated=0 breach=0 processed=491 avg_e2e_latency_s=7.93 p95_e2e_latency_s=10.54
2026-04-09T06:40:28Z INFO Batch 108 summary valid=577 invalid=0 deduplicated=0 breach=0 processed=577 avg_e2e_latency_s=6.95 p95_e2e_latency_s=9.89
2026-04-09T06:40:30Z INFO Batch 37: wrote 30 latest device states to Redis
2026-04-09T06:40:31Z INFO Batch 109 summary valid=423 invalid=0 deduplicated=0 breach=0 processed=423 avg_e2e_latency_s=5.86 p95_e2e_latency_s=7.80
2026-04-09T06:40:36Z INFO Batch 110 summary valid=392 invalid=0 deduplicated=0 breach=18 processed=392 avg_e2e_latency_s=6.81 p95_e2e_latency_s=8.83
[Stage 3502:============>   (3 + 1) / 4][Stage 3504:==>             (1 + 1) / 6][Stage 3502:==========================================>             (3 + 1) / 4]                                                                                2026-04-09T06:40:42Z INFO Batch 111 summary valid=485 invalid=0 deduplicated=0 breach=36 processed=485 avg_e2e_latency_s=7.87 p95_e2e_latency_s=10.27
2026-04-09T06:40:45Z INFO Batch 38: wrote 30 latest device states to Redis
2026-04-09T06:40:46Z INFO Batch 112 summary valid=544 invalid=0 deduplicated=0 breach=0 processed=544 avg_e2e_latency_s=6.63 p95_e2e_latency_s=9.18
2026-04-09T06:40:51Z INFO Batch 113 summary valid=417 invalid=0 deduplicated=0 breach=0 processed=417 avg_e2e_latency_s=7.02 p95_e2e_latency_s=9.36
[Stage 3596:============>   (3 + 1) / 4][Stage 3598:=====>          (2 + 1) / 6][Stage 3596:============>   (3 + 1) / 4][Stage 3598:==========>     (4 + 1) / 6]                                                                                2026-04-09T06:40:57Z INFO Batch 114 summary valid=494 invalid=0 deduplicated=0 breach=0 processed=494 avg_e2e_latency_s=8.23 p95_e2e_latency_s=10.15
2026-04-09T06:41:00Z INFO Batch 39: wrote 30 latest device states to Redis
2026-04-09T06:41:01Z INFO Batch 115 summary valid=591 invalid=0 deduplicated=0 breach=0 processed=591 avg_e2e_latency_s=6.88 p95_e2e_latency_s=9.22
2026-04-09T06:41:06Z INFO Batch 116 summary valid=395 invalid=0 deduplicated=0 breach=0 processed=395 avg_e2e_latency_s=7.27 p95_e2e_latency_s=9.53
[Stage 3690:============>   (3 + 1) / 4][Stage 3692:>               (0 + 1) / 6][Stage 3690:============>   (3 + 1) / 4][Stage 3692:=============>  (5 + 1) / 6]                                                                                2026-04-09T06:41:12Z INFO Batch 117 summary valid=533 invalid=0 deduplicated=0 breach=4 processed=533 avg_e2e_latency_s=8.31 p95_e2e_latency_s=10.24
2026-04-09T06:41:15Z INFO Batch 40: wrote 30 latest device states to Redis
2026-04-09T06:41:16Z INFO Batch 118 summary valid=582 invalid=0 deduplicated=0 breach=50 processed=582 avg_e2e_latency_s=6.89 p95_e2e_latency_s=9.38
2026-04-09T06:41:21Z INFO Batch 119 summary valid=415 invalid=0 deduplicated=0 breach=0 processed=415 avg_e2e_latency_s=7.04 p95_e2e_latency_s=8.49
[Stage 3784:============>   (3 + 1) / 4][Stage 3786:==>             (1 + 1) / 6][Stage 3784:============>   (3 + 1) / 4][Stage 3786:==========>     (4 + 1) / 6]                                                                                2026-04-09T06:41:27Z INFO Batch 120 summary valid=494 invalid=0 deduplicated=0 breach=0 processed=494 avg_e2e_latency_s=8.31 p95_e2e_latency_s=10.32
2026-04-09T06:41:33Z INFO Batch 41: wrote 30 latest device states to Redis
2026-04-09T06:41:33Z INFO Batch 121 summary valid=789 invalid=0 deduplicated=0 breach=0 processed=789 avg_e2e_latency_s=8.28 p95_e2e_latency_s=11.71
2026-04-09T06:41:39Z INFO Batch 122 summary valid=436 invalid=0 deduplicated=0 breach=0 processed=436 avg_e2e_latency_s=7.67 p95_e2e_latency_s=9.22
                                                                                2026-04-09T06:41:45Z INFO Batch 123 summary valid=549 invalid=0 deduplicated=0 breach=0 processed=549 avg_e2e_latency_s=8.87 p95_e2e_latency_s=11.34
[Stage 3908:============================>                           (2 + 2) / 4][Stage 3908:============>   (3 + 1) / 4][Stage 3910:=====>          (2 + 1) / 6][Stage 3908:==========================================>             (3 + 1) / 4]                                                                                2026-04-09T06:41:49Z INFO Batch 42: wrote 30 latest device states to Redis
2026-04-09T06:41:49Z INFO Batch 124 summary valid=618 invalid=0 deduplicated=0 breach=33 processed=618 avg_e2e_latency_s=7.28 p95_e2e_latency_s=9.58
2026-04-09T06:41:55Z INFO Batch 125 summary valid=433 invalid=0 deduplicated=0 breach=21 processed=433 avg_e2e_latency_s=7.38 p95_e2e_latency_s=8.96
[Stage 3972:============>   (3 + 1) / 4][Stage 3974:==>             (1 + 1) / 6][Stage 3972:============>   (3 + 1) / 4][Stage 3974:=============>  (5 + 1) / 6]                                                                                2026-04-09T06:42:02Z INFO Batch 126 summary valid=521 invalid=0 deduplicated=0 breach=0 processed=521 avg_e2e_latency_s=9.40 p95_e2e_latency_s=11.74
2026-04-09T06:42:05Z INFO Batch 43: wrote 30 latest device states to Redis
2026-04-09T06:42:07Z INFO Batch 127 summary valid=714 invalid=0 deduplicated=0 breach=0 processed=714 avg_e2e_latency_s=8.62 p95_e2e_latency_s=12.13
2026-04-09T06:42:12Z INFO Batch 128 summary valid=542 invalid=0 deduplicated=0 breach=0 processed=542 avg_e2e_latency_s=7.44 p95_e2e_latency_s=9.25
[Stage 4066:============>   (3 + 1) / 4][Stage 4068:>               (0 + 1) / 6][Stage 4066:============>   (3 + 1) / 4][Stage 4068:=====>          (2 + 1) / 6]                                                                                2026-04-09T06:42:18Z INFO Batch 129 summary valid=494 invalid=0 deduplicated=1 breach=2 processed=493 avg_e2e_latency_s=8.37 p95_e2e_latency_s=10.35
2026-04-09T06:42:21Z INFO Batch 44: wrote 30 latest device states to Redis
2026-04-09T06:42:22Z INFO Batch 130 summary valid=621 invalid=0 deduplicated=0 breach=7 processed=621 avg_e2e_latency_s=7.19 p95_e2e_latency_s=9.74
2026-04-09T06:42:27Z INFO Batch 131 summary valid=413 invalid=0 deduplicated=0 breach=7 processed=413 avg_e2e_latency_s=6.95 p95_e2e_latency_s=8.68
[Stage 4163:========>       (2 + 2) / 4][Stage 4164:==========>     (4 + 0) / 6]                                                                                2026-04-09T06:42:33Z INFO Batch 132 summary valid=487 invalid=0 deduplicated=0 breach=7 processed=487 avg_e2e_latency_s=8.46 p95_e2e_latency_s=10.67
[Stage 4190:==================>                                     (2 + 1) / 6]                                                                                2026-04-09T06:42:37Z INFO Batch 45: wrote 30 latest device states to Redis
2026-04-09T06:42:38Z INFO Batch 133 summary valid=611 invalid=0 deduplicated=0 breach=7 processed=611 avg_e2e_latency_s=8.09 p95_e2e_latency_s=10.80
2026-04-09T06:42:43Z INFO Batch 134 summary valid=523 invalid=0 deduplicated=0 breach=7 processed=523 avg_e2e_latency_s=6.96 p95_e2e_latency_s=9.34
2026-04-09T06:42:47Z INFO Batch 135 summary valid=429 invalid=0 deduplicated=0 breach=6 processed=429 avg_e2e_latency_s=5.61 p95_e2e_latency_s=7.74
2026-04-09T06:42:51Z INFO Batch 136 summary valid=368 invalid=0 deduplicated=0 breach=5 processed=368 avg_e2e_latency_s=6.31 p95_e2e_latency_s=8.44
2026-04-09T06:42:55Z INFO Batch 137 summary valid=441 invalid=0 deduplicated=0 breach=8 processed=441 avg_e2e_latency_s=6.00 p95_e2e_latency_s=8.15
2026-04-09T06:42:59Z INFO Batch 138 summary valid=393 invalid=0 deduplicated=0 breach=3 processed=393 avg_e2e_latency_s=5.88 p95_e2e_latency_s=7.22
2026-04-09T06:43:02Z INFO Batch 139 summary valid=422 invalid=0 deduplicated=0 breach=11 processed=422 avg_e2e_latency_s=5.22 p95_e2e_latency_s=6.62
2026-04-09T06:43:07Z INFO Batch 140 summary valid=498 invalid=0 deduplicated=0 breach=6 processed=498 avg_e2e_latency_s=5.72 p95_e2e_latency_s=7.73
                                                                                2026-04-09T06:43:19Z INFO Batch 141 summary valid=408 invalid=92 deduplicated=0 breach=3 processed=408 avg_e2e_latency_s=12.24 p95_e2e_latency_s=13.79
2026-04-09T06:43:28Z INFO Batch 142 summary valid=0 invalid=986 deduplicated=0 breach=0 processed=0 avg_e2e_latency_s=n/a p95_e2e_latency_s=n/a
2026-04-09T06:43:39Z INFO Batch 143 summary valid=0 invalid=362 deduplicated=0 breach=0 processed=0 avg_e2e_latency_s=n/a p95_e2e_latency_s=n/a
```

## S3 Objects

```text
2026-04-09 12:12:35          0 evaluations/baseline-normal-20260409t120000z/breach_windows/_SUCCESS
2026-04-09 12:03:23        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-026505ef-b2c6-48d2-8954-85516423834d-c000.json
2026-04-09 12:05:01        206 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-0cdb9053-b6e5-4950-8715-a35dd6dce2c9-c000.json
2026-04-09 12:12:18        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-11d2ddec-4d11-4ca5-956a-4db1d500ffd0-c000.json
2026-04-09 12:09:40        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-19ca195f-82f1-432c-a11b-dd922353b60f-c000.json
2026-04-09 12:03:58        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-1e05a600-712e-4b2d-ad07-bb0c9f9f1e07-c000.json
2026-04-09 12:01:03        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-2098b647-4991-4685-ab32-348cde4f94fa-c000.json
2026-04-09 12:08:21        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-232f523f-5c85-4a22-8a03-876182dc9c36-c000.json
2026-04-09 12:11:46        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-286c2f73-69d0-4a9e-ac00-c8061051ea9d-c000.json
2026-04-09 12:01:39        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-2bfdfed0-a6d7-4ee7-a6cb-829c78eae148-c000.json
2026-04-09 12:01:21        192 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-2e83ae39-5d6f-4fc8-8fce-7686255930e2-c000.json
2026-04-09 12:09:22        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-33b1236f-26c5-473c-8736-5fb0d6938d43-c000.json
2026-04-09 12:04:46        206 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-33ebabc2-cb61-4254-b498-3ae6c4536047-c000.json
2026-04-09 12:11:13        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-4c5637c5-8b07-4248-b5ae-18e24465a007-c000.json
2026-04-09 12:09:56        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-5524493a-2b8d-42da-b5fe-c42e5a38734e-c000.json
2026-04-09 12:12:34        756 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-58ab6965-682a-485b-befb-22682a5d0fbe-c000.json
2026-04-09 12:07:39        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-591fcb26-3f5e-4521-bddf-40ede702e49f-c000.json
2026-04-09 12:06:52        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-5a6d8093-6f3e-40df-a0e7-5eef61024f75-c000.json
2026-04-09 12:09:07        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-5a9c0db0-b68c-4273-adb3-2eedfbde877f-c000.json
2026-04-09 12:05:16        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-5d7eaf8a-e521-4ebd-9caf-9e9fdfad51bd-c000.json
2026-04-09 12:03:41        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-60267cf1-43ac-467f-9213-24445ba06715-c000.json
2026-04-09 12:08:51        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-605c31fc-1550-4d26-9dc4-9878c99a4071-c000.json
2026-04-09 12:02:49        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-62212723-da78-4e66-805e-c135c44da537-c000.json
2026-04-09 12:03:06        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-6bc91831-76c7-4977-b7cf-e3778a429660-c000.json
2026-04-09 12:07:53        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-6f357572-3e3b-46bc-a72c-b280fa431fd2-c000.json
2026-04-09 12:10:13        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-6f54d539-989b-4fef-89eb-ec74231183f2-c000.json
2026-04-09 12:10:42        811 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-7bc89d68-856f-4116-8c63-cb5581fabf58-c000.json
2026-04-09 12:02:13        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-82f6c8fa-8042-4dbe-bb5c-84304b6885d9-c000.json
2026-04-09 12:07:24        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-835bcb0a-df4e-46df-9afa-6d7f7a6efbf0-c000.json
2026-04-09 12:00:42        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-8cfbf283-6f51-4b2f-92f8-d9d00fe626a6-c000.json
2026-04-09 12:08:07        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-8d9c6abd-4af8-4159-b506-c56dd240993b-c000.json
2026-04-09 12:06:20        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-8fc398eb-5f47-4391-91a9-1296d3f9467e-c000.json
2026-04-09 12:06:03        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-9385855f-9648-488d-8e5b-4e91893146a8-c000.json
2026-04-09 12:10:27       1516 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-96563691-9b97-40f2-8708-f5a03eb86be3-c000.json
2026-04-09 12:05:48        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-9b88a3dd-03a3-4bf1-8e8c-cca1553f49eb-c000.json
2026-04-09 12:02:30        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-a77a5897-0e33-4665-a120-f0630a73ef5f-c000.json
2026-04-09 12:05:32        979 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-aefc2296-6179-4a14-b86d-e86f31db7601-c000.json
2026-04-09 12:01:56        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-b4ae82c0-50a6-48e8-a644-ee3d3a9bfe87-c000.json
2026-04-09 12:04:16        191 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-b4d364f1-3039-4c03-9393-9e0be77e9d88-c000.json
2026-04-09 12:07:08        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-cc3a9381-0fdb-45b0-8be4-5709ca2e70b6-c000.json
2026-04-09 12:12:01        808 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-d15a502e-905a-4099-b224-dca50f264163-c000.json
2026-04-09 12:04:31        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-d9eb4d99-8051-4149-8e3f-62d7a50fddfc-c000.json
2026-04-09 12:08:36        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-dfc38c4f-7a5f-4242-8857-a19271f4e9e7-c000.json
2026-04-09 12:06:36        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-f3e5115f-8694-4388-a865-81095cb315c4-c000.json
2026-04-09 12:10:57        810 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-f42c57bf-8192-4887-a4af-84365a665637-c000.json
2026-04-09 12:11:27        811 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00000-fd118bd2-b037-47f5-b02d-f4f63058b990-c000.json
2026-04-09 12:03:24        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-026505ef-b2c6-48d2-8954-85516423834d-c000.json
2026-04-09 12:05:01        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-0cdb9053-b6e5-4950-8715-a35dd6dce2c9-c000.json
2026-04-09 12:12:19        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-11d2ddec-4d11-4ca5-956a-4db1d500ffd0-c000.json
2026-04-09 12:09:40        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-19ca195f-82f1-432c-a11b-dd922353b60f-c000.json
2026-04-09 12:03:59        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-1e05a600-712e-4b2d-ad07-bb0c9f9f1e07-c000.json
2026-04-09 12:01:03        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-2098b647-4991-4685-ab32-348cde4f94fa-c000.json
2026-04-09 12:08:22        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-232f523f-5c85-4a22-8a03-876182dc9c36-c000.json
2026-04-09 12:11:46        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-286c2f73-69d0-4a9e-ac00-c8061051ea9d-c000.json
2026-04-09 12:01:39        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-2bfdfed0-a6d7-4ee7-a6cb-829c78eae148-c000.json
2026-04-09 12:01:21        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-2e83ae39-5d6f-4fc8-8fce-7686255930e2-c000.json
2026-04-09 12:09:23        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-33b1236f-26c5-473c-8736-5fb0d6938d43-c000.json
2026-04-09 12:04:47        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-33ebabc2-cb61-4254-b498-3ae6c4536047-c000.json
2026-04-09 12:11:13        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-4c5637c5-8b07-4248-b5ae-18e24465a007-c000.json
2026-04-09 12:09:56        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-5524493a-2b8d-42da-b5fe-c42e5a38734e-c000.json
2026-04-09 12:12:35        756 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-58ab6965-682a-485b-befb-22682a5d0fbe-c000.json
2026-04-09 12:07:39        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-591fcb26-3f5e-4521-bddf-40ede702e49f-c000.json
2026-04-09 12:06:52        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-5a6d8093-6f3e-40df-a0e7-5eef61024f75-c000.json
2026-04-09 12:09:07        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-5a9c0db0-b68c-4273-adb3-2eedfbde877f-c000.json
2026-04-09 12:05:17        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-5d7eaf8a-e521-4ebd-9caf-9e9fdfad51bd-c000.json
2026-04-09 12:03:41        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-60267cf1-43ac-467f-9213-24445ba06715-c000.json
2026-04-09 12:08:51        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-605c31fc-1550-4d26-9dc4-9878c99a4071-c000.json
2026-04-09 12:02:49        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-62212723-da78-4e66-805e-c135c44da537-c000.json
2026-04-09 12:03:06        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-6bc91831-76c7-4977-b7cf-e3778a429660-c000.json
2026-04-09 12:07:53        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-6f357572-3e3b-46bc-a72c-b280fa431fd2-c000.json
2026-04-09 12:10:13        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-6f54d539-989b-4fef-89eb-ec74231183f2-c000.json
2026-04-09 12:10:43        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-7bc89d68-856f-4116-8c63-cb5581fabf58-c000.json
2026-04-09 12:02:14        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-82f6c8fa-8042-4dbe-bb5c-84304b6885d9-c000.json
2026-04-09 12:07:25        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-835bcb0a-df4e-46df-9afa-6d7f7a6efbf0-c000.json
2026-04-09 12:00:42        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-8cfbf283-6f51-4b2f-92f8-d9d00fe626a6-c000.json
2026-04-09 12:08:07        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-8d9c6abd-4af8-4159-b506-c56dd240993b-c000.json
2026-04-09 12:06:20        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-8fc398eb-5f47-4391-91a9-1296d3f9467e-c000.json
2026-04-09 12:06:04        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-9385855f-9648-488d-8e5b-4e91893146a8-c000.json
2026-04-09 12:10:27        946 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-96563691-9b97-40f2-8708-f5a03eb86be3-c000.json
2026-04-09 12:05:48        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-9b88a3dd-03a3-4bf1-8e8c-cca1553f49eb-c000.json
2026-04-09 12:02:30        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-a77a5897-0e33-4665-a120-f0630a73ef5f-c000.json
2026-04-09 12:05:33        567 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-aefc2296-6179-4a14-b86d-e86f31db7601-c000.json
2026-04-09 12:01:57        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-b4ae82c0-50a6-48e8-a644-ee3d3a9bfe87-c000.json
2026-04-09 12:04:16        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-b4d364f1-3039-4c03-9393-9e0be77e9d88-c000.json
2026-04-09 12:07:09        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-cc3a9381-0fdb-45b0-8be4-5709ca2e70b6-c000.json
2026-04-09 12:12:02        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-d15a502e-905a-4099-b224-dca50f264163-c000.json
2026-04-09 12:04:32        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-d9eb4d99-8051-4149-8e3f-62d7a50fddfc-c000.json
2026-04-09 12:08:36        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-dfc38c4f-7a5f-4242-8857-a19271f4e9e7-c000.json
2026-04-09 12:06:36        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-f3e5115f-8694-4388-a865-81095cb315c4-c000.json
2026-04-09 12:10:58        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-f42c57bf-8192-4887-a4af-84365a665637-c000.json
2026-04-09 12:11:28        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00001-fd118bd2-b037-47f5-b02d-f4f63058b990-c000.json
2026-04-09 12:03:24        570 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-026505ef-b2c6-48d2-8954-85516423834d-c000.json
2026-04-09 12:05:02        586 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-0cdb9053-b6e5-4950-8715-a35dd6dce2c9-c000.json
2026-04-09 12:09:41        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-19ca195f-82f1-432c-a11b-dd922353b60f-c000.json
2026-04-09 12:03:59        570 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-1e05a600-712e-4b2d-ad07-bb0c9f9f1e07-c000.json
2026-04-09 12:01:03        570 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-2098b647-4991-4685-ab32-348cde4f94fa-c000.json
2026-04-09 12:08:22        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-232f523f-5c85-4a22-8a03-876182dc9c36-c000.json
2026-04-09 12:01:40        570 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-2bfdfed0-a6d7-4ee7-a6cb-829c78eae148-c000.json
2026-04-09 12:01:22        572 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-2e83ae39-5d6f-4fc8-8fce-7686255930e2-c000.json
2026-04-09 12:09:23        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-33b1236f-26c5-473c-8736-5fb0d6938d43-c000.json
2026-04-09 12:04:47        586 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-33ebabc2-cb61-4254-b498-3ae6c4536047-c000.json
2026-04-09 12:09:57        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-5524493a-2b8d-42da-b5fe-c42e5a38734e-c000.json
2026-04-09 12:07:39        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-591fcb26-3f5e-4521-bddf-40ede702e49f-c000.json
2026-04-09 12:06:53        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-5a6d8093-6f3e-40df-a0e7-5eef61024f75-c000.json
2026-04-09 12:09:08        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-5a9c0db0-b68c-4273-adb3-2eedfbde877f-c000.json
2026-04-09 12:05:17        570 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-5d7eaf8a-e521-4ebd-9caf-9e9fdfad51bd-c000.json
2026-04-09 12:03:42        587 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-60267cf1-43ac-467f-9213-24445ba06715-c000.json
2026-04-09 12:08:51        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-605c31fc-1550-4d26-9dc4-9878c99a4071-c000.json
2026-04-09 12:02:50        570 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-62212723-da78-4e66-805e-c135c44da537-c000.json
2026-04-09 12:03:07        586 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-6bc91831-76c7-4977-b7cf-e3778a429660-c000.json
2026-04-09 12:07:53        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-6f357572-3e3b-46bc-a72c-b280fa431fd2-c000.json
2026-04-09 12:10:14        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-6f54d539-989b-4fef-89eb-ec74231183f2-c000.json
2026-04-09 12:02:14        570 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-82f6c8fa-8042-4dbe-bb5c-84304b6885d9-c000.json
2026-04-09 12:07:25        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-835bcb0a-df4e-46df-9afa-6d7f7a6efbf0-c000.json
2026-04-09 12:00:42        587 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-8cfbf283-6f51-4b2f-92f8-d9d00fe626a6-c000.json
2026-04-09 12:08:08        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-8d9c6abd-4af8-4159-b506-c56dd240993b-c000.json
2026-04-09 12:06:20        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-8fc398eb-5f47-4391-91a9-1296d3f9467e-c000.json
2026-04-09 12:06:04        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-9385855f-9648-488d-8e5b-4e91893146a8-c000.json
2026-04-09 12:10:27        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-96563691-9b97-40f2-8708-f5a03eb86be3-c000.json
2026-04-09 12:05:48        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-9b88a3dd-03a3-4bf1-8e8c-cca1553f49eb-c000.json
2026-04-09 12:02:31        587 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-a77a5897-0e33-4665-a120-f0630a73ef5f-c000.json
2026-04-09 12:05:33       1357 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-aefc2296-6179-4a14-b86d-e86f31db7601-c000.json
2026-04-09 12:01:57        587 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-b4ae82c0-50a6-48e8-a644-ee3d3a9bfe87-c000.json
2026-04-09 12:04:17        586 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-b4d364f1-3039-4c03-9393-9e0be77e9d88-c000.json
2026-04-09 12:07:09        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-cc3a9381-0fdb-45b0-8be4-5709ca2e70b6-c000.json
2026-04-09 12:04:32        570 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-d9eb4d99-8051-4149-8e3f-62d7a50fddfc-c000.json
2026-04-09 12:08:37        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-dfc38c4f-7a5f-4242-8857-a19271f4e9e7-c000.json
2026-04-09 12:06:36        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00002-f3e5115f-8694-4388-a865-81095cb315c4-c000.json
2026-04-09 12:03:24        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-026505ef-b2c6-48d2-8954-85516423834d-c000.json
2026-04-09 12:05:02        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-0cdb9053-b6e5-4950-8715-a35dd6dce2c9-c000.json
2026-04-09 12:12:19        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-11d2ddec-4d11-4ca5-956a-4db1d500ffd0-c000.json
2026-04-09 12:09:41        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-19ca195f-82f1-432c-a11b-dd922353b60f-c000.json
2026-04-09 12:04:00        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-1e05a600-712e-4b2d-ad07-bb0c9f9f1e07-c000.json
2026-04-09 12:01:04        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-2098b647-4991-4685-ab32-348cde4f94fa-c000.json
2026-04-09 12:08:22        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-232f523f-5c85-4a22-8a03-876182dc9c36-c000.json
2026-04-09 12:11:47        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-286c2f73-69d0-4a9e-ac00-c8061051ea9d-c000.json
2026-04-09 12:01:40        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-2bfdfed0-a6d7-4ee7-a6cb-829c78eae148-c000.json
2026-04-09 12:01:23        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-2e83ae39-5d6f-4fc8-8fce-7686255930e2-c000.json
2026-04-09 12:09:24        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-33b1236f-26c5-473c-8736-5fb0d6938d43-c000.json
2026-04-09 12:04:47        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-33ebabc2-cb61-4254-b498-3ae6c4536047-c000.json
2026-04-09 12:11:13        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-4c5637c5-8b07-4248-b5ae-18e24465a007-c000.json
2026-04-09 12:09:57        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-5524493a-2b8d-42da-b5fe-c42e5a38734e-c000.json
2026-04-09 12:12:35        378 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-58ab6965-682a-485b-befb-22682a5d0fbe-c000.json
2026-04-09 12:07:40        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-591fcb26-3f5e-4521-bddf-40ede702e49f-c000.json
2026-04-09 12:06:53        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-5a6d8093-6f3e-40df-a0e7-5eef61024f75-c000.json
2026-04-09 12:09:08        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-5a9c0db0-b68c-4273-adb3-2eedfbde877f-c000.json
2026-04-09 12:05:18        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-5d7eaf8a-e521-4ebd-9caf-9e9fdfad51bd-c000.json
2026-04-09 12:03:42        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-60267cf1-43ac-467f-9213-24445ba06715-c000.json
2026-04-09 12:08:52        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-605c31fc-1550-4d26-9dc4-9878c99a4071-c000.json
2026-04-09 12:02:50        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-62212723-da78-4e66-805e-c135c44da537-c000.json
2026-04-09 12:03:07        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-6bc91831-76c7-4977-b7cf-e3778a429660-c000.json
2026-04-09 12:07:54        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-6f357572-3e3b-46bc-a72c-b280fa431fd2-c000.json
2026-04-09 12:10:14        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-6f54d539-989b-4fef-89eb-ec74231183f2-c000.json
2026-04-09 12:10:43        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-7bc89d68-856f-4116-8c63-cb5581fabf58-c000.json
2026-04-09 12:02:15        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-82f6c8fa-8042-4dbe-bb5c-84304b6885d9-c000.json
2026-04-09 12:07:25        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-835bcb0a-df4e-46df-9afa-6d7f7a6efbf0-c000.json
2026-04-09 12:00:43        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-8cfbf283-6f51-4b2f-92f8-d9d00fe626a6-c000.json
2026-04-09 12:08:08        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-8d9c6abd-4af8-4159-b506-c56dd240993b-c000.json
2026-04-09 12:06:21        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-8fc398eb-5f47-4391-91a9-1296d3f9467e-c000.json
2026-04-09 12:06:05        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-9385855f-9648-488d-8e5b-4e91893146a8-c000.json
2026-04-09 12:10:28        568 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-96563691-9b97-40f2-8708-f5a03eb86be3-c000.json
2026-04-09 12:05:49        190 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-9b88a3dd-03a3-4bf1-8e8c-cca1553f49eb-c000.json
2026-04-09 12:02:31        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-a77a5897-0e33-4665-a120-f0630a73ef5f-c000.json
2026-04-09 12:05:33        979 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-aefc2296-6179-4a14-b86d-e86f31db7601-c000.json
2026-04-09 12:01:57        777 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-b4ae82c0-50a6-48e8-a644-ee3d3a9bfe87-c000.json
2026-04-09 12:04:17        776 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-b4d364f1-3039-4c03-9393-9e0be77e9d88-c000.json
2026-04-09 12:07:10        207 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-cc3a9381-0fdb-45b0-8be4-5709ca2e70b6-c000.json
2026-04-09 12:12:02        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-d15a502e-905a-4099-b224-dca50f264163-c000.json
2026-04-09 12:04:32        760 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-d9eb4d99-8051-4149-8e3f-62d7a50fddfc-c000.json
2026-04-09 12:08:37        206 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-dfc38c4f-7a5f-4242-8857-a19271f4e9e7-c000.json
2026-04-09 12:06:37        206 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-f3e5115f-8694-4388-a865-81095cb315c4-c000.json
2026-04-09 12:10:58        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-f42c57bf-8192-4887-a4af-84365a665637-c000.json
2026-04-09 12:11:28        380 evaluations/baseline-normal-20260409t120000z/breach_windows/part-00003-fd118bd2-b037-47f5-b02d-f4f63058b990-c000.json
2026-04-09 12:09:45         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/100
2026-04-09 12:09:51         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/101
2026-04-09 12:09:58         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/102
2026-04-09 12:10:02         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/103
2026-04-09 12:10:08         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/104
2026-04-09 12:10:14         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/105
2026-04-09 12:10:19         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/106
2026-04-09 12:10:25         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/107
2026-04-09 12:10:29         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/108
2026-04-09 12:10:33         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/109
2026-04-09 12:10:38         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/110
2026-04-09 12:10:43         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/111
2026-04-09 12:10:47         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/112
2026-04-09 12:10:52         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/113
2026-04-09 12:10:58         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/114
2026-04-09 12:11:02         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/115
2026-04-09 12:11:08         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/116
2026-04-09 12:11:13         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/117
2026-04-09 12:11:17         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/118
2026-04-09 12:11:22         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/119
2026-04-09 12:11:28         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/120
2026-04-09 12:11:35         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/121
2026-04-09 12:11:40         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/122
2026-04-09 12:11:46         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/123
2026-04-09 12:11:51         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/124
2026-04-09 12:11:56         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/125
2026-04-09 12:12:03         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/126
2026-04-09 12:12:08         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/127
2026-04-09 12:12:13         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/128
2026-04-09 12:12:20         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/129
2026-04-09 12:12:24         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/130
2026-04-09 12:12:29         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/131
2026-04-09 12:12:35         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/132
2026-04-09 12:12:40         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/133
2026-04-09 12:12:44         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/134
2026-04-09 12:12:48         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/135
2026-04-09 12:12:52         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/136
2026-04-09 12:12:56         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/137
2026-04-09 12:13:00         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/138
2026-04-09 12:13:04         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/139
2026-04-09 12:13:09         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/140
2026-04-09 12:13:20         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/141
2026-04-09 12:13:30         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/142
2026-04-09 12:13:40         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/143
2026-04-09 12:04:51         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/43
2026-04-09 12:04:56         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/44
2026-04-09 12:05:03         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/45
2026-04-09 12:05:07         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/46
2026-04-09 12:05:12         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/47
2026-04-09 12:05:17         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/48
2026-04-09 12:05:22         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/49
2026-04-09 12:05:27         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/50
2026-04-09 12:05:33         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/51
2026-04-09 12:05:38         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/52
2026-04-09 12:05:43         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/53
2026-04-09 12:05:49         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/54
2026-04-09 12:05:53         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/55
2026-04-09 12:05:58         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/56
2026-04-09 12:06:05         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/57
2026-04-09 12:06:09         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/58
2026-04-09 12:06:15         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/59
2026-04-09 12:06:21         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/60
2026-04-09 12:06:25         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/61
2026-04-09 12:06:31         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/62
2026-04-09 12:06:37         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/63
2026-04-09 12:06:42         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/64
2026-04-09 12:06:47         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/65
2026-04-09 12:06:53         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/66
2026-04-09 12:06:58         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/67
2026-04-09 12:07:03         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/68
2026-04-09 12:07:10         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/69
2026-04-09 12:07:14         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/70
2026-04-09 12:07:20         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/71
2026-04-09 12:07:25         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/72
2026-04-09 12:07:29         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/73
2026-04-09 12:07:35         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/74
2026-04-09 12:07:40         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/75
2026-04-09 12:07:43         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/76
2026-04-09 12:07:49         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/77
2026-04-09 12:07:54         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/78
2026-04-09 12:07:58         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/79
2026-04-09 12:08:02         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/80
2026-04-09 12:08:08         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/81
2026-04-09 12:08:12         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/82
2026-04-09 12:08:17         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/83
2026-04-09 12:08:22         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/84
2026-04-09 12:08:26         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/85
2026-04-09 12:08:32         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/86
2026-04-09 12:08:37         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/87
2026-04-09 12:08:41         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/88
2026-04-09 12:08:47         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/89
2026-04-09 12:08:52         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/90
2026-04-09 12:08:56         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/91
2026-04-09 12:09:02         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/92
2026-04-09 12:09:09         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/93
2026-04-09 12:09:13         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/94
2026-04-09 12:09:18         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/95
2026-04-09 12:09:24         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/96
2026-04-09 12:09:29         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/97
2026-04-09 12:09:34         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/98
2026-04-09 12:09:41         29 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/commits/99
2026-04-09 12:00:01         45 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/metadata
2026-04-09 12:09:41        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/100
2026-04-09 12:09:46        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/101
2026-04-09 12:09:51        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/102
2026-04-09 12:09:58        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/103
2026-04-09 12:10:03        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/104
2026-04-09 12:10:08        748 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/105
2026-04-09 12:10:14        752 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/106
2026-04-09 12:10:19        752 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/107
2026-04-09 12:10:25        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/108
2026-04-09 12:10:29        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/109
2026-04-09 12:10:33        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/110
2026-04-09 12:10:38        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/111
2026-04-09 12:10:44        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/112
2026-04-09 12:10:48        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/113
2026-04-09 12:10:53        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/114
2026-04-09 12:10:59        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/115
2026-04-09 12:11:03        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/116
2026-04-09 12:11:08        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/117
2026-04-09 12:11:14        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/118
2026-04-09 12:11:18        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/119
2026-04-09 12:11:23        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/120
2026-04-09 12:11:31        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/121
2026-04-09 12:11:35        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/122
2026-04-09 12:11:41        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/123
2026-04-09 12:11:47        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/124
2026-04-09 12:11:51        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/125
2026-04-09 12:11:56        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/126
2026-04-09 12:12:04        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/127
2026-04-09 12:12:09        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/128
2026-04-09 12:12:14        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/129
2026-04-09 12:12:20        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/130
2026-04-09 12:12:24        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/131
2026-04-09 12:12:29        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/132
2026-04-09 12:12:35        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/133
2026-04-09 12:12:40        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/134
2026-04-09 12:12:45        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/135
2026-04-09 12:12:48        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/136
2026-04-09 12:12:53        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/137
2026-04-09 12:12:57        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/138
2026-04-09 12:13:01        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/139
2026-04-09 12:13:06        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/140
2026-04-09 12:13:11        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/141
2026-04-09 12:13:21        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/142
2026-04-09 12:13:30        753 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/143
2026-04-09 12:04:48        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/43
2026-04-09 12:04:52        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/44
2026-04-09 12:04:57        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/45
2026-04-09 12:05:03        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/46
2026-04-09 12:05:08        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/47
2026-04-09 12:05:13        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/48
2026-04-09 12:05:18        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/49
2026-04-09 12:05:22        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/50
2026-04-09 12:05:28        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/51
2026-04-09 12:05:34        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/52
2026-04-09 12:05:38        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/53
2026-04-09 12:05:44        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/54
2026-04-09 12:05:49        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/55
2026-04-09 12:05:53        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/56
2026-04-09 12:05:58        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/57
2026-04-09 12:06:05        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/58
2026-04-09 12:06:10        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/59
2026-04-09 12:06:15        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/60
2026-04-09 12:06:21        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/61
2026-04-09 12:06:26        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/62
2026-04-09 12:06:31        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/63
2026-04-09 12:06:37        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/64
2026-04-09 12:06:42        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/65
2026-04-09 12:06:48        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/66
2026-04-09 12:06:53        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/67
2026-04-09 12:06:58        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/68
2026-04-09 12:07:03        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/69
2026-04-09 12:07:10        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/70
2026-04-09 12:07:15        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/71
2026-04-09 12:07:20        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/72
2026-04-09 12:07:26        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/73
2026-04-09 12:07:30        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/74
2026-04-09 12:07:35        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/75
2026-04-09 12:07:40        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/76
2026-04-09 12:07:44        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/77
2026-04-09 12:07:49        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/78
2026-04-09 12:07:54        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/79
2026-04-09 12:07:58        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/80
2026-04-09 12:08:03        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/81
2026-04-09 12:08:08        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/82
2026-04-09 12:08:12        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/83
2026-04-09 12:08:17        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/84
2026-04-09 12:08:23        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/85
2026-04-09 12:08:26        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/86
2026-04-09 12:08:32        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/87
2026-04-09 12:08:37        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/88
2026-04-09 12:08:42        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/89
2026-04-09 12:08:47        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/90
2026-04-09 12:08:52        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/91
2026-04-09 12:08:57        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/92
2026-04-09 12:09:02        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/93
2026-04-09 12:09:09        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/94
2026-04-09 12:09:13        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/95
2026-04-09 12:09:18        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/96
2026-04-09 12:09:24        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/97
2026-04-09 12:09:29        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/98
2026-04-09 12:09:35        747 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/offsets/99
2026-04-09 12:00:04         94 evaluations/baseline-normal-20260409t120000z/checkpoints/classified_side_effects/sources/0/0
2026-04-09 12:00:25         29 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/0
2026-04-09 12:00:48         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/1
2026-04-09 12:03:29         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/10
2026-04-09 12:03:46         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/11
2026-04-09 12:04:04         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/12
2026-04-09 12:04:21         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/13
2026-04-09 12:04:36         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/14
2026-04-09 12:04:50         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/15
2026-04-09 12:05:06         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/16
2026-04-09 12:05:21         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/17
2026-04-09 12:05:37         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/18
2026-04-09 12:05:52         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/19
2026-04-09 12:01:08         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/2
2026-04-09 12:06:09         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/20
2026-04-09 12:06:25         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/21
2026-04-09 12:06:41         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/22
2026-04-09 12:06:57         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/23
2026-04-09 12:07:14         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/24
2026-04-09 12:07:29         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/25
2026-04-09 12:07:43         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/26
2026-04-09 12:07:57         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/27
2026-04-09 12:08:12         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/28
2026-04-09 12:08:26         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/29
2026-04-09 12:01:26         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/3
2026-04-09 12:08:40         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/30
2026-04-09 12:08:56         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/31
2026-04-09 12:09:12         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/32
2026-04-09 12:09:28         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/33
2026-04-09 12:09:45         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/34
2026-04-09 12:10:02         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/35
2026-04-09 12:10:18         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/36
2026-04-09 12:10:31         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/37
2026-04-09 12:10:47         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/38
2026-04-09 12:11:02         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/39
2026-04-09 12:01:44         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/4
2026-04-09 12:11:17         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/40
2026-04-09 12:11:34         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/41
2026-04-09 12:11:50         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/42
2026-04-09 12:12:06         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/43
2026-04-09 12:12:23         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/44
2026-04-09 12:12:39         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/45
2026-04-09 12:12:46         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/46
2026-04-09 12:12:54         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/47
2026-04-09 12:13:02         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/48
2026-04-09 12:13:11         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/49
2026-04-09 12:02:01         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/5
2026-04-09 12:13:21         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/50
2026-04-09 12:13:30         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/51
2026-04-09 12:13:42         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/52
2026-04-09 12:02:19         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/6
2026-04-09 12:02:35         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/7
2026-04-09 12:02:54         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/8
2026-04-09 12:03:12         41 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/commits/9
2026-04-09 11:59:59         45 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/metadata
2026-04-09 12:00:05        729 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/0
2026-04-09 12:00:26        741 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/1
2026-04-09 12:03:12        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/10
2026-04-09 12:03:30        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/11
2026-04-09 12:03:47        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/12
2026-04-09 12:04:05        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/13
2026-04-09 12:04:22        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/14
2026-04-09 12:04:36        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/15
2026-04-09 12:04:51        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/16
2026-04-09 12:05:06        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/17
2026-04-09 12:05:21        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/18
2026-04-09 12:05:37        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/19
2026-04-09 12:00:49        753 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/2
2026-04-09 12:05:53        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/20
2026-04-09 12:06:09        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/21
2026-04-09 12:06:25        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/22
2026-04-09 12:06:41        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/23
2026-04-09 12:06:58        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/24
2026-04-09 12:07:14        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/25
2026-04-09 12:07:29        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/26
2026-04-09 12:07:43        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/27
2026-04-09 12:07:57        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/28
2026-04-09 12:08:12        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/29
2026-04-09 12:01:08        753 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/3
2026-04-09 12:08:26        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/30
2026-04-09 12:08:41        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/31
2026-04-09 12:08:56        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/32
2026-04-09 12:09:12        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/33
2026-04-09 12:09:29        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/34
2026-04-09 12:09:45        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/35
2026-04-09 12:10:02        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/36
2026-04-09 12:10:19        764 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/37
2026-04-09 12:10:32        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/38
2026-04-09 12:10:47        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/39
2026-04-09 12:01:27        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/4
2026-04-09 12:11:02        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/40
2026-04-09 12:11:17        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/41
2026-04-09 12:11:35        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/42
2026-04-09 12:11:51        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/43
2026-04-09 12:12:07        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/44
2026-04-09 12:12:23        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/45
2026-04-09 12:12:39        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/46
2026-04-09 12:12:47        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/47
2026-04-09 12:12:55        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/48
2026-04-09 12:13:03        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/49
2026-04-09 12:01:44        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/5
2026-04-09 12:13:11        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/50
2026-04-09 12:13:21        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/51
2026-04-09 12:13:31        765 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/52
2026-04-09 12:02:02        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/6
2026-04-09 12:02:19        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/7
2026-04-09 12:02:36        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/8
2026-04-09 12:02:54        759 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/offsets/9
2026-04-09 12:00:04         94 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/sources/0/0
2026-04-09 12:03:08       2736 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.10.delta.152c8477-0bbb-4e13-abd4-436e7ddd212f.TID1590.tmp
2026-04-09 12:02:56       2736 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.10.delta.9864177b-0bb5-4282-b355-455e5e72357a.TID1471.tmp
2026-04-09 12:03:26       2406 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.11.delta.b5840b13-908c-4ff4-8652-0be3882b8024.TID1780.tmp
2026-04-09 12:03:13       2406 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.11.delta.d440f046-54e8-4aad-9e00-5a74992b7922.TID1659.tmp
2026-04-09 12:03:31       2420 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.12.delta.511a4441-7501-4906-bb12-3ade33cdc46b.TID1849.tmp
2026-04-09 12:03:43       2420 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.12.delta.c1f90a18-98d2-4d6f-b137-9f3894a84388.TID1970.tmp
2026-04-09 12:04:01       2634 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.13.delta.65ead018-32ad-414c-ab74-9e81ac4632f2.TID2160.tmp
2026-04-09 12:03:48       2634 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.13.delta.825b5ac7-b90b-4cad-9efe-7738b86c9067.TID2038.tmp
2026-04-09 12:04:06       2713 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.14.delta.344fcbd1-a62d-4c0a-9ece-b0455d1c049a.TID2229.tmp
2026-04-09 12:04:18       2713 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.14.delta.e98da10f-2ad2-44c4-993a-e4b9162472ae.TID2351.tmp
2026-04-09 12:04:33       2352 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.15.delta.09a4e6b7-1978-4dc5-8d14-9ec92fade981.TID2540.tmp
2026-04-09 12:04:23       2352 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.15.delta.3823672f-738f-468f-ba15-8adca3df7fb6.TID2419.tmp
2026-04-09 12:04:37       2284 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.16.delta.96bb48b4-2e47-47b5-8ba4-4435ce24b19f.TID2609.tmp
2026-04-09 12:04:48       2284 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.16.delta.d00b516a-49e4-4f72-bb18-02aa4a7857d2.TID2730.tmp
2026-04-09 12:04:52       2106 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.17.delta.43f73000-54a5-4a71-b574-e18f91aac171.TID2798.tmp
2026-04-09 12:05:03       2106 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.17.delta.70d654eb-86dc-4f81-a54e-967033d613b5.TID2920.tmp
2026-04-09 12:05:07       2235 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.18.delta.1c7fc449-dab8-4a3c-bdfd-7b2b7a2996a6.TID2988.tmp
2026-04-09 12:05:18       2235 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.18.delta.69885ac2-819d-4e20-8838-f71b2da4860f.TID3110.tmp
2026-04-09 12:05:34       2537 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.19.delta.0698585e-f7e7-4ac0-b18d-63ed18ff785a.TID3301.tmp
2026-04-09 12:05:22       2537 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.19.delta.7da8cfc8-ae39-49de-9764-7ac956ef6e8b.TID3178.tmp
2026-04-09 12:00:44       1898 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.2.delta.8ac6ef7c-be10-44a5-84e3-4447ae6d0808.TID179.tmp
2026-04-09 12:00:29       1898 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.2.delta.da7460a3-9e1c-461a-8aa0-fedd2ab8519a.TID104.tmp
2026-04-09 12:05:50       2432 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.20.delta.7af3ae48-e0d4-41d7-98b2-8b091e381061.TID3490.tmp
2026-04-09 12:05:38       2432 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.20.delta.c71ede5c-ede9-4bdc-823d-fb42cc21c509.TID3369.tmp
2026-04-09 12:06:06       2314 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.21.delta.d9036d53-ad54-40bf-965a-2e5dada80d84.TID3680.tmp
2026-04-09 12:05:54       2314 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.21.delta.e43a1b78-c5a9-48ec-81bb-6a735532e3cf.TID3559.tmp
2026-04-09 12:06:22       2416 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.22.delta.4be0c1dd-8fef-4439-8975-f9faaef42c1e.TID3870.tmp
2026-04-09 12:06:10       2416 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.22.delta.d4626558-2f64-4c72-8943-f896abfce6c2.TID3748.tmp
2026-04-09 12:06:38       2252 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.23.delta.1876e4ea-469d-4576-9a7f-4064dc3f2c71.TID4060.tmp
2026-04-09 12:06:26       2252 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.23.delta.21621da6-0666-4d89-8ad4-fdc1765b1038.TID3939.tmp
2026-04-09 12:06:54       2385 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.24.delta.13eaee53-143e-4ed8-a959-2bd33bb4b077.TID4250.tmp
2026-04-09 12:06:42       2385 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.24.delta.b6981c1a-ee15-4304-8e07-dbe664ee9fa7.TID4128.tmp
2026-04-09 12:06:58       2708 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.25.delta.ab28126c-6e96-4f53-8b29-b66e7d2c4714.TID4319.tmp
2026-04-09 12:07:11       2708 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.25.delta.f73ae131-2cab-400d-8c81-5c8990bc457d.TID4440.tmp
2026-04-09 12:07:26       2519 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.26.delta.2e1601a4-9411-42d6-a25b-43c985f90fd2.TID4630.tmp
2026-04-09 12:07:15       2519 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.26.delta.4e1613ef-44e8-4c32-ac61-1b7ed5b7a39d.TID4509.tmp
2026-04-09 12:07:30       2099 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.27.delta.5d4d61ba-4b77-47ea-a212-688dc855d686.TID4698.tmp
2026-04-09 12:07:40       2099 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.27.delta.6e714b94-07c6-40d2-a44d-0b308b8fd9df.TID4820.tmp
2026-04-09 12:07:54       2236 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.28.delta.0ec14ec0-4363-41c6-ab1b-ec9c31c9c6ac.TID5010.tmp
2026-04-09 12:07:44       2236 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.28.delta.29d218e2-e263-4d9e-8d38-bacf98d6c767.TID4888.tmp
2026-04-09 12:07:58       2144 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.29.delta.772e0c28-dfc7-41b8-a440-fb44aa5bb663.TID5078.tmp
2026-04-09 12:08:09       2144 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.29.delta.fbea299f-2bf5-49ad-90c6-eb352b91214e.TID5205.tmp
2026-04-09 12:01:05       3683 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.3.delta.02e69b5f-fc38-4c9c-8cf8-b1ffeda4b0ba.TID364.tmp
2026-04-09 12:00:51       3683 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.3.delta.b7e12c36-894e-4114-98e2-9c6d5c2050a5.TID243.tmp
2026-04-09 12:08:14       2227 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.30.delta.d6fcbf70-ca22-4825-be94-af8c45a07dfa.TID5277.tmp
2026-04-09 12:08:24       2227 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.30.delta.d84c9e49-d815-4cc7-b80d-ed56ce53e839.TID5395.tmp
2026-04-09 12:08:27       2109 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.31.delta.0672e5f6-2885-4bea-bd47-a6161992ad8c.TID5461.tmp
2026-04-09 12:08:38       2109 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.31.delta.dc26cddc-4c87-4711-aec2-85e23d1e9bf5.TID5580.tmp
2026-04-09 12:08:42       2385 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.32.delta.2167f9ae-b494-410a-a60a-96f8adee330f.TID5648.tmp
2026-04-09 12:08:53       2385 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.32.delta.7bf3f6d7-b911-4af4-90ef-2de80b765e9c.TID5770.tmp
2026-04-09 12:09:09       2357 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.33.delta.0ec07389-44d9-495c-a348-2382c41ea7a5.TID5960.tmp
2026-04-09 12:08:57       2357 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.33.delta.3f46148b-f64f-42c6-9f44-c8d5ab839b25.TID5839.tmp
2026-04-09 12:09:25       2264 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.34.delta.2ae17829-eabd-4e1c-891e-a28e01a2259c.TID6150.tmp
2026-04-09 12:09:13       2264 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.34.delta.beb4856a-86ee-4afd-a0c7-ffa481bf3761.TID6028.tmp
2026-04-09 12:09:42       2283 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.35.delta.9a62816e-a45b-45a6-acc2-00d316507361.TID6340.tmp
2026-04-09 12:09:30       2283 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.35.delta.c186455e-34fe-4f44-839d-2782f160b17f.TID6219.tmp
2026-04-09 12:09:47       2653 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.36.delta.282118b5-917c-4b65-99af-31ddb904a71c.TID6409.tmp
2026-04-09 12:09:58       2653 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.36.delta.310b0d02-4b83-4f6b-9fab-1b8e7a4c1606.TID6530.tmp
2026-04-09 12:10:15       2340 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.37.delta.3eb46774-6235-4496-8d05-e63731a2790a.TID6720.tmp
2026-04-09 12:10:04       2340 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.37.delta.a780ee78-2f0c-49d3-975c-0b783980bddd.TID6601.tmp
2026-04-09 12:10:19       2691 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.38.delta.9e6cb931-6d9f-4f50-b798-d3fe7485f18f.TID6788.tmp
2026-04-09 12:10:29       2691 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.38.delta.d172d01b-1701-4afc-9382-3e5097209c7a.TID6904.tmp
2026-04-09 12:10:33       1959 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.39.delta.7371daed-e5c5-4294-96e1-839ed3e5e6d2.TID6978.tmp
2026-04-09 12:10:44       1959 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.39.delta.a637ee23-f546-4cda-aff9-8a0ab1892dfc.TID7100.tmp
2026-04-09 12:01:11       3182 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.4.delta.38a7daf2-4b20-4024-aa04-924e8fbc63cb.TID429.tmp
2026-04-09 12:01:23       3182 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.4.delta.a623cf7b-7e2f-4e9a-8dbe-de4ff574314d.TID554.tmp
2026-04-09 12:10:59       2273 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.40.delta.0c085b37-b839-4d35-b2fe-4c1998d59ab3.TID7290.tmp
2026-04-09 12:10:48       2273 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.40.delta.3afe2e38-fde4-4489-a17c-3b722d74d625.TID7168.tmp
2026-04-09 12:11:14       2206 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.41.delta.6f84fe04-a428-48e8-bfd8-eaf5df7d3bc7.TID7481.tmp
2026-04-09 12:11:03       2206 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.41.delta.bae10730-1bd2-4d79-a1fd-c44dba229558.TID7359.tmp
2026-04-09 12:11:31       2243 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.42.delta.04c389cb-0232-4630-bc17-456fe526c1d5.TID7670.tmp
2026-04-09 12:11:18       2243 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.42.delta.aa797db5-3916-429a-ae55-3a942d738044.TID7549.tmp
2026-04-09 12:11:48       2564 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.43.delta.c0e50efd-c7fe-4722-807c-4d61ad637324.TID7865.tmp
2026-04-09 12:12:04       2353 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.44.delta.2e08ccf9-92a9-4f82-9eb9-356a1b034f3c.TID8050.tmp
2026-04-09 12:11:52       2353 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.44.delta.3bf96d9f-947a-420b-9f5a-8743ee682cab.TID7931.tmp
2026-04-09 12:12:20       2398 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.45.delta.956889a9-3545-475e-b23b-2ea5b9847c98.TID8240.tmp
2026-04-09 12:12:09       2398 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.45.delta.bf2e0710-006b-4f29-8ec3-b4f91b6ebf93.TID8118.tmp
2026-04-09 12:01:29       2848 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.5.delta.2b62fdbc-1976-4c64-bc21-9e3acd848c38.TID622.tmp
2026-04-09 12:01:41       2848 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.5.delta.b4794742-14dc-4dfc-8c7c-272d6e57ccb4.TID721.tmp
2026-04-09 12:01:58       2645 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.6.delta.760726f6-3ddb-4936-a28d-9ae4fd48e9dd.TID882.tmp
2026-04-09 12:01:47       2645 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.6.delta.9c81f5b0-0434-4f31-a287-c0122d4507f5.TID788.tmp
2026-04-09 12:02:15       2842 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.7.delta.9bc0cd31-9f20-449e-ae42-2ce7d6228b93.TID1072.tmp
2026-04-09 12:02:04       2842 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.7.delta.de482d84-a57d-4e7c-b650-dcb9409ba6f0.TID950.tmp
2026-04-09 12:02:32       2406 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.8.delta.7f7d6f5a-f771-4e26-ae97-61ebc9f32f47.TID1222.tmp
2026-04-09 12:02:20       2406 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.8.delta.d853f33c-f589-4d34-8e9d-8044f4a666dd.TID1140.tmp
2026-04-09 12:02:51       2457 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.9.delta.0c46d7c7-5aa2-4181-bd9d-d2e28da6cce6.TID1405.tmp
2026-04-09 12:02:38       2457 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/.9.delta.28a62b2e-90db-4771-acee-afdcbeb83eac.TID1287.tmp
2026-04-09 12:00:22         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/1.delta
2026-04-09 12:03:09       2736 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/10.delta
2026-04-09 12:03:26       2406 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/11.delta
2026-04-09 12:03:44       2420 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/12.delta
2026-04-09 12:04:02       2634 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/13.delta
2026-04-09 12:04:19       2713 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/14.delta
2026-04-09 12:04:13      39827 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/14.snapshot
2026-04-09 12:04:34       2352 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/15.delta
2026-04-09 12:04:48       2284 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/16.delta
2026-04-09 12:05:04       2106 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/17.delta
2026-04-09 12:05:19       2235 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/18.delta
2026-04-09 12:05:35       2537 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/19.delta
2026-04-09 12:00:46       1898 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/2.delta
2026-04-09 12:05:51       2432 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/20.delta
2026-04-09 12:06:07       2314 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/21.delta
2026-04-09 12:06:23       2416 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/22.delta
2026-04-09 12:06:39       2252 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/23.delta
2026-04-09 12:06:55       2385 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/24.delta
2026-04-09 12:07:12       2708 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/25.delta
2026-04-09 12:07:13      70865 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/25.snapshot
2026-04-09 12:07:27       2519 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/26.delta
2026-04-09 12:07:41       2099 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/27.delta
2026-04-09 12:07:55       2236 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/28.delta
2026-04-09 12:08:10       2144 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/29.delta
2026-04-09 12:01:06       3683 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/3.delta
2026-04-09 12:08:24       2227 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/30.delta
2026-04-09 12:08:39       2109 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/31.delta
2026-04-09 12:08:54       2385 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/32.delta
2026-04-09 12:09:10       2357 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/33.delta
2026-04-09 12:09:26       2264 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/34.delta
2026-04-09 12:09:43       2283 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/35.delta
2026-04-09 12:09:59       2653 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/36.delta
2026-04-09 12:10:16       2340 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/37.delta
2026-04-09 12:10:13     104392 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/37.snapshot
2026-04-09 12:10:29       2691 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/38.delta
2026-04-09 12:10:45       1959 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/39.delta
2026-04-09 12:01:24       3182 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/4.delta
2026-04-09 12:11:00       2273 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/40.delta
2026-04-09 12:11:15       2206 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/41.delta
2026-04-09 12:11:32       2243 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/42.delta
2026-04-09 12:11:48       2564 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/43.delta
2026-04-09 12:12:04       2353 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/44.delta
2026-04-09 12:12:21       2398 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/45.delta
2026-04-09 12:12:37        754 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/46.delta
2026-04-09 12:12:44         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/47.delta
2026-04-09 12:12:52         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/48.delta
2026-04-09 12:13:00         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/49.delta
2026-04-09 12:01:42       2848 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/5.delta
2026-04-09 12:13:08         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/50.delta
2026-04-09 12:13:13     128024 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/50.snapshot
2026-04-09 12:13:19         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/51.delta
2026-04-09 12:13:28         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/52.delta
2026-04-09 12:13:40         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/53.delta
2026-04-09 12:01:59       2645 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/6.delta
2026-04-09 12:02:17       2842 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/7.delta
2026-04-09 12:02:34       2406 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/8.delta
2026-04-09 12:02:52       2457 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/9.delta
2026-04-09 12:00:13        203 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/0/_metadata/schema
2026-04-09 12:00:23         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/1.delta
2026-04-09 12:03:10       2663 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/10.delta
2026-04-09 12:03:27       2617 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/11.delta
2026-04-09 12:03:44       2567 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/12.delta
2026-04-09 12:04:02       2549 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/13.delta
2026-04-09 12:04:19       2732 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/14.delta
2026-04-09 12:04:13      39009 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/14.snapshot
2026-04-09 12:04:34       2531 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/15.delta
2026-04-09 12:04:49       2029 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/16.delta
2026-04-09 12:05:04       2289 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/17.delta
2026-04-09 12:05:19       2180 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/18.delta
2026-04-09 12:05:35       2404 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/19.delta
2026-04-09 12:00:46       1842 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/2.delta
2026-04-09 12:05:51       2327 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/20.delta
2026-04-09 12:06:07       2155 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/21.delta
2026-04-09 12:06:23       2292 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/22.delta
2026-04-09 12:06:39       2413 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/23.delta
2026-04-09 12:06:55       2268 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/24.delta
2026-04-09 12:07:12       2413 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/25.delta
2026-04-09 12:07:13      69085 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/25.snapshot
2026-04-09 12:07:27       2359 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/26.delta
2026-04-09 12:07:41       2203 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/27.delta
2026-04-09 12:07:55       2040 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/28.delta
2026-04-09 12:08:10       2146 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/29.delta
2026-04-09 12:01:06       3246 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/3.delta
2026-04-09 12:08:25       2195 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/30.delta
2026-04-09 12:08:39       1989 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/31.delta
2026-04-09 12:08:54       2300 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/32.delta
2026-04-09 12:09:10       2245 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/33.delta
2026-04-09 12:09:26       2284 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/34.delta
2026-04-09 12:09:43       2539 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/35.delta
2026-04-09 12:10:00       2528 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/36.delta
2026-04-09 12:10:16       2500 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/37.delta
2026-04-09 12:10:13     102254 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/37.snapshot
2026-04-09 12:10:29       2374 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/38.delta
2026-04-09 12:10:45       1982 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/39.delta
2026-04-09 12:01:24       2846 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/4.delta
2026-04-09 12:11:00       2202 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/40.delta
2026-04-09 12:11:15       2097 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/41.delta
2026-04-09 12:11:32       2206 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/42.delta
2026-04-09 12:11:49       2610 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/43.delta
2026-04-09 12:12:04       2448 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/44.delta
2026-04-09 12:12:21       2544 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/45.delta
2026-04-09 12:12:37        928 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/46.delta
2026-04-09 12:12:45         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/47.delta
2026-04-09 12:12:53         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/48.delta
2026-04-09 12:13:01         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/49.delta
2026-04-09 12:01:42       2753 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/5.delta
2026-04-09 12:13:09         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/50.delta
2026-04-09 12:13:14     125465 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/50.snapshot
2026-04-09 12:13:20         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/51.delta
2026-04-09 12:13:29         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/52.delta
2026-04-09 12:13:41         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/53.delta
2026-04-09 12:02:00       2592 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/6.delta
2026-04-09 12:02:17       2956 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/7.delta
2026-04-09 12:02:34       2495 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/8.delta
2026-04-09 12:02:52       2328 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/1/9.delta
2026-04-09 12:00:24         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/1.delta
2026-04-09 12:03:10       2940 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/10.delta
2026-04-09 12:03:27       2647 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/11.delta
2026-04-09 12:03:45       2418 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/12.delta
2026-04-09 12:04:03       2778 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/13.delta
2026-04-09 12:04:20       2569 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/14.delta
2026-04-09 12:04:13      39425 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/14.snapshot
2026-04-09 12:04:35       2487 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/15.delta
2026-04-09 12:04:49       2036 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/16.delta
2026-04-09 12:05:04       2125 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/17.delta
2026-04-09 12:05:20       2284 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/18.delta
2026-04-09 12:05:35       2356 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/19.delta
2026-04-09 12:00:46       2021 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/2.delta
2026-04-09 12:05:51       2378 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/20.delta
2026-04-09 12:06:07       2297 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/21.delta
2026-04-09 12:06:24       2224 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/22.delta
2026-04-09 12:06:39       2636 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/23.delta
2026-04-09 12:06:55       2468 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/24.delta
2026-04-09 12:07:13       2511 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/25.delta
2026-04-09 12:07:14      70228 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/25.snapshot
2026-04-09 12:07:28       2444 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/26.delta
2026-04-09 12:07:42       2255 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/27.delta
2026-04-09 12:07:56       2017 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/28.delta
2026-04-09 12:08:10       2147 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/29.delta
2026-04-09 12:01:06       3424 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/3.delta
2026-04-09 12:08:25       2187 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/30.delta
2026-04-09 12:08:39       2091 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/31.delta
2026-04-09 12:08:55       2116 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/32.delta
2026-04-09 12:09:10       2376 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/33.delta
2026-04-09 12:09:26       2466 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/34.delta
2026-04-09 12:09:43       2493 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/35.delta
2026-04-09 12:10:00       2374 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/36.delta
2026-04-09 12:10:16       2456 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/37.delta
2026-04-09 12:10:14     103354 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/37.snapshot
2026-04-09 12:10:30       2533 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/38.delta
2026-04-09 12:10:45       2073 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/39.delta
2026-04-09 12:01:25       2924 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/4.delta
2026-04-09 12:11:01       2150 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/40.delta
2026-04-09 12:11:16       2374 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/41.delta
2026-04-09 12:11:33       2234 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/42.delta
2026-04-09 12:11:49       2414 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/43.delta
2026-04-09 12:12:05       2429 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/44.delta
2026-04-09 12:12:22       2284 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/45.delta
2026-04-09 12:12:38        879 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/46.delta
2026-04-09 12:12:45         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/47.delta
2026-04-09 12:12:53         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/48.delta
2026-04-09 12:13:01         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/49.delta
2026-04-09 12:01:43       2712 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/5.delta
2026-04-09 12:13:09         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/50.delta
2026-04-09 12:13:14     126838 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/50.snapshot
2026-04-09 12:13:20         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/51.delta
2026-04-09 12:13:29         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/52.delta
2026-04-09 12:13:41         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/53.delta
2026-04-09 12:02:00       2461 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/6.delta
2026-04-09 12:02:17       2580 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/7.delta
2026-04-09 12:02:34       2515 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/8.delta
2026-04-09 12:02:52       2462 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/2/9.delta
2026-04-09 12:00:24         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/1.delta
2026-04-09 12:03:10       2559 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/10.delta
2026-04-09 12:03:28       2705 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/11.delta
2026-04-09 12:03:45       2601 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/12.delta
2026-04-09 12:04:03       2707 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/13.delta
2026-04-09 12:04:20       2589 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/14.delta
2026-04-09 12:04:12      39206 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/14.snapshot
2026-04-09 12:04:35       2477 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/15.delta
2026-04-09 12:04:49       2067 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/16.delta
2026-04-09 12:05:04       2224 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/17.delta
2026-04-09 12:05:20       2316 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/18.delta
2026-04-09 12:05:36       2393 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/19.delta
2026-04-09 12:00:46       1849 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/2.delta
2026-04-09 12:05:52       2321 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/20.delta
2026-04-09 12:06:08       2244 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/21.delta
2026-04-09 12:06:24       2532 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/22.delta
2026-04-09 12:06:40       2316 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/23.delta
2026-04-09 12:06:56       2283 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/24.delta
2026-04-09 12:07:13       2574 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/25.delta
2026-04-09 12:07:12      70101 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/25.snapshot
2026-04-09 12:07:28       2532 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/26.delta
2026-04-09 12:07:42       2222 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/27.delta
2026-04-09 12:07:56       1960 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/28.delta
2026-04-09 12:08:11       1923 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/29.delta
2026-04-09 12:01:06       3483 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/3.delta
2026-04-09 12:08:25       2074 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/30.delta
2026-04-09 12:08:40       2228 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/31.delta
2026-04-09 12:08:55       2333 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/32.delta
2026-04-09 12:09:11       2146 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/33.delta
2026-04-09 12:09:27       2256 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/34.delta
2026-04-09 12:09:44       2293 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/35.delta
2026-04-09 12:10:01       2371 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/36.delta
2026-04-09 12:10:17       2479 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/37.delta
2026-04-09 12:10:12     102536 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/37.snapshot
2026-04-09 12:10:30       2615 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/38.delta
2026-04-09 12:10:46       1831 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/39.delta
2026-04-09 12:01:25       2861 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/4.delta
2026-04-09 12:11:01       2252 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/40.delta
2026-04-09 12:11:16       2275 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/41.delta
2026-04-09 12:11:33       2219 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/42.delta
2026-04-09 12:11:49       2604 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/43.delta
2026-04-09 12:12:05       2306 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/44.delta
2026-04-09 12:12:22       2417 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/45.delta
2026-04-09 12:12:38        847 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/46.delta
2026-04-09 12:12:46         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/47.delta
2026-04-09 12:12:54         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/48.delta
2026-04-09 12:13:02         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/49.delta
2026-04-09 12:01:43       2566 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/5.delta
2026-04-09 12:13:10         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/50.delta
2026-04-09 12:13:12     125815 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/50.snapshot
2026-04-09 12:13:20         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/51.delta
2026-04-09 12:13:30         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/52.delta
2026-04-09 12:13:41         46 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/53.delta
2026-04-09 12:02:00       2555 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/6.delta
2026-04-09 12:02:18       2628 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/7.delta
2026-04-09 12:02:35       2768 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/8.delta
2026-04-09 12:02:53       2525 evaluations/baseline-normal-20260409t120000z/checkpoints/processed_side_effects/state/0/3/9.delta
2026-04-09 12:13:37          0 evaluations/baseline-normal-20260409t120000z/invalid/_SUCCESS
2026-04-09 12:13:23      98545 evaluations/baseline-normal-20260409t120000z/invalid/part-00000-573b5fcc-3045-4998-88e9-06da072a4e81-c000.json
2026-04-09 12:13:35      41085 evaluations/baseline-normal-20260409t120000z/invalid/part-00000-8cc1bf2e-cb9c-4bde-9261-b348df6b0121-c000.json
2026-04-09 12:13:15      12395 evaluations/baseline-normal-20260409t120000z/invalid/part-00000-d59a6f84-2dbc-44c8-9a51-b55be5437388-c000.json
2026-04-09 12:13:24     118129 evaluations/baseline-normal-20260409t120000z/invalid/part-00001-573b5fcc-3045-4998-88e9-06da072a4e81-c000.json
2026-04-09 12:13:35      35250 evaluations/baseline-normal-20260409t120000z/invalid/part-00001-8cc1bf2e-cb9c-4bde-9261-b348df6b0121-c000.json
2026-04-09 12:13:15       7832 evaluations/baseline-normal-20260409t120000z/invalid/part-00001-d59a6f84-2dbc-44c8-9a51-b55be5437388-c000.json
2026-04-09 12:13:24     112919 evaluations/baseline-normal-20260409t120000z/invalid/part-00002-573b5fcc-3045-4998-88e9-06da072a4e81-c000.json
2026-04-09 12:13:36      31306 evaluations/baseline-normal-20260409t120000z/invalid/part-00002-8cc1bf2e-cb9c-4bde-9261-b348df6b0121-c000.json
2026-04-09 12:13:16       6518 evaluations/baseline-normal-20260409t120000z/invalid/part-00002-d59a6f84-2dbc-44c8-9a51-b55be5437388-c000.json
2026-04-09 12:13:24      98551 evaluations/baseline-normal-20260409t120000z/invalid/part-00003-573b5fcc-3045-4998-88e9-06da072a4e81-c000.json
2026-04-09 12:13:36      33279 evaluations/baseline-normal-20260409t120000z/invalid/part-00003-8cc1bf2e-cb9c-4bde-9261-b348df6b0121-c000.json
2026-04-09 12:13:16       8483 evaluations/baseline-normal-20260409t120000z/invalid/part-00003-d59a6f84-2dbc-44c8-9a51-b55be5437388-c000.json
2026-04-09 12:13:25     102435 evaluations/baseline-normal-20260409t120000z/invalid/part-00004-573b5fcc-3045-4998-88e9-06da072a4e81-c000.json
2026-04-09 12:13:36      48280 evaluations/baseline-normal-20260409t120000z/invalid/part-00004-8cc1bf2e-cb9c-4bde-9261-b348df6b0121-c000.json
2026-04-09 12:13:16      11746 evaluations/baseline-normal-20260409t120000z/invalid/part-00004-d59a6f84-2dbc-44c8-9a51-b55be5437388-c000.json
2026-04-09 12:13:25     112881 evaluations/baseline-normal-20260409t120000z/invalid/part-00005-573b5fcc-3045-4998-88e9-06da072a4e81-c000.json
2026-04-09 12:13:37      46986 evaluations/baseline-normal-20260409t120000z/invalid/part-00005-8cc1bf2e-cb9c-4bde-9261-b348df6b0121-c000.json
2026-04-09 12:13:17      13048 evaluations/baseline-normal-20260409t120000z/invalid/part-00005-d59a6f84-2dbc-44c8-9a51-b55be5437388-c000.json
2026-04-09 12:12:29          0 evaluations/baseline-normal-20260409t120000z/processed/_SUCCESS
2026-04-09 12:07:18      33517 evaluations/baseline-normal-20260409t120000z/processed/part-00000-045195bb-8bcf-446b-abeb-6b733d2a7917-c000.snappy.parquet
2026-04-09 12:01:50      34452 evaluations/baseline-normal-20260409t120000z/processed/part-00000-06ace898-ed22-49d2-ab28-40ab8aa7dc47-c000.snappy.parquet
2026-04-09 12:09:49      34701 evaluations/baseline-normal-20260409t120000z/processed/part-00000-07721e71-bcb6-4882-aaa8-5087fb3de5ba-c000.snappy.parquet
2026-04-09 12:00:55      43515 evaluations/baseline-normal-20260409t120000z/processed/part-00000-07f47a3e-dc41-4ef1-9903-965147a470be-c000.snappy.parquet
2026-04-09 12:12:27      16493 evaluations/baseline-normal-20260409t120000z/processed/part-00000-0ce37e46-2d61-42ca-b61b-0eb1e4d38e0a-c000.snappy.parquet
2026-04-09 12:05:25      32215 evaluations/baseline-normal-20260409t120000z/processed/part-00000-16550c37-a61b-4b31-b742-ea03161c303e-c000.snappy.parquet
2026-04-09 12:02:07      34801 evaluations/baseline-normal-20260409t120000z/processed/part-00000-1e8dc07c-75f7-43d9-8c4e-0f40711c48d4-c000.snappy.parquet
2026-04-09 12:05:57      31476 evaluations/baseline-normal-20260409t120000z/processed/part-00000-27a389ec-475e-416d-9c06-b164dcffbfbc-c000.snappy.parquet
2026-04-09 12:03:34      32622 evaluations/baseline-normal-20260409t120000z/processed/part-00000-2e0668e1-49a9-4337-984c-d2625f241282-c000.snappy.parquet
2026-04-09 12:02:23      32993 evaluations/baseline-normal-20260409t120000z/processed/part-00000-3643b0ee-a04f-4cba-b1e3-2c8a3a76060c-c000.snappy.parquet
2026-04-09 12:07:47      30718 evaluations/baseline-normal-20260409t120000z/processed/part-00000-3ee787d7-a9e4-4e2d-96d3-7627b0702ecc-c000.snappy.parquet
2026-04-09 12:10:07      32138 evaluations/baseline-normal-20260409t120000z/processed/part-00000-4ae8ae58-8be7-4794-846d-68a815ac3214-c000.snappy.parquet
2026-04-09 12:11:55      32138 evaluations/baseline-normal-20260409t120000z/processed/part-00000-52050ca6-21d3-4e88-ba0f-e60c89496fe3-c000.snappy.parquet
2026-04-09 12:02:40      32569 evaluations/baseline-normal-20260409t120000z/processed/part-00000-554540e1-990b-4936-a702-8d33c0b00f85-c000.snappy.parquet
2026-04-09 12:11:21      31169 evaluations/baseline-normal-20260409t120000z/processed/part-00000-57b32b79-7bee-45ca-b010-98da4ffc3b8e-c000.snappy.parquet
2026-04-09 12:07:33      29250 evaluations/baseline-normal-20260409t120000z/processed/part-00000-58b0dc1f-725e-466b-8295-c04ed2eea918-c000.snappy.parquet
2026-04-09 12:01:14      39316 evaluations/baseline-normal-20260409t120000z/processed/part-00000-62705bc9-8e18-4dc4-a21d-af195066b9ff-c000.snappy.parquet
2026-04-09 12:05:41      32410 evaluations/baseline-normal-20260409t120000z/processed/part-00000-63c34683-b179-4ae3-978f-f311f1e623b1-c000.snappy.parquet
2026-04-09 12:11:39      33837 evaluations/baseline-normal-20260409t120000z/processed/part-00000-64f4c465-6c2c-43c4-8d6c-916e848b00e7-c000.snappy.parquet
2026-04-09 12:06:13      32598 evaluations/baseline-normal-20260409t120000z/processed/part-00000-6647e812-c851-4cdc-866d-60c305456c33-c000.snappy.parquet
2026-04-09 12:00:34      27695 evaluations/baseline-normal-20260409t120000z/processed/part-00000-67c5799d-40e6-4d96-8556-3b759c0feeaf-c000.snappy.parquet
2026-04-09 12:04:54      29372 evaluations/baseline-normal-20260409t120000z/processed/part-00000-6837a47e-63ba-4376-880b-ad45ff522f06-c000.snappy.parquet
2026-04-09 12:08:30      29434 evaluations/baseline-normal-20260409t120000z/processed/part-00000-688321f5-d6d3-4d80-8ad1-96abe917606d-c000.snappy.parquet
2026-04-09 12:03:16      32609 evaluations/baseline-normal-20260409t120000z/processed/part-00000-6fcd7015-63af-4bc1-8bfd-0f3d2cad6f62-c000.snappy.parquet
2026-04-09 12:02:59      35819 evaluations/baseline-normal-20260409t120000z/processed/part-00000-70c410a4-d54d-4a78-a181-e69bd5e97698-c000.snappy.parquet
2026-04-09 12:09:33      31587 evaluations/baseline-normal-20260409t120000z/processed/part-00000-765543d9-de54-4826-b2ec-6a3c0cbc50be-c000.snappy.parquet
2026-04-09 12:04:40      31049 evaluations/baseline-normal-20260409t120000z/processed/part-00000-766b2c6d-1cdc-4a69-993e-4b1b919dbba1-c000.snappy.parquet
2026-04-09 12:07:01      33810 evaluations/baseline-normal-20260409t120000z/processed/part-00000-789a7ba7-fc49-45a8-9221-3fcab2598759-c000.snappy.parquet
2026-04-09 12:01:32      36572 evaluations/baseline-normal-20260409t120000z/processed/part-00000-79b227e5-733c-4242-9f3c-02fa284b671d-c000.snappy.parquet
2026-04-09 12:06:45      32214 evaluations/baseline-normal-20260409t120000z/processed/part-00000-7b8ede77-0e4c-4426-86fb-d58c90d4d639-c000.snappy.parquet
2026-04-09 12:10:36      28035 evaluations/baseline-normal-20260409t120000z/processed/part-00000-8321d437-b70e-4a91-ab85-36053574c9da-c000.snappy.parquet
2026-04-09 12:08:16      30651 evaluations/baseline-normal-20260409t120000z/processed/part-00000-87b1b8d3-48fe-431c-aec9-535def31d4db-c000.snappy.parquet
2026-04-09 12:12:11      31916 evaluations/baseline-normal-20260409t120000z/processed/part-00000-8edfe7c8-bc54-4bae-bee1-297a651341a7-c000.snappy.parquet
2026-04-09 12:09:16      31198 evaluations/baseline-normal-20260409t120000z/processed/part-00000-8f3f6585-d391-4216-9c5f-083160ce8b72-c000.snappy.parquet
2026-04-09 12:11:06      30446 evaluations/baseline-normal-20260409t120000z/processed/part-00000-91aa9652-9a84-4bde-ab01-ea9465d84d8a-c000.snappy.parquet
2026-04-09 12:05:10      30883 evaluations/baseline-normal-20260409t120000z/processed/part-00000-9762b5d8-b234-4fe7-a53a-47a7e41f926f-c000.snappy.parquet
2026-04-09 12:08:01      29556 evaluations/baseline-normal-20260409t120000z/processed/part-00000-9e3969c2-cfc0-4b7f-b82c-0face9ebc36b-c000.snappy.parquet
2026-04-09 12:08:59      31908 evaluations/baseline-normal-20260409t120000z/processed/part-00000-d007f0f4-ddb6-4824-bcb2-456425530b5c-c000.snappy.parquet
2026-04-09 12:04:25      31925 evaluations/baseline-normal-20260409t120000z/processed/part-00000-e37c9ed1-1918-40eb-a765-25b135b1cb12-c000.snappy.parquet
2026-04-09 12:10:51      31332 evaluations/baseline-normal-20260409t120000z/processed/part-00000-edef5587-835b-438f-bfd5-b0f388efee18-c000.snappy.parquet
2026-04-09 12:10:22      33683 evaluations/baseline-normal-20260409t120000z/processed/part-00000-ef5f80e3-2d45-4eae-a556-5fbf6dac02a8-c000.snappy.parquet
2026-04-09 12:04:09      35572 evaluations/baseline-normal-20260409t120000z/processed/part-00000-f64d5155-6c26-4126-a5ba-64571dc73224-c000.snappy.parquet
2026-04-09 12:03:51      33574 evaluations/baseline-normal-20260409t120000z/processed/part-00000-f7a9ca78-445e-46fe-9c86-ee708616f2ef-c000.snappy.parquet
2026-04-09 12:06:29      30780 evaluations/baseline-normal-20260409t120000z/processed/part-00000-faf2e080-3ffb-49e8-b44d-49d1ffffee34-c000.snappy.parquet
2026-04-09 12:08:45      30824 evaluations/baseline-normal-20260409t120000z/processed/part-00000-fef4b94f-c96c-44d2-bf01-31b9b8572b37-c000.snappy.parquet
2026-04-09 12:07:19      31959 evaluations/baseline-normal-20260409t120000z/processed/part-00001-045195bb-8bcf-446b-abeb-6b733d2a7917-c000.snappy.parquet
2026-04-09 12:01:50      34190 evaluations/baseline-normal-20260409t120000z/processed/part-00001-06ace898-ed22-49d2-ab28-40ab8aa7dc47-c000.snappy.parquet
2026-04-09 12:09:50      33364 evaluations/baseline-normal-20260409t120000z/processed/part-00001-07721e71-bcb6-4882-aaa8-5087fb3de5ba-c000.snappy.parquet
2026-04-09 12:00:55      39846 evaluations/baseline-normal-20260409t120000z/processed/part-00001-07f47a3e-dc41-4ef1-9903-965147a470be-c000.snappy.parquet
2026-04-09 12:12:28      17976 evaluations/baseline-normal-20260409t120000z/processed/part-00001-0ce37e46-2d61-42ca-b61b-0eb1e4d38e0a-c000.snappy.parquet
2026-04-09 12:05:26      30774 evaluations/baseline-normal-20260409t120000z/processed/part-00001-16550c37-a61b-4b31-b742-ea03161c303e-c000.snappy.parquet
2026-04-09 12:02:07      35796 evaluations/baseline-normal-20260409t120000z/processed/part-00001-1e8dc07c-75f7-43d9-8c4e-0f40711c48d4-c000.snappy.parquet
2026-04-09 12:05:57      30003 evaluations/baseline-normal-20260409t120000z/processed/part-00001-27a389ec-475e-416d-9c06-b164dcffbfbc-c000.snappy.parquet
2026-04-09 12:03:34      34111 evaluations/baseline-normal-20260409t120000z/processed/part-00001-2e0668e1-49a9-4337-984c-d2625f241282-c000.snappy.parquet
2026-04-09 12:02:24      33485 evaluations/baseline-normal-20260409t120000z/processed/part-00001-3643b0ee-a04f-4cba-b1e3-2c8a3a76060c-c000.snappy.parquet
2026-04-09 12:07:47      28759 evaluations/baseline-normal-20260409t120000z/processed/part-00001-3ee787d7-a9e4-4e2d-96d3-7627b0702ecc-c000.snappy.parquet
2026-04-09 12:10:07      33665 evaluations/baseline-normal-20260409t120000z/processed/part-00001-4ae8ae58-8be7-4794-846d-68a815ac3214-c000.snappy.parquet
2026-04-09 12:11:55      33145 evaluations/baseline-normal-20260409t120000z/processed/part-00001-52050ca6-21d3-4e88-ba0f-e60c89496fe3-c000.snappy.parquet
2026-04-09 12:02:41      31466 evaluations/baseline-normal-20260409t120000z/processed/part-00001-554540e1-990b-4936-a702-8d33c0b00f85-c000.snappy.parquet
2026-04-09 12:11:22      30802 evaluations/baseline-normal-20260409t120000z/processed/part-00001-57b32b79-7bee-45ca-b010-98da4ffc3b8e-c000.snappy.parquet
2026-04-09 12:07:33      30248 evaluations/baseline-normal-20260409t120000z/processed/part-00001-58b0dc1f-725e-466b-8295-c04ed2eea918-c000.snappy.parquet
2026-04-09 12:01:14      36581 evaluations/baseline-normal-20260409t120000z/processed/part-00001-62705bc9-8e18-4dc4-a21d-af195066b9ff-c000.snappy.parquet
2026-04-09 12:05:41      31551 evaluations/baseline-normal-20260409t120000z/processed/part-00001-63c34683-b179-4ae3-978f-f311f1e623b1-c000.snappy.parquet
2026-04-09 12:11:39      34466 evaluations/baseline-normal-20260409t120000z/processed/part-00001-64f4c465-6c2c-43c4-8d6c-916e848b00e7-c000.snappy.parquet
2026-04-09 12:06:14      31294 evaluations/baseline-normal-20260409t120000z/processed/part-00001-6647e812-c851-4cdc-866d-60c305456c33-c000.snappy.parquet
2026-04-09 12:00:34      26837 evaluations/baseline-normal-20260409t120000z/processed/part-00001-67c5799d-40e6-4d96-8556-3b759c0feeaf-c000.snappy.parquet
2026-04-09 12:04:55      31275 evaluations/baseline-normal-20260409t120000z/processed/part-00001-6837a47e-63ba-4376-880b-ad45ff522f06-c000.snappy.parquet
2026-04-09 12:08:30      28573 evaluations/baseline-normal-20260409t120000z/processed/part-00001-688321f5-d6d3-4d80-8ad1-96abe917606d-c000.snappy.parquet
2026-04-09 12:03:17      34521 evaluations/baseline-normal-20260409t120000z/processed/part-00001-6fcd7015-63af-4bc1-8bfd-0f3d2cad6f62-c000.snappy.parquet
2026-04-09 12:02:59      34795 evaluations/baseline-normal-20260409t120000z/processed/part-00001-70c410a4-d54d-4a78-a181-e69bd5e97698-c000.snappy.parquet
2026-04-09 12:09:33      33668 evaluations/baseline-normal-20260409t120000z/processed/part-00001-765543d9-de54-4826-b2ec-6a3c0cbc50be-c000.snappy.parquet
2026-04-09 12:04:40      28750 evaluations/baseline-normal-20260409t120000z/processed/part-00001-766b2c6d-1cdc-4a69-993e-4b1b919dbba1-c000.snappy.parquet
2026-04-09 12:07:02      31670 evaluations/baseline-normal-20260409t120000z/processed/part-00001-789a7ba7-fc49-45a8-9221-3fcab2598759-c000.snappy.parquet
2026-04-09 12:01:32      35494 evaluations/baseline-normal-20260409t120000z/processed/part-00001-79b227e5-733c-4242-9f3c-02fa284b671d-c000.snappy.parquet
2026-04-09 12:06:45      31131 evaluations/baseline-normal-20260409t120000z/processed/part-00001-7b8ede77-0e4c-4426-86fb-d58c90d4d639-c000.snappy.parquet
2026-04-09 12:10:36      28111 evaluations/baseline-normal-20260409t120000z/processed/part-00001-8321d437-b70e-4a91-ab85-36053574c9da-c000.snappy.parquet
2026-04-09 12:08:16      30345 evaluations/baseline-normal-20260409t120000z/processed/part-00001-87b1b8d3-48fe-431c-aec9-535def31d4db-c000.snappy.parquet
2026-04-09 12:12:12      33198 evaluations/baseline-normal-20260409t120000z/processed/part-00001-8edfe7c8-bc54-4bae-bee1-297a651341a7-c000.snappy.parquet
2026-04-09 12:09:16      31205 evaluations/baseline-normal-20260409t120000z/processed/part-00001-8f3f6585-d391-4216-9c5f-083160ce8b72-c000.snappy.parquet
2026-04-09 12:11:07      29452 evaluations/baseline-normal-20260409t120000z/processed/part-00001-91aa9652-9a84-4bde-ab01-ea9465d84d8a-c000.snappy.parquet
2026-04-09 12:05:10      30336 evaluations/baseline-normal-20260409t120000z/processed/part-00001-9762b5d8-b234-4fe7-a53a-47a7e41f926f-c000.snappy.parquet
2026-04-09 12:08:01      29580 evaluations/baseline-normal-20260409t120000z/processed/part-00001-9e3969c2-cfc0-4b7f-b82c-0face9ebc36b-c000.snappy.parquet
2026-04-09 12:09:00      30911 evaluations/baseline-normal-20260409t120000z/processed/part-00001-d007f0f4-ddb6-4824-bcb2-456425530b5c-c000.snappy.parquet
2026-04-09 12:04:26      33629 evaluations/baseline-normal-20260409t120000z/processed/part-00001-e37c9ed1-1918-40eb-a765-25b135b1cb12-c000.snappy.parquet
2026-04-09 12:10:51      30511 evaluations/baseline-normal-20260409t120000z/processed/part-00001-edef5587-835b-438f-bfd5-b0f388efee18-c000.snappy.parquet
2026-04-09 12:10:22      31241 evaluations/baseline-normal-20260409t120000z/processed/part-00001-ef5f80e3-2d45-4eae-a556-5fbf6dac02a8-c000.snappy.parquet
2026-04-09 12:04:10      35373 evaluations/baseline-normal-20260409t120000z/processed/part-00001-f64d5155-6c26-4126-a5ba-64571dc73224-c000.snappy.parquet
2026-04-09 12:03:51      32415 evaluations/baseline-normal-20260409t120000z/processed/part-00001-f7a9ca78-445e-46fe-9c86-ee708616f2ef-c000.snappy.parquet
2026-04-09 12:06:30      32381 evaluations/baseline-normal-20260409t120000z/processed/part-00001-faf2e080-3ffb-49e8-b44d-49d1ffffee34-c000.snappy.parquet
2026-04-09 12:08:45      29523 evaluations/baseline-normal-20260409t120000z/processed/part-00001-fef4b94f-c96c-44d2-bf01-31b9b8572b37-c000.snappy.parquet
2026-04-09 12:07:19      32889 evaluations/baseline-normal-20260409t120000z/processed/part-00002-045195bb-8bcf-446b-abeb-6b733d2a7917-c000.snappy.parquet
2026-04-09 12:01:50      32945 evaluations/baseline-normal-20260409t120000z/processed/part-00002-06ace898-ed22-49d2-ab28-40ab8aa7dc47-c000.snappy.parquet
2026-04-09 12:09:50      31835 evaluations/baseline-normal-20260409t120000z/processed/part-00002-07721e71-bcb6-4882-aaa8-5087fb3de5ba-c000.snappy.parquet
2026-04-09 12:00:56      41512 evaluations/baseline-normal-20260409t120000z/processed/part-00002-07f47a3e-dc41-4ef1-9903-965147a470be-c000.snappy.parquet
2026-04-09 12:12:28      17540 evaluations/baseline-normal-20260409t120000z/processed/part-00002-0ce37e46-2d61-42ca-b61b-0eb1e4d38e0a-c000.snappy.parquet
2026-04-09 12:05:26      30427 evaluations/baseline-normal-20260409t120000z/processed/part-00002-16550c37-a61b-4b31-b742-ea03161c303e-c000.snappy.parquet
2026-04-09 12:02:07      32668 evaluations/baseline-normal-20260409t120000z/processed/part-00002-1e8dc07c-75f7-43d9-8c4e-0f40711c48d4-c000.snappy.parquet
2026-04-09 12:05:57      31583 evaluations/baseline-normal-20260409t120000z/processed/part-00002-27a389ec-475e-416d-9c06-b164dcffbfbc-c000.snappy.parquet
2026-04-09 12:03:35      32655 evaluations/baseline-normal-20260409t120000z/processed/part-00002-2e0668e1-49a9-4337-984c-d2625f241282-c000.snappy.parquet
2026-04-09 12:02:24      33440 evaluations/baseline-normal-20260409t120000z/processed/part-00002-3643b0ee-a04f-4cba-b1e3-2c8a3a76060c-c000.snappy.parquet
2026-04-09 12:07:48      28897 evaluations/baseline-normal-20260409t120000z/processed/part-00002-3ee787d7-a9e4-4e2d-96d3-7627b0702ecc-c000.snappy.parquet
2026-04-09 12:10:07      33115 evaluations/baseline-normal-20260409t120000z/processed/part-00002-4ae8ae58-8be7-4794-846d-68a815ac3214-c000.snappy.parquet
2026-04-09 12:11:55      32944 evaluations/baseline-normal-20260409t120000z/processed/part-00002-52050ca6-21d3-4e88-ba0f-e60c89496fe3-c000.snappy.parquet
2026-04-09 12:02:41      32594 evaluations/baseline-normal-20260409t120000z/processed/part-00002-554540e1-990b-4936-a702-8d33c0b00f85-c000.snappy.parquet
2026-04-09 12:11:22      30795 evaluations/baseline-normal-20260409t120000z/processed/part-00002-57b32b79-7bee-45ca-b010-98da4ffc3b8e-c000.snappy.parquet
2026-04-09 12:07:34      30719 evaluations/baseline-normal-20260409t120000z/processed/part-00002-58b0dc1f-725e-466b-8295-c04ed2eea918-c000.snappy.parquet
2026-04-09 12:01:14      37467 evaluations/baseline-normal-20260409t120000z/processed/part-00002-62705bc9-8e18-4dc4-a21d-af195066b9ff-c000.snappy.parquet
2026-04-09 12:05:42      32279 evaluations/baseline-normal-20260409t120000z/processed/part-00002-63c34683-b179-4ae3-978f-f311f1e623b1-c000.snappy.parquet
2026-04-09 12:11:40      32488 evaluations/baseline-normal-20260409t120000z/processed/part-00002-64f4c465-6c2c-43c4-8d6c-916e848b00e7-c000.snappy.parquet
2026-04-09 12:06:14      30659 evaluations/baseline-normal-20260409t120000z/processed/part-00002-6647e812-c851-4cdc-866d-60c305456c33-c000.snappy.parquet
2026-04-09 12:00:35      28538 evaluations/baseline-normal-20260409t120000z/processed/part-00002-67c5799d-40e6-4d96-8556-3b759c0feeaf-c000.snappy.parquet
2026-04-09 12:04:55      29394 evaluations/baseline-normal-20260409t120000z/processed/part-00002-6837a47e-63ba-4376-880b-ad45ff522f06-c000.snappy.parquet
2026-04-09 12:08:31      29270 evaluations/baseline-normal-20260409t120000z/processed/part-00002-688321f5-d6d3-4d80-8ad1-96abe917606d-c000.snappy.parquet
2026-04-09 12:03:17      34790 evaluations/baseline-normal-20260409t120000z/processed/part-00002-6fcd7015-63af-4bc1-8bfd-0f3d2cad6f62-c000.snappy.parquet
2026-04-09 12:02:59      37312 evaluations/baseline-normal-20260409t120000z/processed/part-00002-70c410a4-d54d-4a78-a181-e69bd5e97698-c000.snappy.parquet
2026-04-09 12:09:34      33459 evaluations/baseline-normal-20260409t120000z/processed/part-00002-765543d9-de54-4826-b2ec-6a3c0cbc50be-c000.snappy.parquet
2026-04-09 12:04:41      28975 evaluations/baseline-normal-20260409t120000z/processed/part-00002-766b2c6d-1cdc-4a69-993e-4b1b919dbba1-c000.snappy.parquet
2026-04-09 12:07:02      31894 evaluations/baseline-normal-20260409t120000z/processed/part-00002-789a7ba7-fc49-45a8-9221-3fcab2598759-c000.snappy.parquet
2026-04-09 12:01:33      35193 evaluations/baseline-normal-20260409t120000z/processed/part-00002-79b227e5-733c-4242-9f3c-02fa284b671d-c000.snappy.parquet
2026-04-09 12:06:46      32726 evaluations/baseline-normal-20260409t120000z/processed/part-00002-7b8ede77-0e4c-4426-86fb-d58c90d4d639-c000.snappy.parquet
2026-04-09 12:10:37      29265 evaluations/baseline-normal-20260409t120000z/processed/part-00002-8321d437-b70e-4a91-ab85-36053574c9da-c000.snappy.parquet
2026-04-09 12:08:17      30571 evaluations/baseline-normal-20260409t120000z/processed/part-00002-87b1b8d3-48fe-431c-aec9-535def31d4db-c000.snappy.parquet
2026-04-09 12:12:12      30749 evaluations/baseline-normal-20260409t120000z/processed/part-00002-8edfe7c8-bc54-4bae-bee1-297a651341a7-c000.snappy.parquet
2026-04-09 12:09:17      33287 evaluations/baseline-normal-20260409t120000z/processed/part-00002-8f3f6585-d391-4216-9c5f-083160ce8b72-c000.snappy.parquet
2026-04-09 12:11:07      31767 evaluations/baseline-normal-20260409t120000z/processed/part-00002-91aa9652-9a84-4bde-ab01-ea9465d84d8a-c000.snappy.parquet
2026-04-09 12:05:11      31219 evaluations/baseline-normal-20260409t120000z/processed/part-00002-9762b5d8-b234-4fe7-a53a-47a7e41f926f-c000.snappy.parquet
2026-04-09 12:08:02      29700 evaluations/baseline-normal-20260409t120000z/processed/part-00002-9e3969c2-cfc0-4b7f-b82c-0face9ebc36b-c000.snappy.parquet
2026-04-09 12:09:00      32089 evaluations/baseline-normal-20260409t120000z/processed/part-00002-d007f0f4-ddb6-4824-bcb2-456425530b5c-c000.snappy.parquet
2026-04-09 12:04:26      33116 evaluations/baseline-normal-20260409t120000z/processed/part-00002-e37c9ed1-1918-40eb-a765-25b135b1cb12-c000.snappy.parquet
2026-04-09 12:10:52      30239 evaluations/baseline-normal-20260409t120000z/processed/part-00002-edef5587-835b-438f-bfd5-b0f388efee18-c000.snappy.parquet
2026-04-09 12:10:22      32726 evaluations/baseline-normal-20260409t120000z/processed/part-00002-ef5f80e3-2d45-4eae-a556-5fbf6dac02a8-c000.snappy.parquet
2026-04-09 12:04:10      33978 evaluations/baseline-normal-20260409t120000z/processed/part-00002-f64d5155-6c26-4126-a5ba-64571dc73224-c000.snappy.parquet
2026-04-09 12:03:52      34870 evaluations/baseline-normal-20260409t120000z/processed/part-00002-f7a9ca78-445e-46fe-9c86-ee708616f2ef-c000.snappy.parquet
2026-04-09 12:06:30      34339 evaluations/baseline-normal-20260409t120000z/processed/part-00002-faf2e080-3ffb-49e8-b44d-49d1ffffee34-c000.snappy.parquet
2026-04-09 12:08:46      27951 evaluations/baseline-normal-20260409t120000z/processed/part-00002-fef4b94f-c96c-44d2-bf01-31b9b8572b37-c000.snappy.parquet
2026-04-09 12:07:19      33471 evaluations/baseline-normal-20260409t120000z/processed/part-00003-045195bb-8bcf-446b-abeb-6b733d2a7917-c000.snappy.parquet
2026-04-09 12:01:51      33906 evaluations/baseline-normal-20260409t120000z/processed/part-00003-06ace898-ed22-49d2-ab28-40ab8aa7dc47-c000.snappy.parquet
2026-04-09 12:09:51      32076 evaluations/baseline-normal-20260409t120000z/processed/part-00003-07721e71-bcb6-4882-aaa8-5087fb3de5ba-c000.snappy.parquet
2026-04-09 12:00:56      41713 evaluations/baseline-normal-20260409t120000z/processed/part-00003-07f47a3e-dc41-4ef1-9903-965147a470be-c000.snappy.parquet
2026-04-09 12:12:29      17271 evaluations/baseline-normal-20260409t120000z/processed/part-00003-0ce37e46-2d61-42ca-b61b-0eb1e4d38e0a-c000.snappy.parquet
2026-04-09 12:05:26      30789 evaluations/baseline-normal-20260409t120000z/processed/part-00003-16550c37-a61b-4b31-b742-ea03161c303e-c000.snappy.parquet
2026-04-09 12:02:08      32864 evaluations/baseline-normal-20260409t120000z/processed/part-00003-1e8dc07c-75f7-43d9-8c4e-0f40711c48d4-c000.snappy.parquet
2026-04-09 12:05:58      30908 evaluations/baseline-normal-20260409t120000z/processed/part-00003-27a389ec-475e-416d-9c06-b164dcffbfbc-c000.snappy.parquet
2026-04-09 12:03:35      34266 evaluations/baseline-normal-20260409t120000z/processed/part-00003-2e0668e1-49a9-4337-984c-d2625f241282-c000.snappy.parquet
2026-04-09 12:02:24      35964 evaluations/baseline-normal-20260409t120000z/processed/part-00003-3643b0ee-a04f-4cba-b1e3-2c8a3a76060c-c000.snappy.parquet
2026-04-09 12:07:48      28211 evaluations/baseline-normal-20260409t120000z/processed/part-00003-3ee787d7-a9e4-4e2d-96d3-7627b0702ecc-c000.snappy.parquet
2026-04-09 12:10:08      33592 evaluations/baseline-normal-20260409t120000z/processed/part-00003-4ae8ae58-8be7-4794-846d-68a815ac3214-c000.snappy.parquet
2026-04-09 12:11:56      31732 evaluations/baseline-normal-20260409t120000z/processed/part-00003-52050ca6-21d3-4e88-ba0f-e60c89496fe3-c000.snappy.parquet
2026-04-09 12:02:42      33220 evaluations/baseline-normal-20260409t120000z/processed/part-00003-554540e1-990b-4936-a702-8d33c0b00f85-c000.snappy.parquet
2026-04-09 12:11:22      30978 evaluations/baseline-normal-20260409t120000z/processed/part-00003-57b32b79-7bee-45ca-b010-98da4ffc3b8e-c000.snappy.parquet
2026-04-09 12:07:34      30366 evaluations/baseline-normal-20260409t120000z/processed/part-00003-58b0dc1f-725e-466b-8295-c04ed2eea918-c000.snappy.parquet
2026-04-09 12:01:15      36730 evaluations/baseline-normal-20260409t120000z/processed/part-00003-62705bc9-8e18-4dc4-a21d-af195066b9ff-c000.snappy.parquet
2026-04-09 12:05:43      31327 evaluations/baseline-normal-20260409t120000z/processed/part-00003-63c34683-b179-4ae3-978f-f311f1e623b1-c000.snappy.parquet
2026-04-09 12:11:40      34451 evaluations/baseline-normal-20260409t120000z/processed/part-00003-64f4c465-6c2c-43c4-8d6c-916e848b00e7-c000.snappy.parquet
2026-04-09 12:06:14      33726 evaluations/baseline-normal-20260409t120000z/processed/part-00003-6647e812-c851-4cdc-866d-60c305456c33-c000.snappy.parquet
2026-04-09 12:00:35      26953 evaluations/baseline-normal-20260409t120000z/processed/part-00003-67c5799d-40e6-4d96-8556-3b759c0feeaf-c000.snappy.parquet
2026-04-09 12:04:56      30289 evaluations/baseline-normal-20260409t120000z/processed/part-00003-6837a47e-63ba-4376-880b-ad45ff522f06-c000.snappy.parquet
2026-04-09 12:08:31      30395 evaluations/baseline-normal-20260409t120000z/processed/part-00003-688321f5-d6d3-4d80-8ad1-96abe917606d-c000.snappy.parquet
2026-04-09 12:03:17      35350 evaluations/baseline-normal-20260409t120000z/processed/part-00003-6fcd7015-63af-4bc1-8bfd-0f3d2cad6f62-c000.snappy.parquet
2026-04-09 12:03:00      33873 evaluations/baseline-normal-20260409t120000z/processed/part-00003-70c410a4-d54d-4a78-a181-e69bd5e97698-c000.snappy.parquet
2026-04-09 12:09:34      31668 evaluations/baseline-normal-20260409t120000z/processed/part-00003-765543d9-de54-4826-b2ec-6a3c0cbc50be-c000.snappy.parquet
2026-04-09 12:04:41      29128 evaluations/baseline-normal-20260409t120000z/processed/part-00003-766b2c6d-1cdc-4a69-993e-4b1b919dbba1-c000.snappy.parquet
2026-04-09 12:07:03      32840 evaluations/baseline-normal-20260409t120000z/processed/part-00003-789a7ba7-fc49-45a8-9221-3fcab2598759-c000.snappy.parquet
2026-04-09 12:01:33      33751 evaluations/baseline-normal-20260409t120000z/processed/part-00003-79b227e5-733c-4242-9f3c-02fa284b671d-c000.snappy.parquet
2026-04-09 12:06:47      31232 evaluations/baseline-normal-20260409t120000z/processed/part-00003-7b8ede77-0e4c-4426-86fb-d58c90d4d639-c000.snappy.parquet
2026-04-09 12:10:37      26784 evaluations/baseline-normal-20260409t120000z/processed/part-00003-8321d437-b70e-4a91-ab85-36053574c9da-c000.snappy.parquet
2026-04-09 12:08:17      29485 evaluations/baseline-normal-20260409t120000z/processed/part-00003-87b1b8d3-48fe-431c-aec9-535def31d4db-c000.snappy.parquet
2026-04-09 12:12:12      32019 evaluations/baseline-normal-20260409t120000z/processed/part-00003-8edfe7c8-bc54-4bae-bee1-297a651341a7-c000.snappy.parquet
2026-04-09 12:09:17      31355 evaluations/baseline-normal-20260409t120000z/processed/part-00003-8f3f6585-d391-4216-9c5f-083160ce8b72-c000.snappy.parquet
2026-04-09 12:11:08      31157 evaluations/baseline-normal-20260409t120000z/processed/part-00003-91aa9652-9a84-4bde-ab01-ea9465d84d8a-c000.snappy.parquet
2026-04-09 12:05:11      31519 evaluations/baseline-normal-20260409t120000z/processed/part-00003-9762b5d8-b234-4fe7-a53a-47a7e41f926f-c000.snappy.parquet
2026-04-09 12:08:02      27566 evaluations/baseline-normal-20260409t120000z/processed/part-00003-9e3969c2-cfc0-4b7f-b82c-0face9ebc36b-c000.snappy.parquet
2026-04-09 12:09:01      30184 evaluations/baseline-normal-20260409t120000z/processed/part-00003-d007f0f4-ddb6-4824-bcb2-456425530b5c-c000.snappy.parquet
2026-04-09 12:04:26      33311 evaluations/baseline-normal-20260409t120000z/processed/part-00003-e37c9ed1-1918-40eb-a765-25b135b1cb12-c000.snappy.parquet
2026-04-09 12:10:52      31382 evaluations/baseline-normal-20260409t120000z/processed/part-00003-edef5587-835b-438f-bfd5-b0f388efee18-c000.snappy.parquet
2026-04-09 12:10:23      33175 evaluations/baseline-normal-20260409t120000z/processed/part-00003-ef5f80e3-2d45-4eae-a556-5fbf6dac02a8-c000.snappy.parquet
2026-04-09 12:04:10      34308 evaluations/baseline-normal-20260409t120000z/processed/part-00003-f64d5155-6c26-4126-a5ba-64571dc73224-c000.snappy.parquet
2026-04-09 12:03:52      33912 evaluations/baseline-normal-20260409t120000z/processed/part-00003-f7a9ca78-445e-46fe-9c86-ee708616f2ef-c000.snappy.parquet
2026-04-09 12:06:31      31709 evaluations/baseline-normal-20260409t120000z/processed/part-00003-faf2e080-3ffb-49e8-b44d-49d1ffffee34-c000.snappy.parquet
2026-04-09 12:08:46      30130 evaluations/baseline-normal-20260409t120000z/processed/part-00003-fef4b94f-c96c-44d2-bf01-31b9b8572b37-c000.snappy.parquet
2026-04-09 11:59:11   15254191 evaluations/baseline-normal-20260409t120000z/workloads/normal.events.ndjson
```

## Cluster Snapshot

```text
NAME                                    READY   STATUS      RESTARTS   AGE
pod/kafka-0                             1/1     Running     0          24m
pod/replay-producer-dcqpz               0/1     Completed   0          16m
pod/stream-processor-5fbc9bb6d9-lxkxs   1/1     Running     0          16m

NAME            TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
service/kafka   ClusterIP   None         <none>        9092/TCP   32h

NAME                               READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/stream-processor   1/1     1            1           32h

NAME                                          DESIRED   CURRENT   READY   AGE
replicaset.apps/stream-processor-56d59966c    0         0         0       8h
replicaset.apps/stream-processor-579c8d5596   0         0         0       8h
replicaset.apps/stream-processor-59b8959fdc   0         0         0       27m
replicaset.apps/stream-processor-5f8996bc77   0         0         0       10h
replicaset.apps/stream-processor-5fbc9bb6d9   1         1         1       16m
replicaset.apps/stream-processor-65948df994   0         0         0       10h
replicaset.apps/stream-processor-65964b7b84   0         0         0       8h
replicaset.apps/stream-processor-65fd575586   0         0         0       8h
replicaset.apps/stream-processor-7998b5dbcd   0         0         0       7h35m
replicaset.apps/stream-processor-7d9b4f75c6   0         0         0       10h
replicaset.apps/stream-processor-866bd8567d   0         0         0       23m

NAME                     READY   AGE
statefulset.apps/kafka   1/1     32h

NAME                                            STATUS      COMPLETIONS   DURATION   AGE
job.batch/evaluation-controller                 Suspended   0/1                      8h
job.batch/evaluation-controller-smoke-run-001   Failed      0/1           8h         8h
job.batch/evaluation-controller-smoke-run-002   Complete    1/1           9s         8h
job.batch/replay-producer                       Complete    1/1           13m        16m
```

## Important Findings

- Replay completed successfully at the configured `100.0 eps` rate for the full `79,200`-event normal workload.
- The pipeline produced `32,610` processed events, `1,440` invalid events, `1` deduplicated event, and `457` breach events.
- Reported end-to-end latency stayed above the project target: average `7.6 s`, p95 `13.79 s`.
- During live observation, the stream processor repeatedly logged `Current batch is falling behind` warnings while handling the normal workload.
- Output evidence was substantial and complete enough for evaluation: `181` processed objects, `19` invalid objects, and `173` breach-window objects were written under the run prefix.
