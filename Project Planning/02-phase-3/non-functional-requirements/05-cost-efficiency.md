# Cost Efficiency

## Requirement
The brief expects the system to minimize cost per GB processed.

## What This Means
The pipeline should avoid using more compute or storage than needed, especially when workload is low.

## How The Architecture Addresses It
### Compute Efficiency
- `Spark dynamic allocation` reduces idle executor usage.
- `KEDA` allows some workers to shrink when Kafka backlog is low.
- `Airflow` schedules heavy batch jobs only when needed.
- `Airflow pools` limit how many expensive jobs run at the same time.

### Storage Efficiency
- `S3` provides lower-cost long-term storage for historical data.
- `Parquet` reduces storage size and improves analytical efficiency compared with plain text formats.
- `Redis` is used only for hot operational state, not long-term history.

### Architectural Efficiency
- using one main processing engine keeps the platform smaller
- fewer always-on components reduce unnecessary overhead

## How We Will Measure It
If exact billing is available, we will use:

- total experiment cost
- cost per GB processed

If exact billing is not available, we will use cost proxy metrics such as:

- total executor time
- total pod runtime
- average active replicas
- storage volume written

## What We Can Honestly Claim
If the optimized version processes the same workload with less compute time, fewer active resources, or lower cost per GB while maintaining acceptable latency, we can claim improved cost efficiency.
