# Phase 3: Non-Functional Requirements Mapping

## Purpose
This document explains how the selected architecture and tool stack will address the non-functional requirements listed in the project brief:

- scalability
- latency
- reliability
- data quality
- cost efficiency

It also explains how each requirement will be measured during evaluation so that the claims are evidence-based.

## Quick Summary
| Requirement | Target From Brief | Main Mechanisms | Main Metrics |
| --- | --- | --- | --- |
| Scalability | Auto-scale for 10x traffic spikes | Kafka partitions, KEDA, HPA, Spark dynamic allocation | throughput, lag, replica count, executor count, recovery time |
| Latency | Near-real-time analytics under 5 seconds | Kafka, Spark Structured Streaming, lightweight enrichment, Redis hot path | end-to-end latency, p95 delay, watermark delay |
| Reliability | 99.9% pipeline uptime | Kubernetes restarts, replicated services, Kafka durability, Spark checkpoints, Airflow retries | uptime, successful processing rate, recovery time, failed job count |
| Data Quality | Validation and deduplication | schema checks, range validation, required fields, event ID deduplication, quarantine path | invalid record count, duplicate count, late record count, clean record rate |
| Cost Efficiency | Minimize cost per GB processed | Airflow scheduling, Spark dynamic allocation, KEDA scale-in, S3, Parquet | cost per GB, active compute time, average replicas, latency-cost trade-off |

## 1. Scalability
### Requirement
The system should handle large changes in incoming traffic and should automatically scale during a 10x spike.

### How The Architecture Addresses It
Scalability is handled in both the ingestion layer and the processing layer.

#### Ingestion Layer
- `Kafka` absorbs spikes by buffering incoming events instead of forcing all downstream services to keep up instantly.
- `Kafka partitions` allow parallel consumption, so the system is not limited to a single consumer path.
- `KEDA` watches Kafka lag and scales relevant Kubernetes pods when backlog grows.
- `HPA` can scale regular services that are better measured by CPU or memory rather than event lag.

#### Processing Layer
- `Spark Structured Streaming` processes stream data continuously in controlled micro-batches.
- `Spark dynamic allocation` increases executors when more compute is needed and reduces them when load drops.
- `Kubernetes` provides the environment in which additional processing resources can be created.

### Why This Is A Good Fit
This design gives a clear elasticity story:

- traffic spikes first appear as increased message backlog
- the ingestion side scales so data is pulled faster
- the processing side scales so records are processed faster
- resources shrink again when traffic returns to normal

### How We Will Measure It
We will run controlled workloads such as:

- normal load
- 5x spike
- 10x spike
- burst-and-recovery pattern

For each run, we will capture:

- input rate
- throughput
- Kafka lag
- number of running pods
- number of Spark executors
- time taken to stabilize after the spike

### What We Can Honestly Claim
If the optimized pipeline handles the 10x spike with acceptable lag growth and recovery, we can claim that the design supports elastic scaling. We should not claim infinite scalability. We should claim tested scalability within the defined workload range.

## 2. Latency
### Requirement
Near-real-time analytics should remain below 5 seconds.

### How The Architecture Addresses It
Latency is mainly handled by keeping the live path short and lightweight.

- `Kafka` accepts events quickly and decouples producers from consumers.
- `Spark Structured Streaming` processes events in small intervals instead of waiting for large batch windows.
- `Device and facility lookup data` is small, so enrichment joins can be kept lightweight.
- `Redis` stores the latest operational state so the live path does not depend on heavy analytical queries.
- `S3 + Parquet` is used for historical storage, not for the immediate alerting path.

### Design Principle
The system uses two different paths:

- a fast live path for alerts and current state
- a slower batch path for reporting and historical analysis

This separation is important because trying to serve live alerts from historical analytics storage would increase delay.

### How We Will Measure It
The main latency metric will be:

- `end-to-end delay = output_time - event_time`

We should measure:

- average latency
- p95 latency
- p99 latency if possible
- watermark delay or processing lag if exposed

We should test latency under:

- normal load
- burst load
- after scaling events
- during partial failure and recovery

### What We Can Honestly Claim
If the live path stays under 5 seconds in normal operation and remains reasonable during spikes, we can claim near-real-time performance. If burst scenarios temporarily exceed 5 seconds, we should report that honestly as a trade-off.

## 3. Reliability
### Requirement
The brief mentions at least 99.9% pipeline uptime.

### How The Architecture Addresses It
Reliability is addressed through restart, replay, and recovery mechanisms.

- `Kubernetes` can restart failed containers automatically.
- critical stateless services can run with multiple replicas in the optimized setup
- `Kafka` stores events durably so short downstream failures do not immediately lose data
- `Spark checkpoints` preserve processing progress and help the stream job recover
- `Airflow` retries failed batch tasks
- `Prometheus + Grafana` provide visibility into failures and recovery time

### Failure Scenarios We Should Test
- stop a consumer or processing component
- kill a Spark pod or container
- overload the system with burst traffic
- restart a service during active ingestion

### How We Will Measure It
We should track:

- successful processing time versus failure time
- failed job count
- restart count
- recovery time
- data loss count
- duplicate count after restart

### Important Academic Caution
In a student project, claiming true 99.9% uptime over a long production period would be too strong unless you actually ran the system for that duration and measured it.

The better academic framing is:

- `the architecture is designed for high reliability`
- `controlled failure tests show that the system recovers automatically within a measured time`
- `the observed behavior supports the reliability goal`

That is honest and defensible.

## 4. Data Quality
### Requirement
The brief explicitly asks for validation and deduplication.

### How The Architecture Addresses It
Data quality should be enforced inside the processing logic before records are accepted as valid outputs.

#### Validation
We should validate:

- required fields such as `event_id`, `device_id`, and `event_time`
- correct field types
- valid temperature ranges
- valid battery percentage range
- valid reference match for known devices if needed

#### Deduplication
We should deduplicate using `event_id` as the primary key for uniqueness.

If duplicate events arrive because of retries or replay, the pipeline should:

- detect repeated `event_id` values
- suppress duplicate downstream effects
- avoid double-counting breaches or alerts

#### Bad Data Handling
Instead of silently dropping bad records, we should place them into a quarantine or rejected-record path such as:

- a Kafka dead-letter topic
- an S3 rejected-record folder

### How We Will Measure It
We should track:

- total input records
- valid output records
- invalid record count
- duplicate record count
- late record count
- percentage of clean records

### What We Can Honestly Claim
If validation rules are enforced and duplicates are suppressed consistently during normal and failure scenarios, we can claim that the pipeline addresses the data quality requirement. The important point is not perfection, but measurable control over bad and duplicate data.

## 5. Cost Efficiency
### Requirement
The system should minimize cost per GB processed.

### How The Architecture Addresses It
Cost efficiency comes from a combination of scheduling, scaling, and storage decisions.

#### Efficient Processing
- `Spark dynamic allocation` reduces idle executor usage.
- `KEDA` reduces unnecessary always-on workers when backlog is low.
- `Airflow` schedules heavy batch jobs only when needed instead of keeping them active continuously.
- `Airflow pools` prevent too many expensive jobs from running at once.

#### Efficient Storage
- `S3` provides lower-cost long-term storage than keeping everything in a hot system.
- `Parquet` reduces storage size and improves analytical efficiency compared with plain text formats.
- `Redis` is used only for hot operational state, not for full historical retention.

#### Efficient Architecture
- using one main processing engine avoids the cost and complexity of running multiple engines
- keeping the stack small reduces operational overhead

### How We Will Measure It
If exact cloud billing is available, we can use:

- total cloud cost for the experiment window
- cost per GB processed

If exact billing is not available for local or mixed environments, we can use cost proxy metrics such as:

- total executor time
- total pod runtime
- average active replicas
- storage volume written

### What We Can Honestly Claim
If the optimized version processes the same workload with less compute time, fewer active resources, or lower cost per GB while preserving latency goals, then we can claim improved cost efficiency.

## 6. Requirement-To-Mechanism Mapping
| Requirement | Tool Or Mechanism | Role |
| --- | --- | --- |
| Scalability | Kafka partitions | support parallel ingestion and consumption |
| Scalability | KEDA | scale workers based on Kafka lag |
| Scalability | HPA | scale services using CPU or memory metrics |
| Scalability | Spark dynamic allocation | scale processing compute up and down |
| Latency | Kafka | fast ingestion and decoupling |
| Latency | Spark Structured Streaming | near-real-time processing |
| Latency | Redis | fast operational reads for live state |
| Reliability | Kubernetes | restart failed components |
| Reliability | Kafka | durable buffering and replay |
| Reliability | Spark checkpoints | resume processing progress |
| Reliability | Airflow retries | recover failed batch tasks |
| Data Quality | schema and range validation | reject malformed data |
| Data Quality | event ID deduplication | suppress duplicate effects |
| Data Quality | quarantine path | keep bad data visible for analysis |
| Cost Efficiency | Airflow scheduling | avoid unnecessary compute windows |
| Cost Efficiency | Spark dynamic allocation | reduce idle compute |
| Cost Efficiency | KEDA scale-in | shrink low-demand workers |
| Cost Efficiency | S3 + Parquet | efficient long-term storage |

## Final Interpretation
The non-functional requirements are not handled by one single product. They are handled by a coordinated architecture:

- Kafka and Spark provide the live and batch data movement
- Kubernetes, HPA, KEDA, and Spark dynamic allocation provide elasticity
- checkpoints, retries, and replay provide reliability
- validation and deduplication provide data quality
- scheduling, scale-in behavior, and efficient storage provide cost control

The important academic point is that every non-functional requirement should later be backed by:

- one or more implementation mechanisms
- one or more measurable metrics
- controlled experimental evidence
