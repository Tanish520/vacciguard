# SLA-Aware Split Pipeline for Low-Latency Vaccine Cold-Chain Monitoring

## Introduction

In a vaccine cold-chain monitoring system, low latency is not merely a performance preference. It is a functional requirement. The system must continuously monitor the temperature and state of refrigeration devices and make the latest status available to operators quickly enough that corrective action can be taken before vaccine quality is compromised. If the pipeline becomes overloaded and fresh device status is delayed, the operational value of the system is reduced even if all records are eventually processed correctly.

For this reason, the most important service-level objective in the project is the freshness of the real-time monitoring path. Historical storage, invalid-record archival, and breach-window summaries remain important, but they are not equally urgent. A delay in generating a historical S3 file is operationally acceptable for a short period. A delay in exposing the latest fridge status in Redis is much more serious because it directly affects breach visibility and response time.

This document explains:

- what was happening in the original unoptimized pipeline,
- why that behavior motivated the selection of an SLA-aware research direction,
- why **SLA-Aware Data Processing** was the most suitable novel aspect for this pipeline,
- and how the final split optimized design improved the system under both normal and spike load.

The discussion is based on the actual AWS evaluation results collected during the project.

---

## 1. Behavior of the Unoptimized Pipeline

### 1.1 Original Design

The unoptimized pipeline used a single stream-processing service to perform both real-time and archival work. Within the same streaming application, the system had to:

- consume Kafka telemetry events,
- validate records,
- deduplicate repeated event IDs,
- enrich the events with lookup data,
- compute breach status,
- update the latest device state in Redis,
- write processed outputs to S3,
- write invalid records to S3,
- and generate breach-window outputs for later analysis.

Although this design was functionally correct, it treated all work as if it had the same urgency. In practice, this meant that the real-time Redis update path and the slower S3 archival path shared the same execution environment, the same JVM, and the same compute resources.

### 1.2 Why the Unoptimized Design Became a Bottleneck

The main architectural problem in the unoptimized pipeline was not correctness but contention. The same service was expected to satisfy two very different goals:

- **low-latency state updates** for operational visibility, and
- **throughput-oriented archival processing** for durable storage and reporting.

These goals compete with each other under load. Redis state updates are time-sensitive and should complete quickly. S3 writes, batch aggregation, and archive generation are heavier operations that can tolerate delay. When both are executed inside the same processing unit, the urgent path becomes vulnerable to delay from the non-urgent path.

This contention became especially visible during spike load. The cold path continued to consume CPU, memory, Spark scheduling attention, and I/O bandwidth even while the hot path was trying to keep Redis up to date. As a result, the pipeline did not fail functionally, but it stopped satisfying the real-time latency goal.

### 1.3 Baseline Performance Under Normal Load

Under normal load, the baseline pipeline performed acceptably. The recorded AWS baseline normal-load evaluation reported:

- average end-to-end latency: `2.60 s`
- p95 end-to-end latency: `3.11 s`
- ingest-to-Redis p95 latency: `3.15 s`
- throughput: `100.0 eps`
- processed events: `32,349 / 33,000`
- consumer lag: `0`

These results show that the original design could meet the target under moderate load. The system remained stable because the arrival rate did not force severe contention between the hot and cold responsibilities.

### 1.4 Baseline Performance Under Spike Load

The weakness of the unoptimized pipeline became obvious under the spike workload. In the AWS baseline spike evaluation, the system processed:

- input rate: `1000 eps`
- total input events: `330,000`

but reported:

- average end-to-end latency: `225.76 s`
- p95 end-to-end latency: `225.77 s`
- ingest-to-Redis p95 latency: `225.77 s`
- throughput: `999.9 eps`
- processed events: `323,919 / 330,000`
- consumer lag: `0`

The most important observation is that the pipeline remained correct, but it was no longer timely. It eventually drained the Kafka topic and preserved data, yet it allowed latency to grow to almost four minutes. This means the system still functioned as an archive pipeline, but it no longer functioned as a real-time monitoring system.

### 1.5 What Was Actually Going Wrong

The spike result showed that the pipeline’s main weakness was architectural rather than logical. The classification logic still worked. Invalid-event rates remained consistent. The breach counts remained meaningful. No large-scale correctness failure occurred. Instead, the pipeline became saturated because the urgent and non-urgent workloads were tightly coupled.

Under overload, the unoptimized design had no mechanism to protect the real-time SLA. It handled every task inside one processing path, which meant that archival work could indirectly slow the latest-state path. From a systems perspective, the design had no explicit notion of priority.

This is exactly the type of problem addressed by **SLA-aware data processing**.

---

## 2. Why SLA-Aware Data Processing Was Chosen as the Novel Aspect

### 2.1 Assignment Context

The project required the exploration of at least one research and innovation opportunity. The available directions included:

- cost-latency optimization,
- intelligent auto-scaling,
- SLA-aware data processing,
- and fault tolerance and consistency improvements.

Although several of these topics are academically valuable, not all of them fit this system equally well.

### 2.2 Why Other Options Were Less Suitable

**Cost-latency optimization** was not the strongest choice because the dominant project challenge was not cost inefficiency but latency collapse under spike load. A cost-focused extension would have required a more detailed pricing model and a larger experimental study around resource classes, spot instances, or dynamic provisioning.

**Intelligent auto-scaling** was also less suitable for the current project scope. Predictive scaling, reinforcement learning, or workload forecasting can be interesting research topics, but they introduce significant implementation and validation complexity. More importantly, they do not directly address the fact that the original pipeline was architecturally coupling urgent and non-urgent work.

**Fault tolerance and consistency** is important in any streaming system, but it was not the clearest research opportunity for this pipeline. The main observable weakness in the baseline was not that the system lost correctness under failure. It was that the system preserved correctness while sacrificing timeliness under overload.

### 2.3 Why SLA-Aware Data Processing Was the Best Fit

SLA-aware data processing was the strongest choice because it directly addressed the central weakness of the original pipeline: the lack of separation between time-critical and non-time-critical work.

In this project, the natural SLA-aware interpretation is:

- prioritize live state freshness,
- preserve breach visibility under overload,
- and allow non-urgent archival tasks to progress independently.

This made SLA-aware processing both academically meaningful and practically relevant. It was not an artificial research add-on. It emerged directly from the observed system behavior.

### 2.4 Research Framing

The chosen novel aspect can therefore be stated as follows:

> The optimized system introduces an **SLA-aware split-processing architecture** in which the real-time operational path is isolated from the archival path, so that overload in historical storage tasks does not compromise latency guarantees for live vaccine cold-chain monitoring.

This is a valid research and innovation contribution because it combines:

- **priority-based processing**, by protecting the operational Redis path,
- **graceful degradation**, by allowing archival work to proceed independently rather than blocking the live path,
- and **architectural adaptation**, by redesigning the pipeline around service-level objectives rather than around a single undifferentiated batch path.

---

## 3. The Chosen Solution: SLA-Aware Split Pipeline

### 3.1 Core Idea

The optimized design separates the pipeline into two cooperating services:

- `optimized-hot`
- `optimized-cold`

Both consume the same Kafka input, but they serve different purposes.

### 3.2 Optimized Hot Path

The hot service is responsible only for the latency-sensitive tasks:

- reading telemetry from Kafka,
- validating records needed for live state,
- deduplicating repeated records,
- computing the latest device state,
- determining breach status for live monitoring,
- writing the latest device status to Redis,
- and publishing the latency metrics that represent the SLA.

This path is intentionally minimal. It exists to keep the latest operational view as fresh as possible.

### 3.3 Optimized Cold Path

The cold service is responsible for slower archival tasks:

- generating processed outputs,
- writing invalid records,
- producing breach-window summaries,
- and persisting these outputs to S3.

These tasks remain important for analytics, verification, and reporting, but they are no longer allowed to interfere with the hot path.

### 3.4 Why This Is SLA-Aware

This design is SLA-aware because it makes the service-level objective an explicit architectural concern.

Instead of assuming that all work is equally urgent, the pipeline now acknowledges that:

- Redis freshness is part of the operational SLA,
- S3 archival is necessary but less time-sensitive,
- and overload should first be absorbed by the non-critical side rather than by the real-time monitoring path.

This is the essence of SLA-aware data processing: the system is organized around what must remain fast even when the workload becomes difficult.

### 3.5 Graceful Degradation

The design also provides graceful degradation. If the system experiences stress:

- the hot path should continue keeping Redis current,
- the cold path may lag slightly in S3 archival work,
- but the real-time visibility of vaccine device status remains protected.

This is a much better overload behavior than the original design, in which every type of work slowed down together.

---

## 4. How the Chosen Aspect Helps the System

### 4.1 Protection of the Most Important Business Function

The most important function in a vaccine cold-chain monitoring system is timely awareness of unsafe storage conditions. If a refrigeration unit enters breach state, the system should expose that information immediately so that staff can intervene.

By isolating the Redis state-update path, the optimized design ensures that this operational goal is not held hostage by archival work.

### 4.2 Reduced Resource Contention

The split design reduces contention in several ways:

- separate deployments isolate hot and cold execution,
- the hot path receives its own trigger settings and resource profile,
- the cold path no longer competes directly with Redis-state publication,
- and metrics ownership is clearly separated so reporting remains interpretable.

This improves not only latency but also observability, because the latency metrics now genuinely reflect the live path rather than a mixed workload.

### 4.3 Better Overload Behavior

Under overload, the system now behaves in a way that is more suitable for healthcare monitoring:

- live visibility remains fast,
- archival output continues independently,
- and the pipeline avoids catastrophic latency blow-up.

This is a more operationally mature behavior than simply “processing everything eventually.”

### 4.4 Easier Future Extensions

The SLA-aware split architecture also creates a strong foundation for future research or production features, including:

- independent scaling of hot and cold services,
- more advanced admission control or prioritization,
- adaptive trigger intervals,
- or even separate cost strategies for real-time and archival paths.

In that sense, the chosen innovation does not only solve the current problem. It also creates a better structure for future system evolution.

---

## 5. Evidence From Evaluation

### 5.1 Baseline vs Optimized Under Normal Load

Under normal load, both designs satisfied the latency target, but the optimized design performed better:

| Metric | Baseline Normal | Optimized Normal |
|---|---:|---:|
| Input rate | `100 eps` | `100 eps` |
| Avg latency | `2.60 s` | `1.56 s` |
| P95 latency | `3.11 s` | `1.75 s` |
| Ingest-to-Redis P95 | `3.15 s` | `1.75 s` |
| Processed rate | `98.03%` | `98.05%` |

This shows that the optimized design is not only better under overload. It also improves steady-state latency while preserving the expected output quality.

### 5.2 Baseline vs Optimized Under Spike Load

The most important comparison is the spike scenario:

| Metric | Baseline Spike | Optimized Spike Mean |
|---|---:|---:|
| Input rate | `1000 eps` | `1000 eps` |
| Avg latency | `225.76 s` | `2.10 s` |
| P95 latency | `225.77 s` | `2.32 s` |
| Ingest-to-Redis P95 | `225.77 s` | `2.32 s` |
| Throughput | `999.9 eps` | `999.93 eps` |
| Processed rate | `98.16%` | `98.04%` |
| Consumer lag | `0` | `0` |

This result is the clearest validation of the research choice. The optimized split pipeline preserved throughput and correctness while reducing spike latency from several minutes to a little above two seconds on average.

### 5.3 Interpretation of the Results

These results support three important conclusions:

1. The baseline system’s main problem was architectural contention, not logical failure.
2. The chosen SLA-aware split design directly addressed that contention.
3. The improvement is large enough to be academically meaningful, not merely incremental.

The optimized pipeline did not simply become “slightly faster.” It changed from a system that lost its real-time value under spike load into one that maintained a stable low-latency profile even at `1000 eps`.

---

## 6. Academic Significance of the Chosen Aspect

This work is significant from a research and engineering perspective because it demonstrates that low-latency guarantees in a monitoring pipeline depend not only on raw compute capacity, but also on how the pipeline prioritizes competing forms of work.

The contribution of this project is therefore not limited to implementation detail. The important insight is architectural:

> In real-time cold-chain monitoring, urgent operational state and non-urgent archival processing should not be treated as equal citizens in the same execution path.

By recognizing this and redesigning the system around the SLA, the project demonstrates a concrete instance of **priority-aware stream processing** for a healthcare-adjacent use case.

This makes the work suitable for presentation as a novel aspect under **7.3 SLA-Aware Data Processing**.

---

## 7. Conclusion

The unoptimized pipeline was functionally correct but architecturally unsuitable for high-load real-time monitoring. It combined urgent Redis state updates and slower S3 archival work within a single processing path, which caused severe latency degradation under spike load. Although the system continued to process data correctly, it no longer met the operational needs of a vaccine cold-chain monitoring scenario.

SLA-aware data processing was chosen as the research and innovation focus because it directly addressed this weakness. Among the candidate innovation topics, it was the most natural and most defensible choice for the observed system behavior. The resulting optimized split architecture introduced a hot path for real-time Redis visibility and a cold path for archival S3 outputs, thereby aligning the system structure with its service-level priorities.

The evaluation results demonstrate that this choice was effective. Under spike load, the baseline pipeline showed a p95 latency of `225.77 s`, whereas the optimized split pipeline achieved a three-run mean p95 latency of `2.32 s` while preserving throughput and output correctness. This confirms that the selected novel aspect was not only theoretically justified but practically successful.

For this reason, the **SLA-Aware Split Pipeline for Low-Latency Vaccine Cold-Chain Monitoring** can be presented as a meaningful research contribution within the project.
