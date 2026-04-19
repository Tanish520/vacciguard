# Optimized Pipeline Improvements and SLA-Aware Design

This document explains, in a friendly technical way, what was going wrong in the unoptimized pipeline, what was improved in the optimized pipeline, and why the SLA-aware split architecture was the most important novel idea. It is written to be easy to understand and useful for report writing.

---

## 1. What Was the Problem in the Unoptimized Pipeline?

The original pipeline was functionally correct, but it was not architected to preserve latency guarantees under load. Its central limitation was not data correctness; rather, it was the absence of workload prioritization. In effect, the system treated latency-critical live monitoring and latency-insensitive archival processing as if they were equivalent, even though they serve very different operational purposes.

From a systems perspective, the pipeline had two distinct classes of work:

- **Hot work**: updating the latest device state in Redis so operators can observe current refrigerator status with minimal delay.
- **Cold work**: writing processed data, invalid records, and breach windows to S3 for historical analysis, auditability, and reporting.

The bottleneck emerged because these two classes of work were coupled too tightly in the same execution environment. When the system became busy performing archival writes or heavier Spark transformations, the live Redis path was forced to compete for the same compute and streaming resources. The result was a loss of responsiveness in the very part of the system that needed to remain fresh. For a vaccine cold-chain monitoring application, that is a serious design flaw: stale live state can delay breach recognition and reduce the operational usefulness of the dashboard.

### What the data showed

The measurements make the structural problem explicit:

- **Baseline normal load**: `2.60 s` average latency, `3.11 s` p95
- **Baseline spike load**: `225.76 s` average latency, `225.77 s` p95

These results show that the baseline pipeline was adequate only under steady conditions. Under spike load, the system did not merely degrade modestly; it failed to sustain real-time behavior altogether. Average latency increased from seconds to several minutes, which indicates a queueing and backlog problem rather than a small performance regression.

This is why the issue should be understood as architectural rather than incidental. The pipeline was not bottlenecked by a single isolated function. It was bottlenecked by the way urgent and non-urgent work shared the same processing path.

---

## 2. Why the Baseline Architecture Struggled

There were several compounding reasons the unoptimized pipeline could not sustain spike load:

### a. Real-time and archival work competed for the same compute

The hot Redis update path and the cold S3 path were not isolated strongly enough. Under load, archival operations could consume processing time, Spark scheduling capacity, and JVM attention that should have been reserved for the live path. This competition is especially harmful in streaming systems because latency-sensitive batches must complete on schedule to prevent backlog growth.

### b. The hot path still did too much work in one place

The live path itself was not sufficiently streamlined. It still carried expensive responsibilities such as latest-state selection, Redis updates, and Spark coordination. That means the path intended to be “real-time” was still doing enough work to become sensitive to any increase in input rate or JVM overhead. A real-time path should be narrow and predictable; the baseline path was broader and more fragile than it should have been.

### c. The system did not prioritize SLA-critical work

The pipeline did not explicitly encode the fact that some outputs are more time-sensitive than others. In a monitoring system, the freshest device status is the most operationally important artifact. Historical files are valuable, but they are not equally urgent. Because the baseline architecture did not express this priority, it could not protect the latency target that matters most.

### d. Spike load created backlog faster than the pipeline could drain it

At `1000 eps`, the baseline system could not drain the incoming workload as quickly as it arrived. Once backlog began to accumulate, each additional batch had to wait behind earlier work, which caused latency to compound over time. This is the classic failure mode of an undersized or poorly prioritized streaming pipeline: the system remains busy, but the output becomes increasingly stale.

In short, the baseline was not failing because of data corruption or functional errors. It was failing because the architecture did not create a sufficiently strong separation between urgent and non-urgent processing, and therefore could not preserve latency under stress.

---

## 3. What We Changed in the Optimized Pipeline

The optimized pipeline was redesigned to solve the problem at the architectural level, not just by tweaking small settings.

### a. We split the pipeline into hot and cold services

This is the most important change.

- **`optimized-hot`** handles the real-time Redis and latency-sensitive path.
- **`optimized-cold`** handles the slower archival and reporting path.

This means the pipeline no longer treats all work as equal. The live monitoring path is protected first, and the archival path is isolated so it cannot slow down Redis freshness.

### b. We made the hot path lighter and more focused

The hot service was tuned to do only the work needed for immediate visibility:

- consume Kafka telemetry
- validate events needed for real-time state
- deduplicate
- compute latest state per device
- write live state to Redis
- publish SLA latency metrics

It does not own the S3 archival responsibilities anymore. That is a big reason the hot path became much faster and more stable.

### c. We separated metric ownership

This was important for clean reporting.

- Hot metrics now belong to the hot service.
- Cold archive counts belong to the cold service.

That prevents the archival path from corrupting or overwriting the live latency metrics. It also makes the evaluation results easier to trust.

### d. We increased the hot-path capacity and removed artificial bottlenecks

We also made the hot path more capable of handling spike load by:

- increasing the intake capacity
- using a faster trigger cadence
- removing the hot-path Kafka intake cap as a limiting factor
- using partition-local Redis writes instead of a driver-only loop
- replacing a heavier latest-row strategy with a cheaper grouped latest-state approach

These changes matter because they reduce the amount of work the hot service must do per batch and let it focus on the SLA-critical path.

### e. We kept the external outputs the same

The important part is that the outputs did not change from the user’s point of view:

- same Redis device status shape
- same S3 output layout
- same report fields

So the optimization improved performance without breaking the evaluation contract.

---

## 4. Why the SLA-Aware Novelty Matters

The novel aspect we chose was **SLA-aware data processing**.

That sounds academic, but the idea is simple:

> The pipeline should protect the latency-sensitive part of the workload first, and allow the less urgent part to lag if needed.

For vaccine cold-chain monitoring, that is exactly the right behavior.

### Why this is useful

In this domain, there are two different priorities:

- **Priority 1**: show the latest fridge status and breach state as quickly as possible
- **Priority 2**: write historical outputs and archival records reliably

If the system is overloaded, it is acceptable for the archive to be a little slower. It is **not** acceptable for the live safety dashboard to become stale.

So the SLA-aware split pipeline is useful because it makes the system behave like a real monitoring system should behave:

- it protects the safety-critical path
- it degrades gracefully under overload
- it keeps the system useful even when traffic spikes

That is a much stronger design than simply saying “we optimized Spark.”

---

## 5. How We Achieved the SLA-Aware Design

The SLA-aware design was not just a label. It was implemented in the architecture and the evaluation flow.

### Hot path

The hot path became the SLA owner. It is responsible for:

- live device status
- Redis freshness
- latency metrics
- quick recovery after restarts

### Cold path

The cold path became the archival worker. It is responsible for:

- processed output files
- invalid output files
- breach window output files
- slower S3 writes

### Controller behavior

The evaluation controller now orchestrates both services together and combines their outputs into one optimized report. That lets us preserve the same external report format while still separating the internal responsibilities.

### Why this helped in practice

The results show the benefit clearly:

- The hot path stayed around `1 to 2 seconds` in the optimized normal and spike runs.
- The cold path could still be slower, but that did not drag down Redis latency anymore.
- The system stayed under `5 seconds` in both normal and spike scenarios.

This is the key point: the SLA-aware split did not just make the pipeline “faster.” It made the pipeline **priority-aware**.

---

## 6. What the Numbers Say

The best way to understand the improvement is to compare the baseline and optimized results side by side.

### Normal load comparison

- **Baseline normal**: `2.60 s` avg, `3.11 s` p95
- **Optimized normal mean across 3 runs**: `1.56 s` avg, `1.75 s` p95

That means the optimized design is not only under the SLA, it is comfortably below it with headroom.

### Spike load comparison

- **Baseline spike**: `225.76 s` avg, `225.77 s` p95
- **Optimized spike mean across 3 runs**: `2.10 s` avg, `2.32 s` p95

That is the most important comparison in the project. The baseline pipeline became unusably slow under spike traffic, while the optimized split pipeline stayed in the low-second range.

### Failure recovery comparison

- **Baseline failure recovery**: `24 s` recovery
- **Optimized failure recovery mean across 3 runs**: `15.67 s` recovery

That shows the optimized pipeline is also better at recovering from a forced hot-service restart.

### What this means

The improvement is not just a small tuning gain. It is an architectural improvement:

- the live path is faster
- the live path is more stable
- the live path recovers faster
- the archival path no longer dominates the SLA

---

## 7. Why the Optimized Results Are More Trustworthy

The optimized results are strong because they are consistent across multiple runs:

- normal load was tested three times
- spike load was tested three times
- failure recovery was tested three times

That matters because a single good run could be luck. Three repeated runs showing the same pattern are much better evidence.

The important repeated pattern is:

- consumer lag stayed at `0`
- the hot path latency stayed low
- the system drained the workload cleanly

That means the optimized pipeline is not merely hiding backlog. It is actually handling the workload better.

---

## 8. Final Takeaway

The unoptimized pipeline struggled because it did not separate urgent live monitoring from slower archival work. Under spike load, that caused backlog and huge latency growth.

The optimized pipeline fixed that by introducing an SLA-aware split architecture:

- the hot service protects real-time Redis freshness
- the cold service handles archival work independently
- the controller merges both into one consistent report

The result is easy to explain and easy to justify with data:

- normal load stays below `5 seconds`
- spike load also stays below `5 seconds`
- failure recovery is faster
- the system is more stable and more appropriate for vaccine cold-chain monitoring

In short, the novelty helped because it made the system prioritize the right thing: **fresh live safety data first, archival work second**.
