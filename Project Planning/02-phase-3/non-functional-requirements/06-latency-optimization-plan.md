# Latency Optimization Plan

## Why This File Exists
This note explains three things in simple language:

1. what the current baseline pipeline is doing wrong for the latency requirement
2. what the optimized pipeline should do differently
3. what exact code and design changes we should make to move toward the `<5s` target

This file is intentionally more practical than [02-latency.md](/Users/tanishgupta/Downloads/vacciguard/Project%20Planning/02-phase-3/non-functional-requirements/02-latency.md). The latency requirement file says what the goal is. This file explains why the current baseline misses that goal and how we should redesign the pipeline to improve it.

## The Requirement In Plain Language
The professor's latency requirement is:

- near-real-time analytics under `5 seconds`

For VacciGuard, the most useful interpretation is:

- a sensor sends a temperature reading
- Kafka receives it
- the stream processor classifies it as `SAFE`, `LOW_BREACH`, or `HIGH_BREACH`
- Redis is updated with the latest device state
- all of this should happen in under `5 seconds` for the live alert path under normal load

This is the important operational path. If a vaccine storage breach is detected after `20` or `40` seconds, the system is still useful for demos and analytics, but it is not truly near-real-time for alerting.

## What We Learned From The Fresh AWS Run
The baseline was tested in AWS using the agreed normal-load methodology:

- workload family: `main/normal-load`
- replay rate: `100 events/second`
- duration: `8 minutes`

The fresh run showed:

- the stream pipeline worked end to end
- Redis writes worked
- S3 writes worked
- CloudWatch metrics worked
- Kafka lag ended at `0`
- the batch processor later produced valid facility and district outputs

So the baseline pipeline is functionally working.

However, the latency requirement is still not met.

### Important Metric Distinction
The baseline originally exposed a latency metric based on:

- `processing_time - event_time_ts`

That number appeared in CloudWatch and was about `41 seconds`.

That is not the best metric for the professor's alerting SLA, because `event_time_ts` is the business timestamp stored inside the workload payload. It is not the same thing as "when Kafka actually received the message."

The better live-path latency metric is:

- `processing_time - kafka_timestamp`

That is closer to the real operational question:

- once Kafka has the event, how long until the pipeline finishes processing it?

When we measured the real latency from processed parquet in S3, we found:

- row count: `33,782`
- average true latency: about `20.82 seconds`
- p50 latency: `13 seconds`
- p95 latency: `51 seconds`
- p99 latency: `54 seconds`
- max latency: `56 seconds`

This means the baseline is better than the misleading `41s` metric suggested, but it is still far above the `<5s` requirement.

## What The Baseline Is Doing Wrong
The baseline is not "broken" in the sense of crashing. The real issue is that the hot path is doing too much work in one place and forcing the alert path to behave like a heavier micro-batch job.

### 1. The Alert Path And Historical Path Are Coupled Together
Right now, the baseline stream processor handles the live alert path and the historical archive path inside the same `foreachBatch` flow in [job.py](/Users/tanishgupta/Downloads/vacciguard/services/stream-processor/job.py).

The batch callback does all of this in one chain:

- validate incoming rows
- deduplicate rows
- enrich with the lookup table
- classify breaches
- write latest device state to Redis
- write processed historical output to S3 parquet
- write invalid rows to a separate S3 audit path

This is the main architectural problem.

Why this hurts latency:

- the professor only cares that the live alert path reaches Redis quickly
- but the current code treats a micro-batch as "done" only after Redis and S3 work both happen
- S3 writes are slower and heavier than Redis writes
- because they happen in the same `foreachBatch`, the Redis path waits inside a larger unit of work

In simple words:

- the pipeline is making the fast path carry the slow path on its back

### 2. The Baseline Still Behaves Like A Real Micro-Batch System
The trigger interval was reduced from `30s` to `5s`, which was the correct baseline fix. But even with a `5s` trigger, the actual end-to-end delay stayed much higher than `5s`.

Why:

- Spark waits for the next trigger window
- then it schedules a micro-batch
- then it runs the whole processing chain
- then it writes Redis and S3
- then it starts the next cycle

Even when the batch execution itself became fast, the whole cycle still behaved like a queue.

This is why we saw a surprising result:

- many batch durations were below `1 second`
- but real Kafka-to-processing latency still averaged about `20.82 seconds`

That means the problem is not just "Spark is slow." The problem is:

- records still spend too much time waiting for the whole batch cycle to complete

### 3. The Metric Used For Monitoring Was Not The Best SLA Metric
The baseline histogram records:

- `processing_time - event_time_ts`

This is useful as a business freshness metric, but it is not the same as the true alert-path SLA.

That created confusion:

- CloudWatch showed around `41s`
- the real Kafka-to-processing latency was lower
- but still not low enough

So the baseline did not just have a performance problem. It also had a measurement problem.

### 4. One Query Is Trying To Serve Two Different Goals
The current baseline query is trying to satisfy both:

- live operations
- historical analytics

These goals are not identical.

The live operations path wants:

- fast decisions
- small writes
- low waiting time

The historical path wants:

- durable storage
- partitioned parquet
- audit outputs
- completeness over speed

Trying to optimize both in the same query is exactly what makes the baseline miss the `<5s` target.

### 5. The Baseline Is Designed To Be A Fair Reference, Not A Low-Latency Champion
This point matters for the report.

The baseline is intentionally a simpler, fixed-capacity deployment. It is supposed to work correctly and provide a fair comparison point for the optimized version.

So the honest conclusion is not:

- "the baseline is useless"

The honest conclusion is:

- "the baseline is correct and stable, but its current hot-path design is not lean enough to satisfy the strict near-real-time SLA"

## What The Optimized Pipeline Should Do
The optimized pipeline should not change the business logic. It should change how the work is arranged.

That is very important for a fair baseline-versus-optimized comparison.

The same business logic should stay the same:

- schema parsing
- validation rules
- deduplication intent
- lookup enrichment
- breach classification rules
- Redis device status structure
- historical parquet output structure

What should change is the execution design around that logic.

### Principle 1: Make The Hot Path Extremely Small
The optimized live path should do only the work required to produce a useful alert and current state.

That means the hot path should be:

- read from Kafka
- parse the event
- validate it
- deduplicate it
- enrich it with the lookup table
- classify the breach
- write the current state to Redis
- publish alert-path metrics

That is the true near-real-time path.

Everything else should be treated as secondary.

### Principle 2: Do Not Make Redis Wait For S3
This is the single biggest design change.

In the optimized version:

- Redis updates should complete independently of the historical S3 write
- S3 archival should happen in a separate query, separate job, or asynchronous downstream path

This lets the alert path finish quickly even if S3 is slower.

In simple words:

- live alerting should not stand in line behind archive writing

### Principle 3: Measure The SLA At The Right Point
The optimized design should report the latency metric the professor actually cares about:

- `redis_write_time - kafka_timestamp`

This is better than:

- `processing_time - event_time_ts`

because it measures the operational path directly.

If we later want a business freshness metric too, we can keep both:

- `business_freshness_seconds = processing_time - event_time_ts`
- `alert_latency_seconds = redis_write_time - kafka_timestamp`

That would remove ambiguity in the evaluation.

### Principle 4: Keep Historical Writing Durable But Less Urgent
The optimized design still needs S3 output. We are not removing it.

We are only changing its urgency.

Historical output can be:

- a second Spark query
- a second service reading from Kafka
- or a downstream fan-out path

Its job is:

- append processed telemetry parquet
- store invalid records for audit
- preserve historical analytics inputs

This path can tolerate a slightly larger trigger and slightly more latency, because it is not the immediate alert path.

### Principle 5: Scale The Right Layer
If we need more compute, we should scale the actual event-processing layer, not just add complexity everywhere.

For the optimized version, scaling tools can help:

- Spark dynamic allocation
- more Spark workers
- KEDA or HPA for stateless components where appropriate

But scaling alone will not fix the SLA if the design still couples Redis and S3 tightly. Architecture first, scaling second.

## Exact Code And Design Changes To Target `<5s`
This section is the most practical one. It describes what we should change in code and deployment.

### Change 1: Split The Current `foreachBatch` Into Two Paths
Right now the main `process_batch` function in [job.py](/Users/tanishgupta/Downloads/vacciguard/services/stream-processor/job.py) does both Redis and S3 work.

We should split this into:

- `hot path` for Redis and alert metrics
- `archive path` for S3 processed output and invalid audit output

There are two reasonable ways to implement that.

#### Option A: Two Spark Queries From The Same Parsed Stream
The cleaner optimized design is:

- Query 1:
  - handles valid rows
  - deduplicates
  - enriches
  - classifies
  - writes to Redis
  - emits alert-path metrics
  - uses a small trigger such as `1s`

- Query 2:
  - writes processed telemetry to S3 parquet
  - writes invalid rows to S3 audit
  - uses a larger trigger such as `10s` or `30s`

This gives the live path and archive path separate pacing.

#### Option B: Two Services
An even stronger separation is:

- service 1: live alert processor to Redis
- service 2: historical writer to S3

This is more complex operationally, but it gives the clearest separation of concerns.

For this project, Option A is likely the best next step because:

- it keeps the same core codebase
- it is easier to compare with the baseline
- it does not force a completely new architecture

### Change 2: Introduce A Dedicated Redis Alert Latency Metric
Right now the code exposes a histogram named:

- `vacciguard_e2e_latency_seconds`

and defines it as:

- `event_time -> processing_time`

For the optimized version, we should add a new metric such as:

- `vacciguard_alert_latency_seconds`

Definition:

- `redis_write_time - kafka_timestamp`

This is the real alert-path SLA.

Recommended metrics for the optimized version:

- `vacciguard_alert_latency_seconds`
- `vacciguard_business_freshness_seconds`
- `vacciguard_redis_write_duration_seconds`
- `vacciguard_archive_write_duration_seconds`

This lets us answer different questions clearly:

- how quickly did the alert path finish?
- how fresh was the business event timestamp?
- how expensive were Redis writes?
- how expensive were S3 writes?

### Change 3: Lower The Hot-Path Trigger Interval Further
The baseline improved with a `5s` trigger, but that is still borderline for a `<5s` SLA.

For the optimized alert path, we should target:

- `1 second` trigger

Why:

- if the trigger itself is `5s`, the system already spends a large part of the SLA budget just waiting for the next batch
- a `1s` trigger gives the hot path a realistic chance to stay under `5s`

Important caution:

- lowering the trigger without splitting S3 away can make the system thrash
- so this change should happen together with the hot-path split

### Change 4: Stop Using The Same "Done" Point For Everything
In the baseline, it is easy to think a record is only "complete" after:

- Redis is updated
- S3 is written
- metrics are updated

For the optimized version, we should define completion more clearly.

For the live SLA:

- a record is complete when Redis is updated and the alert metric is emitted

For historical durability:

- a record is complete when the archive path writes parquet to S3

This conceptual separation matters because it changes how we design and measure the system.

### Change 5: Make Invalid Handling Non-Blocking For The Hot Path
Invalid rows should still be preserved for audit, but their handling should not slow down the alert path.

That means:

- hot path validates rows quickly
- invalid rows are tagged immediately
- invalid rows are sent to the archive path for S3 audit writing
- valid rows continue toward Redis without waiting for the invalid write to finish

This keeps correctness while protecting latency.

### Change 6: Keep Lookup Enrichment Fast And Local
The lookup table is already loaded as a broadcast DataFrame in the current design. That is the right idea and should be kept in the optimized version.

Why it helps:

- no heavy shuffle join on every micro-batch
- fast access to facility thresholds
- predictable classification cost

This is one area where the baseline design is already doing the right thing.

### Change 7: Tune Partitions And Executor Resources For The Hot Path
Once the alert path is separated, tuning becomes more meaningful.

Recommended optimized tuning areas:

- lower shuffle overhead
- size executors for low-latency work rather than mixed Redis-plus-S3 work
- allow dynamic allocation in the optimized deployment
- ensure enough executor slots exist to avoid queueing during normal load and short spikes

These are deployment-level changes, not business-logic changes.

### Change 8: Preserve Fair Comparison By Keeping The Same Business Outputs
The optimized version should not "cheat" by weakening the work.

The following should remain the same between baseline and optimized:

- same input workload
- same validation semantics
- same dedup semantics
- same lookup logic
- same breach thresholds
- same Redis data meaning
- same processed parquet schema

Otherwise, the optimized version would no longer be a fair improvement of the same system.

## A Practical Before-And-After Picture
This is the simplest way to explain the redesign.

### Baseline Today
One micro-batch does:

- Kafka read
- validate
- dedup
- enrich
- classify
- Redis write
- S3 processed write
- S3 invalid write
- metrics update

This is simple and correct, but too heavy for strict alert latency.

### Optimized Target
The same event should effectively flow through two lanes.

Lane 1: live alert lane

- Kafka read
- validate
- dedup
- enrich
- classify
- Redis write
- alert latency metric

Lane 2: historical archive lane

- same processed result
- processed parquet to S3
- invalid rows to S3 audit
- batch analytics inputs preserved

This is still the same business pipeline, but the fast lane is no longer blocked by the archive lane.

## Suggested Implementation Sequence
The safest way to do this is in steps.

### Step 1
Add the correct alert-path metric without changing the architecture yet.

Goal:

- expose `kafka_timestamp -> Redis write completion`

This gives a trustworthy SLA metric.

### Step 2
Split Redis and S3 work into separate queries or separate logical stages.

Goal:

- make Redis stop waiting on S3

### Step 3
Reduce the hot-path trigger from `5s` to `1s`.

Goal:

- reduce scheduling wait in the alert path

### Step 4
Tune optimized deployment resources and scaling.

Goal:

- keep the fast path from falling behind during normal load and bursts

### Step 5
Compare baseline and optimized runs with the same `8-minute @ 100 eps` workload.

Goal:

- show whether the redesign actually improved:
  - average alert latency
  - p95 alert latency
  - p99 alert latency
  - lag behavior
  - recovery behavior

## What We Can Honestly Claim Today
Based on the current baseline evidence:

- the baseline pipeline works end to end
- it is stable under the agreed normal-load run
- it writes to Redis, S3, and CloudWatch correctly
- it produces valid batch outputs for facilities and districts
- but it does not yet satisfy the `<5s` alert latency requirement

The fairest explanation is:

- the baseline keeps correctness and simplicity
- the optimized version must make the hot path thinner and more independent

## Bottom Line
The baseline is not failing because the business logic is wrong.

It is missing the `<5s` target because:

- the live alert path is still bundled together with slower historical work
- the system still behaves like a real micro-batch pipeline
- the original latency metric confused business freshness with operational alert latency

The optimized pipeline should improve latency by changing execution structure, not by changing the meaning of the work.

The single most important design move is:

- separate the Redis alert path from the S3 historical archive path

If we do that, add the correct alert metric, and lower the hot-path trigger, we have a realistic path toward the professor's near-real-time requirement.
