# Aayush Execution Guide

## Role Summary
Aayush owns the processing and data-quality side of VacciGuard. His work turns Alok's raw inputs
into correct, meaningful, and measurable outputs for both the real-time and batch paths.

He is responsible for building:
- the Spark Structured Streaming job for the live path
- the Spark batch job for daily analytics and reporting
- validation logic
- deduplication logic
- enrichment with lookup data
- breach detection logic
- the processing outputs that feed Redis, S3, evaluation, and reporting

The most important risk in his area is this:

- if the processing logic is inconsistent between the baseline and optimized pipelines, the
  comparison becomes academically weak
- if validation and deduplication are unclear, the data-quality requirement is not satisfied
- if the stream and batch jobs are designed separately with different assumptions, the "unified
  pipeline" goal is weakened

Because of that, Aayush should build one clear processing contract and reuse as much transformation
logic as possible across both Spark jobs.

## Main Objective
Build the stream and batch processing layer so the pipeline can:
- validate input data
- remove or suppress duplicates
- enrich device events with facility context
- detect unsafe temperature breaches
- update hot operational state
- write historical analytical outputs
- produce results that can be compared fairly between baseline and optimized deployments

## How Aayush Should Think About His Role
Aayush is not building "just a Spark job." He is building the logical heart of the pipeline.

That means his code decides:
- which incoming records are considered valid
- how duplicate events are handled
- how raw telemetry becomes facility-aware information
- how breach conditions are defined
- what the rest of the team sees as the "truth" of the processed pipeline output

In practical terms:
- Alok sends raw events and supporting datasets
- Aayush turns them into correct processed outputs
- Monty runs those outputs reliably on AWS
- Tanish uses the outputs to compare baseline versus optimized behavior

So if Aayush's logic is unclear or inconsistent, the whole project becomes difficult to defend.

## What Aayush Must Deliver
- a Spark Structured Streaming application for live telemetry
- a Spark batch application for daily or scheduled analytics
- a shared processing logic design across stream and batch paths
- validation rules for all required input fields
- deduplication logic using `event_id`
- enrichment logic using the frozen lookup dataset
- breach classification logic using `min_temp_c` and `max_temp_c`
- streaming outputs written to `ElastiCache Redis` and `S3/Parquet`
- batch outputs written to `S3/Parquet`
- processing-side metrics needed for SLA detection and evaluation
- simple smoke-test instructions that others can follow
- a short handoff note explaining what outputs Monty and Tanish should expect

## How Other Members Use Aayush's Work
- **Tanish** uses Aayush's outputs to verify the full pipeline is integrated and to compare baseline versus optimized behavior fairly
- **Alok** uses Aayush's feedback to confirm the generated schema is valid and useful for processing
- **Monty** deploys Aayush's stream and batch jobs on AWS, configures their runtime settings, and tests scaling and failure behavior around them

## Folders Aayush Should Use
- [services/stream-processor/](/Users/tanishgupta/Desktop/vacciguard/services/stream-processor): main code for the Spark Structured Streaming application
- [services/batch-processor/](/Users/tanishgupta/Desktop/vacciguard/services/batch-processor): main code for the Spark batch application
- [tests/smoke/](/Users/tanishgupta/Desktop/vacciguard/tests/smoke): small end-to-end checks for processing logic
- [tests/workload/](/Users/tanishgupta/Desktop/vacciguard/tests/workload): notes or checks related to how the processing behaves under replay workloads
- [scripts/](/Users/tanishgupta/Desktop/vacciguard/scripts): helper utilities that are shared across both processor services

These are the main input folders Aayush will consume from:
- [data/reference/](/Users/tanishgupta/Desktop/vacciguard/data/reference)
- [data/batch/](/Users/tanishgupta/Desktop/vacciguard/data/batch)
- [data/workloads/dev/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/dev)
- [data/workloads/main/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/main)

## Baseline Coordination Rules
For the shared dependency flow, Aayush should also read:
- [baseline-dependency-and-coordination-plan.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/baseline-dependency-and-coordination-plan.md)

### When Aayush can start
Aayush can start immediately because the schema is frozen and sample files already exist.

### When Aayush must inform others
Aayush should stop and inform the team at these baseline checkpoints:
- after the stream and batch processing contract is clearly defined
- after stream smoke behavior works on sample data
- after the sink behavior for Redis and S3 is clear
- after one batch smoke run works correctly

He should inform:
- **Alok**, so input assumptions can be confirmed or corrected early
- **Monty**, so deployment and runtime expectations are clear
- **Tanish**, so the baseline business logic can be treated as fixed

### When Aayush's baseline task is complete
For the baseline pipeline, Aayush's work is complete when:
- stream processing logic works on the frozen schema
- validation, deduplication, enrichment, and breach logic are stable
- Redis and S3 sink behavior is clear and tested
- one useful batch path works correctly
- the same business logic is ready to be used unchanged in the baseline deployment

## Inputs Aayush Must Treat As Fixed
Aayush should treat the following document as the source of truth for all input fields:
- [input-schema-freeze.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/01-foundation/input-schema-freeze.md)

He should use these starter files when beginning implementation:
- [live-telemetry-sample.ndjson](/Users/tanishgupta/Desktop/vacciguard/data/workloads/dev/live-telemetry-sample.ndjson)
- [device-facility-lookup-template.csv](/Users/tanishgupta/Desktop/vacciguard/data/reference/device-facility-lookup-template.csv)
- [daily-operations-log-template.csv](/Users/tanishgupta/Desktop/vacciguard/data/batch/daily-operations-log-template.csv)

## Processing Outputs Aayush Must Produce

### Streaming outputs
- **current device state** for fast operational access in `ElastiCache Redis`
- **processed telemetry history** in `S3` using `Parquet`
- **breach classification results** included in the processed stream output

### Batch outputs
- **daily or period summaries** in `S3` using `Parquet`
- **compliance-oriented analytical outputs** that combine historical telemetry, lookup data, and batch logs

### Monitoring outputs
Aayush should also expose processing-side metrics that the monitoring stack can use later for SLA
violation detection.

These are not the same as business outputs like breach records.

At minimum, Aayush should plan to expose:
- **end-to-end latency** derived from processing time versus `event_time`
- **processed-record count** or processed-record rate
- **invalid-record count** or invalid-record ratio
- **failed micro-batch count** if feasible
- **breach count** for dashboards, even though breach alerts are not themselves SLA violations

Important rule:
- Aayush is responsible for exposing the signals
- Monty is responsible for turning those signals into Prometheus, Grafana, or CloudWatch alert rules
- Tanish is responsible for freezing the thresholds used in evaluation

### Important rule
The baseline and optimized pipelines should use the same processing logic and the same output
meaning. Only deployment or scaling settings should differ later.

## Core Concepts Aayush Must Understand Before Building

### 1. Continuous processing does not mean every sink behaves the same way
The live path is continuous because events keep arriving and Spark keeps processing them. But that
does not mean every output system should be written in the same style.

For VacciGuard:
- `ElastiCache Redis` is used for current operational state
- `S3` is used for historical analytical storage

These two sinks have different jobs, so Aayush should not treat them the same way.

### 2. Redis is for latest state, not history
Redis should hold only the most recent and operationally useful information, such as:
- the latest status for a device
- the latest known breach state
- fast lookup data for dashboards or operational views

Redis is not where Aayush should store every processed event forever.

### 3. S3 is for analytical history, not per-event operational writes
S3 is object storage. It is good for durable file-based historical storage and later analytics.

That means the stream job should write to S3 in **micro-batches**, not one record at a time.

Why:
- writing one record at a time to S3 creates too many tiny files
- too many tiny files make analytics slower and storage management messier
- Spark Structured Streaming is designed to process continuously while still writing file outputs in batches

So the correct mental model is:
- events arrive continuously
- Spark processes continuously
- Spark writes historical results to S3 every fixed trigger interval as a micro-batch of files

### 4. The stream job and the batch job have different purposes
The stream job answers:
- what is happening right now?
- is there a breach right now?
- what is the current latest state of each device?

The batch job answers:
- what happened over the day or over a reporting period?
- which facilities had repeated issues?
- how do operational logs relate to historical breach behavior?

So the stream job is about near-real-time operational value, while the batch job is about deeper
historical analysis.

### 5. Baseline processing logic must stay stable
Aayush should think of the baseline pipeline as:
- logically correct
- fixed-capacity
- intentionally simple

That means the baseline can still have:
- validation
- deduplication
- enrichment
- breach logic
- Redis writes
- S3 writes

But the baseline should not depend on:
- Spark dynamic allocation
- different business rules
- special optimized logic that only exists in one version

This is what keeps the comparison fair later.

### 6. Business alerts and SLA violations are different
Aayush should keep these two ideas separate in his implementation:

- **business alerts**
  - example: a device temperature is outside the allowed threshold
  - handled inside stream-processing business logic
  - represented by fields like `breach_status` and `breach_type`

- **SLA violations**
  - example: the pipeline is too slow, falling behind, or recovering too slowly
  - not decided by the breach logic itself
  - detected later through monitoring rules built on top of processing metrics

This matters because the project can have correct breach detection and still violate platform-level
SLA expectations under load or failure.

## Detailed Step-by-Step Approach

### Step 1: Read and accept the frozen input contract
Before writing Spark code, Aayush should study the frozen schema document and confirm he understands:
- which fields are required
- which fields are optional
- which keys are used for joins
- which fields can be used for validation and breach logic

At this stage, he should not change field names on his own.

He should identify:
- the stream join key: `device_id`
- the facility join key: `facility_id`
- the event uniqueness key: `event_id`
- the threshold fields: `min_temp_c` and `max_temp_c`

Why this step matters:
- processing code must match Alok's generated data exactly
- schema drift at this point will slow down the whole team

### Step 2: Define the processing contract before writing jobs
Before implementing the stream or batch job, Aayush should write down what the processing layer is
supposed to do in order.

He should think of this as defining the pipeline's business meaning before defining code modules.
If this order is unclear, implementation becomes messy very quickly because validation,
deduplication, enrichment, and breach logic start getting mixed together.

The recommended processing order for live telemetry is:
1. read event records
2. parse schema
3. validate required fields
4. filter or tag invalid records
5. deduplicate using `event_id`
6. enrich with lookup data
7. classify breach status using temperature thresholds
8. write current-state output to Redis
9. write historical processed output to `S3/Parquet`

The recommended processing order for batch logs is:
1. read batch log file
2. validate schema and required columns
3. join batch logs with lookup data
4. join batch logs with historical telemetry outputs if needed
5. compute summaries or analytical views
6. write batch outputs to `S3/Parquet`

Why this step matters:
- the team needs a stable processing story
- Monty needs predictable runtime behavior to deploy
- Tanish needs a clean methodology for the report

What Aayush should produce at the end of this step:
- one written sequence for the stream path
- one written sequence for the batch path
- a note about which parts are shared across both

### Step 3: Create a shared transformation design for stream and batch
Even though there are two Spark jobs, Aayush should avoid building two unrelated logic stacks.

This does not mean both jobs will look identical. It means they should agree on the same business
rules wherever the rules overlap.

He should identify which logic is shared:
- schema validation rules
- lookup-data loading logic
- breach-threshold logic
- field naming conventions
- output naming conventions

He should then structure the code so these common rules are reused as much as practical across:
- [services/stream-processor/](/Users/tanishgupta/Desktop/vacciguard/services/stream-processor)
- [services/batch-processor/](/Users/tanishgupta/Desktop/vacciguard/services/batch-processor)

Why this step matters:
- this supports the project's unified processing goal
- it reduces inconsistent logic between the stream and batch paths

Simple test for this step:
- if the same device reading is considered a breach in the stream path, the batch analytics should
  not later treat the same threshold rules differently

### Step 4: Implement validation logic first
Validation should be built before enrichment or breach detection.

This order matters because enrichment and breach logic only make sense when the incoming record has
already passed the minimum structural checks. If Aayush skips validation and starts with joins or
threshold logic, later errors become harder to understand.

For live telemetry, Aayush should validate:
- `event_id` is present
- `device_id` is present
- `event_time` is parseable
- `temperature_c` is present and numeric
- `door_open` is parseable as boolean
- `battery_pct` is numeric and within `0` to `100`

For lookup data, he should validate:
- `device_id` is unique
- `facility_id` is present
- `min_temp_c < max_temp_c`

For batch logs, he should validate:
- `facility_id` is present
- `log_date` is valid
- numeric fields are not negative
- boolean flags are parseable

Important design choice:
- invalid records should not silently disappear without trace
- Aayush should either filter them with clear counts or route them to an identifiable invalid-record output later

Why this step matters:
- data quality is one of the professor's explicit non-functional requirements

### Step 5: Implement deduplication logic for the live path
Deduplication is required because the project plans to test duplicate-event scenarios and recovery.

This step is especially important for a streaming system because retries, replays, and failure
recovery can all cause the same logical event to appear more than once.

Aayush should deduplicate the live stream using:
- `event_id` as the primary event identity

He should design this carefully so that:
- normal events are processed once
- duplicate replay or retry events do not create incorrect repeated state updates

At this stage, the practical project goal is:
- **effectively-once end-to-end behavior**

That means:
- Kafka keeps events durably
- Spark checkpoints preserve progress
- deduplication suppresses harmful duplicate processing

Why this step matters:
- this is how the project satisfies the "exactly-once or effectively-once" objective in a realistic student-safe way

What Aayush should keep in mind:
- the goal is not to prove perfect theoretical exactly-once behavior everywhere
- the goal is to make the end-to-end result correct enough that duplicates do not corrupt state or analytics

### Step 6: Implement lookup-data enrichment
Once validation is in place, Aayush should join live telemetry with the lookup dataset.

This step is what turns the project from a generic sensor pipeline into a meaningful cold-chain
monitoring pipeline.

This enrichment should add:
- `facility_id`
- `facility_name`
- `district`
- `state`
- `min_temp_c`
- `max_temp_c`
- optional `storage_type`

After enrichment, the stream should no longer be just raw device events. It should become
facility-aware operational data.

Why this step matters:
- breach decisions require threshold fields
- reporting requires district and state context
- this is how the stream path demonstrates joins and enrichment

Without this step, the pipeline could only say:
- `device FR-0102 had temperature 9.1`

After this step, the pipeline can say:
- `facility FAC-0002 in Udaipur had a temperature above the allowed threshold`

### Step 7: Implement breach detection logic
After enrichment, Aayush should classify whether each event is safe or unsafe.

This classification should be simple and explicit. It should not depend on hidden assumptions or
hard-coded values scattered across the codebase. The threshold values should come from the lookup
data so the rule is transparent and easy to explain.

Recommended logic:
- if `temperature_c < min_temp_c`, mark as low breach
- if `temperature_c > max_temp_c`, mark as high breach
- otherwise mark as safe

He may represent this with fields such as:
- `breach_status`
- `breach_type`

Example meanings:
- `SAFE`
- `LOW_BREACH`
- `HIGH_BREACH`

Why this step matters:
- breach classification is the central domain behavior of the project
- it also gives the team something meaningful to measure during evaluation

### Step 8: Design the streaming sink outputs carefully
The live Spark job should not write everything everywhere. Aayush should keep the sink logic simple
and purposeful.

Recommended sink behavior:
- write only the latest or operationally relevant state to `ElastiCache Redis`
- write processed historical records to `S3` in `Parquet`

#### Why Redis and S3 must be treated differently
These sinks solve different problems:

- `Redis` answers: what is the latest current state right now?
- `S3` answers: what processed history should be stored for later analytics?

Because of that:
- Redis can be updated in a more state-oriented way
- S3 should be written as file output in micro-batches

#### Why S3 should not be written one record at a time
Aayush should not design the S3 sink as if it were an operational row store.

S3 is object storage, so one-record-at-a-time writes are a poor fit because they:
- create too many tiny files
- add unnecessary overhead
- make downstream analytics harder

The correct baseline behavior is:
- Spark Structured Streaming runs continuously
- Spark collects records into micro-batches based on a fixed trigger interval
- each micro-batch is written to S3 as Parquet files

This means the system is still near-real-time, but the historical sink is file-oriented and
efficient.

#### How Aayush should think about the baseline S3 sink
The baseline live path should behave like this:
1. Kafka receives events continuously
2. Spark reads and processes them continuously
3. Redis gets the latest operational state
4. S3 receives processed historical output every fixed trigger interval

So the stream is continuous, but the S3 write pattern is batched.

#### What Aayush should decide for the baseline
He should choose a fixed baseline micro-batch trigger and keep it stable.

Examples of reasonable baseline trigger intervals:
- `10 seconds`
- `15 seconds`
- `30 seconds`

The exact value can be finalized with Monty during deployment, but the important rule is:
- baseline uses a fixed trigger
- optimized may later tune processing settings, but business logic stays the same

For the Redis side, the output should support:
- current status by device
- current breach status
- fast operational lookups

For the S3 side, the output should support:
- historical analysis
- batch joins later
- compliance reporting

Why this step matters:
- this preserves the hot-versus-cold storage design
- it keeps Redis focused on fast operational state rather than long-term storage

At the end of this step, Aayush should be able to explain:
- what goes to Redis
- what goes to S3
- why the same processed event is not written in the same way to both sinks

### Step 8A: Expose the metrics needed for SLA detection
Once the streaming outputs are designed, Aayush should make the processing layer observable enough
for SLA-oriented evaluation. This is part of the implementation, not a cosmetic extra.

The key idea is:
- Spark processing emits the important health signals
- Monty connects those signals to dashboards and alert rules
- Tanish later uses the exact same signals for baseline versus optimized analysis

The most important metric is end-to-end latency.

A practical implementation approach is:
1. preserve `event_time` from the incoming event
2. capture a processing timestamp when the record or micro-batch is handled
3. compute latency as `processing_timestamp - event_time`
4. aggregate that into:
   - average latency
   - p95 latency
   - p99 latency if feasible

In addition, Aayush should expose:
- processed-record count over time
- invalid-record count over time
- duplicate-suppression count if feasible
- failed-micro-batch count if feasible

Why these matter:
- latency supports SLA-1
- processed-record progress supports stalled-processing detection
- invalid-record metrics support quality and trust analysis
- failed-micro-batch metrics support stability analysis

Important implementation rule:
- Aayush does not need to decide the final alert thresholds here
- he needs to make sure the processing layer surfaces the signals cleanly and consistently

### Step 9: Implement the batch-processing job
Once the stream path is clear, Aayush should build the batch path using Spark batch.

He should think of the batch job as a separate analytical lens over the same project, not as a
replacement for the stream job.

The batch job should:
- read daily operations or maintenance logs
- validate the batch schema
- join batch logs with lookup data
- optionally join with processed telemetry history from S3
- compute simple analytical outputs

Examples of valid batch outputs:
- facility-level daily summaries
- district-level compliance summaries
- counts of breaches versus outage conditions

Important rule:
- Airflow should schedule this batch job later
- Airflow should **not** be treated as the lifecycle manager of the long-running stream job

Why this step matters:
- the professor explicitly wants both stream and batch analytics

### Step 10: Keep stream and batch logic aligned
After both jobs exist, Aayush should check for alignment.

This is a quality-control step. The goal is to verify that the project still behaves like one
unified pipeline instead of two disconnected mini-projects.

He should confirm:
- shared field names are consistent
- lookup joins behave the same way
- breach classification means the same thing in both paths
- output naming stays understandable

He should avoid situations where:
- the stream path says one thing is a breach
- the batch path uses different assumptions and says something else

Why this step matters:
- inconsistent logic makes the report and demo weaker

### Step 11: Prepare the stream job for long-running deployment
The stream job should be designed as a long-running Spark application on Kubernetes or EKS.

This is important because the stream job is part of the always-on pipeline path. It should not be
written with the mindset of "run once, finish, and exit" like a daily batch script.

Aayush should therefore:
- avoid assuming it is launched fresh for every event batch
- keep checkpointing in mind
- keep configuration external where practical
- avoid building the stream path as an Airflow-only scheduled task

Why this step matters:
- the live pipeline is continuous
- Monty will deploy it as a long-running application, not as a daily batch DAG

What this means in practice:
- checkpoint locations matter
- restart behavior matters
- output paths must be stable
- the job should resume processing rather than pretending every run is brand new

### Step 12: Create small smoke tests before full-scale workload runs
Before the team uses the main workload, Aayush should verify the processing logic on small samples.

This step exists so the team can debug logic in minutes instead of debugging a large distributed
run for hours.

He should test:
- one safe event
- one high breach event
- one duplicate event
- one invalid event
- one late-arrival example if the logic handles it
- one batch log sample

These checks should be stored or documented under:
- [tests/smoke/](/Users/tanishgupta/Desktop/vacciguard/tests/smoke)

Why this step matters:
- it prevents the team from debugging huge workloads when simple logic is still broken

### Step 13: Define handoff expectations for Monty
Before deployment starts, Aayush should tell Monty:
- how to run the stream job
- how to run the batch job
- what configuration the jobs need
- what outputs should appear in Redis and S3
- what logs or metrics indicate healthy processing
- what processing metrics are available for SLA detection

Why this step matters:
- deployment gets much easier when runtime expectations are explicit

A useful handoff here should include:
- expected input paths
- expected output paths
- whether a checkpoint path is required
- what "healthy processing" looks like in logs or counts
- how end-to-end latency is computed
- which counters should keep increasing during healthy processing
- which counters indicate invalid-record or failed-batch behavior

### Step 14: Define handoff expectations for Tanish
Before evaluation starts, Aayush should tell Tanish:
- what output datasets the pipeline produces
- what counts or summaries should be checked after a run
- what correctness indicators should be compared between baseline and optimized
- what processing metrics should be used when discussing SLA behavior

Why this step matters:
- the final comparison should not look only at speed and cost
- it should also confirm that processing correctness is preserved

A useful handoff here should include:
- what output files or tables should exist after a successful run
- what record counts are expected approximately
- what breach indicators should appear in sample cases
- what latency and progress metrics should be visible in dashboards
- what values would suggest the pipeline is falling behind or misbehaving

### Step 15: Protect comparison fairness
This is one of Aayush's most important responsibilities.

He should make sure that:
- baseline and optimized pipelines use the same Spark business logic
- the same input schema is used for both
- the same validation, deduplication, enrichment, and breach rules are used for both

Allowed to change later:
- executor counts
- Spark resource settings
- scaling settings
- checkpoint or deployment tuning if carefully documented

Not allowed to change between baseline and optimized:
- business rules
- event meaning
- breach logic
- join logic
- output meaning

Why this step matters:
- changing the logic would make the comparison invalid

## Practical Build Sequence
If Aayush wants the shortest path, he should complete the work in this order:

1. Read the frozen input schema
2. Confirm the join keys and required fields
3. Define the stream and batch processing order
4. Build validation logic
5. Build deduplication logic
6. Build lookup enrichment logic
7. Build breach detection logic
8. Write the stream outputs to Redis and S3
9. Build the batch job
10. Align shared rules across both jobs
11. Add small smoke tests
12. Hand off runtime expectations to Monty
13. Hand off correctness expectations to Tanish
14. Keep the same logic in both baseline and optimized runs

## Done Criteria
Aayush's task should be considered complete only when all of the following are true:
- the stream processor can read the frozen event schema correctly
- validation logic exists for stream, lookup, and batch inputs
- duplicate live events are handled using `event_id`
- enrichment with lookup data works correctly
- breach classification works correctly
- current operational state can be written to Redis
- processed historical outputs can be written to `S3/Parquet`
- the batch processor can produce at least one useful analytical output
- small smoke tests exist for the main processing cases
- Monty knows how to deploy the jobs
- Tanish knows what outputs should be checked during evaluation
- the processing layer exposes the metrics needed for SLA-oriented evaluation

## Common Mistakes To Avoid
- building different business logic for baseline and optimized pipelines
- treating Airflow as the controller for the always-on stream job
- performing breach detection before enrichment has provided thresholds
- dropping invalid records without tracking counts or behavior
- exposing no processing-health metrics and expecting Monty to invent SLA detection later
- using inconsistent join keys across datasets
- writing all processed history into Redis instead of keeping it in S3
- making stream and batch jobs use different meanings for the same field
- treating S3 like a one-record-at-a-time operational sink instead of a micro-batch historical sink

## One-Sentence Summary
Aayush's job is to turn frozen input data into clean, enriched, deduplicated, breach-aware outputs
using one clear Spark-based processing design that stays logically identical across the baseline and
optimized pipelines.
