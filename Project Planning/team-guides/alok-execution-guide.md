# Alok Execution Guide

## Role Summary
Alok owns the data-source and ingestion side of VacciGuard. His work must ensure that the
project has realistic, repeatable, and scalable inputs for both the baseline pipeline and the
optimized pipeline.

The most important risk in his area is the simulator bottleneck problem:

- if the simulator cannot generate enough traffic, the team cannot prove a true 10x spike
- if the dataset changes every run, the baseline and optimized pipelines cannot be compared fairly
- if the event schema is inconsistent, Aayush cannot build stable Spark logic and Monty cannot
  run reliable deployment tests

Because of that, Alok should not rely only on a random live generator. He should build a
precomputed workload system plus a replay producer that can publish data to Kafka at controlled
rates.

## Main Objective
Build the full input side of the project so the rest of the team can develop, deploy, and
evaluate the pipeline using fixed and repeatable workloads.

## What Alok Must Deliver
- precomputed synthetic datasets for multiple workload types
- a replay producer that publishes those datasets into Kafka
- configurable rate control for normal load and burst load
- support for parallel producer instances if one producer is not enough
- clear schema documentation for all generated events
- sample lookup data and batch logs
- a short handoff note telling Aayush and Monty how to use the generated inputs

## How Other Members Use Alok's Work
- **Tanish** uses Alok's datasets and replay plan to keep the baseline and optimized experiments fair
- **Aayush** uses Alok's event schema, lookup data, and batch files to build Spark validation, enrichment, deduplication, and breach logic
- **Monty** uses Alok's workload generator to trigger traffic spikes, observe Kafka lag, and verify KEDA and scaling behavior on AWS

## Folders Alok Should Use
- [data/reference/](/Users/tanishgupta/Desktop/vacciguard/data/reference): store the device and facility lookup dataset and any threshold-enrichment files
- [data/batch/](/Users/tanishgupta/Desktop/vacciguard/data/batch): store sample daily operations or maintenance log files
- [data/workloads/dev/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/dev): store small workload files for quick checks
- [data/workloads/main/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/main): store the main precomputed workloads used for baseline versus optimized comparison
- [data/workloads/heavy/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/heavy): store stronger fallback workloads if the main profile is not stressful enough
- [services/replay-producer/](/Users/tanishgupta/Desktop/vacciguard/services/replay-producer): keep the replay producer code, configuration, and container-related files
- [tests/workload/](/Users/tanishgupta/Desktop/vacciguard/tests/workload): keep simple validation checks for replay rate, producer behavior, and workload execution notes
- [scripts/](/Users/tanishgupta/Desktop/vacciguard/scripts): keep helper scripts for offline data generation if they do not belong strictly inside the replay-producer service

## Baseline Coordination Rules
For the shared dependency flow, Alok should also read:
- [baseline-dependency-and-coordination-plan.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/baseline-dependency-and-coordination-plan.md)

### When Alok can start
Alok can start immediately because the schema is already frozen and the starter files already exist.

### When Alok must inform others
Alok should stop and inform the team at these baseline checkpoints:
- after the schema and starter data artifacts are stable
- after workload profiles and replay-rate assumptions are defined
- after the replay producer can publish a smoke workload successfully
- after the final baseline workload is frozen

He should inform:
- **Aayush**, so processing logic can proceed against stable inputs
- **Monty**, so deployment and scaling tests can use the right replay behavior
- **Tanish**, so the baseline methodology stays fair

### When Alok's baseline task is complete
For the baseline pipeline, Alok's work is complete when:
- the frozen schema is respected
- sample lookup and batch files exist
- main workload files exist
- replay behavior is stable enough for the team to use
- the final baseline workload has been frozen and handed off

## Detailed Step-by-Step Approach

### Step 1: Freeze the input schema before generating large data
Before generating large datasets, Alok should confirm the event structure with Tanish and Aayush.

The schema is now frozen in:
- [input-schema-freeze.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/01-foundation/input-schema-freeze.md)

Alok should treat that file as the source of truth and use these starter files as templates:
- [live-telemetry-sample.ndjson](/Users/tanishgupta/Desktop/vacciguard/data/workloads/dev/live-telemetry-sample.ndjson)
- [device-facility-lookup-template.csv](/Users/tanishgupta/Desktop/vacciguard/data/reference/device-facility-lookup-template.csv)
- [daily-operations-log-template.csv](/Users/tanishgupta/Desktop/vacciguard/data/batch/daily-operations-log-template.csv)

The live telemetry event should include at least:
- `event_id`
- `device_id`
- `temperature_c`
- `door_open`
- `battery_pct`
- `event_time`

The lookup dataset should include at least:
- `device_id`
- `facility_name`
- `district`
- `state`
- `min_temp_c`
- `max_temp_c`

The batch log dataset should include at least:
- `facility_name` or `device_id`
- `log_date`
- `maintenance_flag`
- `power_outage_minutes`
- `manual_temp_check_c`

Why this step matters:
- Aayush needs a stable schema to write Spark jobs
- Monty needs consistent field names for deployment and test configuration
- Tanish needs stable inputs to compare the two pipelines fairly

### Step 2: Design workload families before generating records
Alok should not create one generic dataset only. He should create multiple workload families,
because the project evaluation will require different scenarios.

Recommended workload families:
- `normal-load`
- `burst-load`
- `breach-heavy`
- `late-arrival`
- `duplicate-event`
- `recovery-replay`

What each one should do:
- `normal-load`: steady event flow, mostly safe readings, low error rate
- `burst-load`: sudden increase in event volume to help create backlog and scaling pressure
- `breach-heavy`: higher rate of out-of-range temperature values
- `late-arrival`: events whose `event_time` is older than arrival time
- `duplicate-event`: repeated `event_id` values to test deduplication
- `recovery-replay`: reusable dataset for reruns after component restart or recovery tests

Why this step matters:
- baseline and optimized pipelines must be tested against the same workload patterns
- the team needs more than one scenario to show correctness, scaling, and recovery behavior

### Step 3: Decide the target data rate and total dataset size before generation
After defining workload families, Alok should decide how fast the replay system must publish data
and how many records must be precomputed for each experiment.

This should be done using a simple workload model, not guesswork.

#### 3.1 Define the experiment timeline
The main scaling experiment should be divided into three phases:

- **warmup**: normal traffic before the spike, used to let the pipeline settle into steady state
- **spike**: high traffic period used to create backlog and scaling pressure
- **cooldown**: normal traffic after the spike, used to observe recovery, backlog drain, and scale-in

A good starting timeline for the main evaluation run is:
- `warmup_seconds = 600` which is `10 minutes`
- `spike_seconds = 300` which is `5 minutes`
- `cooldown_seconds = 600` which is `10 minutes`

So the replay timeline becomes:
- `0 to 10 minutes`: normal traffic
- `10 to 15 minutes`: spike traffic
- `15 to 25 minutes`: normal traffic again

#### 3.2 Choose the normal event rate from a device model
The normal event rate should come from a realistic device assumption.

Recommended starting assumption:
- `1000 devices`
- each device sends `1 event every 2 seconds`

This gives:

`normal_rate = number_of_devices / seconds_per_event`

So:

`normal_rate = 1000 / 2 = 500 events per second`

Because the project success criteria mention a `10x traffic spike`, the spike rate becomes:

`spike_rate = 10 x normal_rate`

So:

`spike_rate = 10 x 500 = 5000 events per second`

This is the recommended starting rate plan for Alok's main workload.

#### 3.3 Calculate how many events must be precomputed
Once the rates and timeline are fixed, Alok should calculate the total number of event records
needed for the replay dataset.

Use this formula:

`total_events = (normal_rate x warmup_seconds) + (spike_rate x spike_seconds) + (normal_rate x cooldown_seconds)`

Using the recommended starting plan:
- warmup events: `500 x 600 = 300,000`
- spike events: `5000 x 300 = 1,500,000`
- cooldown events: `500 x 600 = 300,000`

So the full main evaluation workload should contain:

`2,100,000 events`

This means Alok should precompute one main streaming workload of about `2.1 million` records for
the baseline versus optimized scale test.

#### 3.4 Estimate the rough storage size
Alok does not need the exact storage size before generation, but a rough estimate helps with
planning.

If one raw JSON event is about `250 bytes`, then:

`2,100,000 x 250 bytes = 525,000,000 bytes`

This is about:
- `525 MB raw`, before compression

This is a manageable size for a project dataset and is a good target for the first main workload.

#### 3.5 Use more than one workload size
Alok should not stop at a single large dataset. He should prepare at least three levels:

- **dev or smoke profile**:
  - `50 events/sec` normal
  - `500 events/sec` spike
  - short run for quick debugging
- **main evaluation profile**:
  - `500 events/sec` normal
  - `5000 events/sec` spike
  - `2.1 million` events total
- **heavy profile**:
  - `1000 events/sec` normal
  - `10000 events/sec` spike
  - used only if the main profile does not stress the baseline enough

This gives the team:
- a cheap profile for early testing
- a realistic profile for baseline versus optimized comparison
- a stronger backup profile if the first spike is too weak

#### 3.6 Precompute once, replay many times
The goal is not to generate new random data for every run.

Instead, Alok should:
- precompute the main workloads once
- save them in organized files
- replay the same workload files for both the baseline and optimized pipelines

This is important because:
- fairness requires the same input data on both sides
- only pipeline settings should change between the two experiments

#### 3.7 Shard the dataset for parallel replay
The large streaming workload should not be stored only as one huge file.

Recommended approach for the `2.1 million` event workload:
- split it into `10` files of about `210,000` events each
or
- split it into `20` files of about `105,000` events each

This makes it easier to:
- run multiple producer instances
- distribute replay load
- avoid the replay producer becoming the new bottleneck

#### 3.8 Validate and adjust after the first baseline calibration
The starting rates above are the recommended first plan, not a permanent final truth.

After the team runs the first fixed-capacity baseline:
- if the baseline is barely stressed, Alok should move toward the heavy profile
- if the baseline is overwhelmed too early, Alok should reduce the main profile
- if the replay producer cannot sustain the publish rate, Alok should increase parallel replay workers

The important rule is:
- the adjustment should happen **before** the final comparison
- once the final comparison workload is chosen, it should remain fixed for both pipelines

### Step 4: Precompute the datasets offline
Alok should generate the synthetic datasets ahead of time instead of generating all records on the
fly during evaluation.

Recommended approach:
- create each workload as a separate dataset file or file group
- store development workloads in [data/workloads/dev/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/dev), main comparison workloads in [data/workloads/main/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/main), and stronger fallback workloads in [data/workloads/heavy/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/heavy)
- store them in a structured folder layout
- include enough volume to support both normal and stressed runs
- preserve event order where needed
- make sure `event_id` values are unique except in the duplicate-event case

Why this step matters:
- precomputed data makes experiments repeatable
- the simulator no longer becomes the main bottleneck
- the same input can be replayed for baseline and optimized runs

### Step 5: Build a replay producer instead of a random-only live simulator
The runtime producer should behave like a controlled replay tool.

It should:
- read the precomputed workload file
- publish records into Kafka
- allow configurable send rate
- allow selecting a workload profile
- support running more than one producer instance if required

The producer implementation itself should live in [services/replay-producer/](/Users/tanishgupta/Desktop/vacciguard/services/replay-producer).

This means the runtime flow becomes:
- offline generation first
- replay into Kafka later

Why this step matters:
- it separates data generation from throughput generation
- it lets Monty create repeatable spike experiments
- it prevents random runtime generation from masking pipeline bottlenecks

### Step 6: Add rate controls that match the evaluation plan
The replay producer must support rate-based execution.

At minimum, it should allow:
- a normal steady rate
- a sudden 10x spike rate
- a cooldown or return-to-normal phase

Ideal controls:
- events per second
- workload file selection
- burst start time
- burst duration
- number of parallel producer instances

Why this step matters:
- the team must prove elastic scaling under controlled pressure
- KEDA and Spark scaling are only meaningful if backlog actually appears and then drops

### Step 7: Plan for horizontal scaling of the producer itself
Alok should assume that one producer process may not be enough.

He should design the producer so it can run:
- as multiple processes locally if needed
- as multiple pods on AWS if needed

Good design choices:
- split workload files into partitions or shards
- assign each producer instance a specific shard
- make sure producers do not accidentally resend the exact same shard unless that is the test case

Why this step matters:
- a single Python process may not be able to create enough traffic
- multiple replay workers make the input side scalable too

### Step 8: Validate that the producer can actually stress Kafka
Before handing the input system to the team, Alok should run a dedicated producer validation.

He should verify:
- the replay tool reaches the intended send rate
- Kafka receives the records successfully
- the observed traffic is large enough to create backlog during stress tests
- burst mode clearly increases message rate above normal mode

Evidence to capture:
- sent-record counts
- records per second achieved
- publish errors or retries
- a simple log showing when burst mode begins and ends

Why this step matters:
- without this proof, the later scaling experiment may fail for the wrong reason

### Step 9: Prepare handoff artifacts for Aayush
Alok should give Aayush everything needed to start processing work without guessing.

That handoff should include:
- the exact event schema
- sample event files
- lookup dataset sample
- batch log sample
- explanation of each workload type
- notes on duplicate and late-arrival cases

Why this step matters:
- Aayush can start Spark logic immediately
- schema mismatches are caught early

### Step 10: Prepare handoff artifacts for Monty
Alok should also give Monty everything needed for deployment and scaling experiments.

That handoff should include:
- replay producer container requirements
- how to configure workload selection
- how to change publish rate
- whether multiple producer instances are supported
- what normal rate and spike rate should be used in AWS tests

Why this step matters:
- Monty needs this to trigger real scaling behavior on AWS
- if deployment starts without this information, scaling tests become guesswork

### Step 11: Align with Tanish on experiment fairness
Before final evaluation, Alok should confirm with Tanish:
- the same workload files will be used for both pipelines
- the same replay schedule will be used for both pipelines
- only pipeline settings, not input data, will change between baseline and optimized runs

Why this step matters:
- this is what makes the methodology academically defensible

## Practical Build Sequence
If Alok wants the shortest path, he should complete the work in this order:

1. Freeze schemas with Tanish and Aayush
2. Create lookup and batch sample datasets
3. Design workload families
4. Decide normal rate, spike rate, and total dataset size
5. Generate precomputed telemetry datasets
6. Build the replay producer
7. Add configurable rate controls
8. Add support for parallel producer instances
9. Validate throughput against Kafka
10. Hand off samples and schema to Aayush
11. Hand off deployment controls and workload profiles to Monty
12. Re-run the producer during baseline and optimized experiments with the same inputs

## Done Criteria
Alok's task should be considered complete only when all of the following are true:
- workload datasets are precomputed and organized
- replay producer can publish normal and burst traffic
- the team has evidence that the producer can create enough pressure for scaling tests
- Aayush has the schema and sample files needed for Spark processing
- Monty has the controls needed to run replay in AWS
- Tanish can use the exact same workloads for baseline and optimized comparisons

## Common Mistakes To Avoid
- relying only on random live generation during evaluation
- generating a different dataset every time without saving it
- not including `event_id` in live events
- building only one workload type
- assuming one local producer instance will be enough for a 10x spike
- changing the input dataset between baseline and optimized runs

## One-Sentence Summary
Alok's job is not just to generate data. His job is to build a repeatable and scalable workload
system that can fairly stress the pipeline and support every later phase of the project.
