# Tanish Execution Guide

## Role Summary
Tanish owns integration, baseline control, coordination, and baseline evaluation for VacciGuard.

This guide is intentionally limited to:
- baseline pipeline completion
- baseline validation
- baseline sign-off
- baseline evaluation readiness

It does **not** cover later optimization work in detail. That will come after the baseline pipeline
is complete and accepted.

Tanish is the person who makes sure the team does not accidentally turn the baseline into:
- an incomplete system
- an unfair system
- an undocumented system
- or a moving target that cannot be compared later

## Main Objective
Get the baseline pipeline to a state where it is:
- integrated end to end
- functionally correct enough to use as a reference
- fixed in scope and resource shape
- measurable
- documented clearly enough to support later comparison

## How Tanish Should Think About His Role
Tanish is not the main builder of Kafka, Spark, or AWS infrastructure. Instead, he is the one who
protects the integrity of the whole baseline project.

In practical terms:
- Alok creates the input side
- Aayush creates the processing logic
- Monty creates the cloud runtime and monitoring layer
- Tanish makes sure these pieces connect correctly and become one valid baseline pipeline

So Tanish's role is a combination of:
- integration owner
- freeze authority
- methodology owner
- review coordinator
- baseline sign-off owner

If this role is weak, the project may still "run," but it will be much harder to defend in front
of the professor.

## What Tanish Must Deliver By Baseline Completion
- clear baseline acceptance criteria
- one integrated baseline pipeline story
- baseline freeze decisions for schema, workload, and deployment assumptions
- baseline evidence pack for later comparison
- clear coordination across Alok, Aayush, and Monty
- baseline sign-off decision when the pipeline is ready

## How Other Members Use Tanish's Work
- **Alok** depends on Tanish to freeze the workload and keep the baseline comparison fair
- **Aayush** depends on Tanish to confirm what business logic is fixed for the baseline
- **Monty** depends on Tanish to confirm which deployment assumptions are part of the fixed baseline

## Folders Tanish Should Use
- [Project Planning/](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning): primary planning, coordination, and methodology area
- [Project Planning/project-context.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/project-context.md): live project sync board
- [Project Planning/baseline-dependency-and-coordination-plan.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/baseline-dependency-and-coordination-plan.md): team-level dependency and gate plan
- [results/baseline/](/Users/tanishgupta/Desktop/vacciguard/results/baseline): place to collect baseline experiment outputs and summaries

## Inputs Tanish Must Treat As Fixed
Tanish should align all integration and baseline decisions with these files:
- [phase-1-problem-statement.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/01-foundation/phase-1-problem-statement.md)
- [dataset-plan.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/01-foundation/dataset-plan.md)
- [input-schema-freeze.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/01-foundation/input-schema-freeze.md)
- [phase-3-technology-stack.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/02-phase-3/phase-3-technology-stack.md)
- [phase-3-objective-mapping.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/02-phase-3/phase-3-objective-mapping.md)
- [project-folder-structure.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/project-folder-structure.md)
- [baseline-dependency-and-coordination-plan.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/baseline-dependency-and-coordination-plan.md)
- [project-context.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/project-context.md)
- [alok-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/alok-execution-guide.md)
- [aayush-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/aayush-execution-guide.md)
- [monty-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/monty-execution-guide.md)

## Baseline Coordination Rules
For the shared dependency flow, Tanish should also read:
- [baseline-dependency-and-coordination-plan.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/baseline-dependency-and-coordination-plan.md)

### When Tanish can start
Tanish can start immediately because the baseline methodology, coordination, and integration logic
depend on existing planning decisions, not on finished code.

### When Tanish must inform others
Tanish should stop and inform the team at these baseline checkpoints:
- after baseline acceptance criteria are written clearly
- after a schema, workload, or deployment assumption is officially frozen
- after a coordination gate is accepted as complete
- after baseline sign-off is granted or rejected

He should inform:
- **Alok**, when workload assumptions or input-side freeze decisions are made
- **Aayush**, when baseline business logic is considered fixed enough to protect
- **Monty**, when deployment assumptions are frozen for the baseline
- **everyone**, when the baseline is officially signed off

### When Tanish's baseline task is complete
For the baseline pipeline, Tanish's work is complete when:
- the baseline is integrated end to end
- the baseline workload is frozen
- the baseline deployment profile is frozen
- baseline correctness is reviewed
- baseline evidence is collected
- baseline sign-off is recorded clearly

## Core Concepts Tanish Must Understand Before Coordinating

### 1. The baseline is a reference system, not a weak prototype
The baseline should not be treated as something intentionally poor.

It should be:
- correct
- simple
- fixed
- measurable

That means the baseline is allowed to have:
- validation
- deduplication
- enrichment
- breach logic
- Redis writes
- S3 writes
- monitoring

What it should avoid is adaptive optimization such as:
- KEDA-driven autoscaling
- HPA-based scaling for the main path
- Spark dynamic allocation
- cost-aware resource tuning logic

### 2. Tanish protects fairness more than speed
If one member wants to change:
- schema
- workload
- business logic
- resource assumptions

Tanish should ask:

> "Does this still belong to the baseline, or are we changing the experiment itself?"

This is the most important question for methodology.

### 3. A working system is not enough
Tanish should not sign off the baseline only because:
- pods are running
- data is flowing
- a dashboard exists

He should also ask:
- is the stream path working?
- is the batch path working?
- are outputs correct enough to trust?
- are the fixed assumptions documented?
- can this baseline be repeated?

### 4. The baseline must be frozen before optimization begins
If the team begins optimization before the baseline is frozen:
- the comparison becomes messy
- people forget what the real baseline was
- metrics and resource assumptions drift

So Tanish should treat baseline freeze as a real milestone, not a vague feeling.

## Detailed Step-by-Step Approach

### Step 1: Define baseline acceptance criteria early
Before serious implementation begins, Tanish should write what "baseline complete" actually means.

At minimum, the baseline should require:
- the stream path works from input to sinks
- the batch path works from input to output
- schema is frozen
- workload is frozen
- deployment profile is fixed
- no autoscaling is enabled in baseline
- metrics are visible

Why this step matters:
- without this, the team may keep saying the baseline is "almost ready" without a real finish line

### Step 2: Treat `project-context.md` as the team sync board
Tanish should use:
- [project-context.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/project-context.md)

as the first place to check before asking:
- what is done
- what is blocked
- what should be worked on next

He should also enforce the rule that:
- no one claims a meaningful handoff until the context file is updated

Why this step matters:
- it keeps the team synced
- it reduces confusion when multiple workstreams run in parallel

### Step 3: Watch the coordination gates closely
Tanish should use:
- [baseline-dependency-and-coordination-plan.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/baseline-dependency-and-coordination-plan.md)

to track whether the baseline is actually progressing through the required gates.

He should explicitly monitor:
- Gate 1: Schema Freeze
- Gate 2: Processing Contract Ready
- Gate 3: AWS Base Ready
- Gate 4: Replay Smoke Works
- Gate 5: Stream Smoke Works
- Gate 6: Batch Smoke Works
- Gate 7: Baseline Deployment Freeze
- Gate 8: Main Workload Freeze
- Gate 9: Baseline Sign-Off

Why this step matters:
- the gates are the best structure for keeping the baseline disciplined

### Step 4: Review Alok's output as baseline input contract
When Alok completes a handoff, Tanish should check:
- is the schema still aligned with the frozen contract?
- are workload files organized and understandable?
- is the main workload definition stable enough to freeze later?
- is the replay behavior clear enough for Aayush and Monty?

Why this step matters:
- the whole baseline depends on the input side being stable and repeatable

### Step 5: Review Aayush's output as baseline business logic
When Aayush completes a handoff, Tanish should check:
- are validation rules clear?
- is deduplication logic reasonable?
- is enrichment logic aligned with the lookup dataset?
- is breach logic explicit and stable?
- are Redis and S3 responsibilities clearly separated?

Why this step matters:
- Aayush's logic becomes the truth of the baseline pipeline

### Step 6: Review Monty's output as baseline runtime contract
When Monty completes a handoff, Tanish should check:
- is the AWS environment really usable?
- is the baseline fixed-capacity?
- is autoscaling disabled in the baseline?
- are S3 and Redis reachable?
- is monitoring visible enough for evaluation?

Why this step matters:
- the baseline deployment profile must be clear before it is frozen

### Step 7: Decide when assumptions become frozen
Tanish should explicitly announce freeze decisions when the team reaches them.

Important freeze points:
- schema freeze
- workload freeze
- baseline deployment freeze
- baseline sign-off

He should record these clearly in:
- [project-context.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/project-context.md)

Why this step matters:
- undocumented freeze decisions are easy to break later

### Step 8: Check the baseline end-to-end path, not only separate components
Before baseline sign-off, Tanish should confirm the whole path works together.

At minimum, he should check this story:
1. Alok's replay workload enters Kafka
2. Aayush's stream logic processes it
3. Redis receives current state
4. S3 receives historical output
5. batch input can be processed as well
6. Monty's monitoring shows useful metrics

Why this step matters:
- component-level success does not always mean integrated success

### Step 9: Require evidence, not only verbal updates
When a member says a step is complete, Tanish should ask for concrete evidence such as:
- file paths
- output paths
- sample outputs
- log evidence
- dashboard screenshots or metric references
- runtime config references

Why this step matters:
- this keeps baseline sign-off evidence-based

### Step 10: Freeze the main workload only after it is usable
Tanish should not freeze the workload too early.

He should freeze it only after:
- the replay behavior is clear
- the rate plan is realistic
- the team agrees it is suitable for the baseline experiment

Why this step matters:
- a workload frozen too early may later prove unusable

### Step 11: Freeze the baseline deployment only after it is stable
Tanish should not freeze the baseline deployment profile too early either.

He should freeze it only after:
- the stream path runs stably
- the batch path runs
- monitoring is visible
- no hidden autoscaling remains enabled

Why this step matters:
- the deployment profile is part of the methodology, not just an infrastructure detail

### Step 12: Build the baseline evidence pack
Once the baseline is working, Tanish should collect baseline evidence into:
- [results/baseline/](/Users/tanishgupta/Desktop/vacciguard/results/baseline)

This evidence should include:
- key deployment assumptions
- workload assumptions
- sample output summaries
- metric screenshots or exported values
- notes on correctness and known limitations

Why this step matters:
- this evidence will later support the optimized comparison

### Step 13: Decide whether the baseline is sign-off ready
Tanish should only sign off the baseline if the following are true:
- stream path works
- batch path works
- workload is frozen
- deployment profile is frozen
- baseline is fixed-capacity
- outputs are correct enough to trust
- metrics are visible enough to evaluate

If one of these is missing, the baseline should stay open.

Why this step matters:
- a weak sign-off makes the later comparison fragile

### Step 14: Record the sign-off clearly
Once the baseline is accepted, Tanish should make the sign-off visible by updating:
- [project-context.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/project-context.md)

He should mark:
- baseline gate status
- frozen assumptions
- any important known limitations

Why this step matters:
- everyone should know exactly when the team has moved from "building baseline" to "baseline finished"

### Step 15: Stop the role at baseline evaluation readiness
This guide ends when the baseline is:
- complete
- frozen
- evidenced
- ready to be used as the reference point for later optimization

At that point, Tanish should not keep making baseline changes casually.

Why this step matters:
- once baseline is complete, later work should compare against it, not silently rewrite it

## Practical Build Sequence
If Tanish wants the clearest path, he should work in this order:

1. Write baseline acceptance criteria
2. Use `project-context.md` as the required team sync board
3. Track the baseline coordination gates
4. Review Alok's input-side handoffs
5. Review Aayush's processing-side handoffs
6. Review Monty's deployment-side handoffs
7. Freeze schema, workload, and deployment assumptions at the right time
8. Check the full end-to-end baseline path
9. Build the baseline evidence pack
10. Sign off the baseline only when it is truly stable and measurable

## Done Criteria
Tanish's baseline role should be considered complete only when:
- baseline acceptance criteria are defined
- all critical coordination gates are tracked
- schema, workload, and deployment assumptions are frozen
- the baseline pipeline is integrated end to end
- baseline outputs and metrics are collected
- baseline sign-off is recorded clearly

## Common Mistakes To Avoid
- treating the baseline like a rough draft instead of a reference system
- allowing schema or workload changes after freeze without recording them
- signing off based only on "it runs"
- letting autoscaling leak into the baseline
- forgetting to collect evidence at the moment the baseline works
- delaying sign-off until after optimization work has already started

## One-Sentence Summary
Tanish's job is to turn the team's separate baseline components into one fixed, integrated,
evidence-backed baseline pipeline that is ready to be used as the official reference for later
comparison.
