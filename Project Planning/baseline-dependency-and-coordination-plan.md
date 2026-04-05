# Baseline Dependency And Coordination Plan

## Purpose
This document defines how the team should coordinate while building the **baseline pipeline**.

It explains:
- who can start immediately
- who depends on whom
- what work can happen in parallel
- what coordination gates must be respected
- when the baseline is considered frozen and ready for comparison

This file is the shared team-level coordination source of truth.

## Important Principle
No one is fully independent for the entire baseline pipeline, but several members can begin work in
parallel right away.

The correct team model is:

- **parallel start**
- **coordinated handoff**
- **dependent integration**
- **shared baseline freeze**

## Who Can Start Immediately

### Alok can start immediately
Why:
- the input schema is frozen
- sample input files already exist
- the folder structure for workloads and replay logic already exists

He can begin:
- lookup data
- batch data
- workload generation
- replay producer

### Aayush can start immediately
Why:
- the input schema is frozen
- sample files already exist
- the stream and batch processing contract can be designed before the full cloud deployment is ready

He can begin:
- validation logic
- deduplication logic
- enrichment logic
- breach logic
- stream and batch processing design

### Monty can start immediately
Why:
- the cloud stack is already decided
- the deployment target is already fixed as AWS and Amazon EKS
- he does not need to wait for full application completion to start Terraform and EKS work

He can begin:
- Terraform skeleton
- EKS foundation
- S3 and ElastiCache planning
- monitoring skeleton
- baseline deployment structure

### Tanish can start immediately
Why:
- baseline methodology, coordination, integration, and evaluation planning already depend on the
  existing project decisions

He can begin:
- integration checklist
- baseline acceptance criteria
- architecture consistency checks
- coordination checkpoints

## Dependency Model

## Alok
### Independent early work
- create lookup dataset
- create batch dataset templates
- create precomputed workloads
- build replay producer
- define workload profiles and rates

### Main dependencies
- light early dependency on Tanish and Aayush only to respect the frozen schema
- later dependency on Monty for real AWS/EKS runtime execution

### Who depends on Alok
- **Aayush** depends on Alok for real sample inputs and workload structure
- **Monty** depends on Alok for replay behavior and traffic generation during tests
- **Tanish** depends on Alok for the frozen evaluation workload

## Aayush
### Independent early work
- define stream and batch processing order
- implement validation logic
- implement deduplication logic
- implement enrichment logic
- implement breach logic
- test on small sample inputs

### Main dependencies
- depends on Alok for stable sample inputs and workload contract
- later depends on Monty for real sink connectivity and AWS runtime deployment

### Who depends on Aayush
- **Monty** depends on Aayush for runtime behavior, sink expectations, and processing outputs
- **Tanish** depends on Aayush for correctness expectations and final processed outputs

## Monty
### Independent early work
- build Terraform skeleton
- create EKS foundation
- plan S3 and ElastiCache setup
- prepare CloudWatch, Prometheus, and Grafana setup
- separate base, baseline, and optimized deployment structure

### Main dependencies
- depends on Alok later for replay producer runtime behavior
- depends on Aayush later for Spark runtime assumptions, checkpoint needs, and sink requirements

### Who depends on Monty
- **Alok** depends on Monty for AWS runtime where workloads are exercised seriously
- **Aayush** depends on Monty for real S3, Redis, EKS, and monitoring setup
- **Tanish** depends on Monty for the final deployment assumptions and metrics

## Tanish
### Independent early work
- baseline methodology
- integration checklist
- acceptance criteria
- architecture consistency
- evaluation planning

### Main dependencies
- depends on all three others for final baseline sign-off

### Who depends on Tanish
- everyone depends on Tanish to freeze decisions and keep the comparison fair

## What Work Can Happen In Parallel

### Parallel work block 1
This work can happen immediately and in parallel:
- **Alok**: sample datasets, workload generation, replay-producer design
- **Aayush**: processing contract, validation, enrichment, breach logic using sample files
- **Monty**: Terraform, EKS, S3, ElastiCache, monitoring skeleton
- **Tanish**: baseline methodology, integration flow, acceptance criteria

### Parallel work block 2
Once the first handoffs happen, this work can continue in parallel:
- **Alok** validates replay behavior
- **Aayush** validates stream and batch smoke behavior
- **Monty** deploys the baseline environment and monitoring
- **Tanish** reviews artifacts and prepares the baseline freeze checklist

## Coordination Gates
The baseline pipeline should be built using explicit coordination gates.

At each gate, the owner should share:
- what is done
- where the relevant files/configs are
- what others can use now
- what is still blocked

## Gate 1: Schema Freeze
### Owner
- Alok, with Tanish oversight

### What must be done
- input schema fixed
- sample telemetry file available
- lookup template available
- batch template available

### Who must be informed
- Aayush
- Monty
- Tanish

### Why this gate matters
- Aayush cannot safely write processing logic without stable fields
- Monty should not prepare deployment assumptions for unstable contracts

## Gate 2: Processing Contract Ready
### Owner
- Aayush

### What must be done
- stream processing order defined
- batch processing order defined
- validation, deduplication, enrichment, breach logic clearly described
- sink behavior for Redis and S3 explained

### Who must be informed
- Alok
- Monty
- Tanish

### Why this gate matters
- Monty needs runtime expectations
- Tanish needs the baseline business logic to remain stable

## Gate 3: AWS Base Ready
### Owner
- Monty

### What must be done
- Terraform skeleton exists
- EKS approach is prepared
- S3 and ElastiCache plan exists
- monitoring direction is established

### Who must be informed
- Alok
- Aayush
- Tanish

### Why this gate matters
- the project should not wait until the end to discover cloud-specific issues

## Gate 4: Replay Smoke Works
### Owner
- Alok

### What must be done
- replay producer publishes sample workload successfully
- topic and message contract are stable
- rate control is usable

### Who must be informed
- Aayush
- Monty
- Tanish

### Why this gate matters
- Aayush needs real input behavior
- Monty needs the workload source for later scaling tests

## Gate 5: Stream Smoke Works
### Owner
- Aayush

### What must be done
- sample events process correctly
- validation works
- deduplication works
- enrichment works
- breach logic works
- Redis and S3 output meaning is clear

### Who must be informed
- Alok
- Monty
- Tanish

### Why this gate matters
- Monty cannot confidently deploy a runtime whose output behavior is unclear

## Gate 6: Batch Smoke Works
### Owner
- Aayush and Monty together

### What must be done
- one batch run completes successfully
- input path is correct
- output lands in the expected location
- batch behavior is observable

### Who must be informed
- Alok
- Tanish

### Why this gate matters
- the baseline pipeline is not complete unless both stream and batch paths work

## Gate 7: Baseline Deployment Freeze
### Owner
- Monty, with Tanish oversight

### What must be done
- fixed pod/resource counts documented
- fixed trigger assumptions documented
- no autoscaling enabled in the baseline
- deployment assumptions are stable enough for comparison

### Who must be informed
- Alok
- Aayush
- Tanish

### Why this gate matters
- nobody should be guessing runtime shape during final evaluation

## Gate 8: Main Workload Freeze
### Owner
- Alok, with Tanish oversight

### What must be done
- final normal rate is chosen
- final spike rate is chosen
- final warmup, spike, and cooldown durations are chosen
- workload files and shard layout are frozen

### Who must be informed
- Aayush
- Monty
- Tanish

### Why this gate matters
- baseline and optimized pipelines must be tested with the same workload

## Gate 9: Baseline Sign-Off
### Owner
- whole team, led by Tanish

### What must be done
- stream path works end to end
- batch path works end to end
- fixed baseline deployment is stable
- the final workload can be executed
- correctness and basic metrics are visible

### Who must be informed
- everyone

### Why this gate matters
- this is the point where the baseline becomes the reference point for later optimization work

## When The Baseline Is Officially Frozen
The baseline should be considered frozen only when all of the following are true:
- schema is fixed
- processing logic is fixed
- main workload is fixed
- deployment profile is fixed
- autoscaling is not enabled in the baseline
- both stream and batch paths work
- metrics are visible

## What Must Not Change After Baseline Freeze
After the baseline is frozen, the following should not change casually:
- schema field names
- business rules
- breach logic
- join logic
- workload definition
- baseline resource assumptions

Only then can the optimized pipeline be compared fairly.

## Simple Work Order For The Baseline
1. Alok freezes and shares input schema artifacts
2. Aayush defines the processing contract
3. Monty prepares the AWS base and deployment structure
4. Alok proves replay smoke behavior
5. Aayush proves stream smoke behavior
6. Aayush and Monty prove batch smoke behavior
7. Monty and Tanish freeze the baseline deployment profile
8. Alok and Tanish freeze the main workload
9. The whole team signs off the end-to-end baseline

## Final Coordination Rule
Each member should read this file together with their own execution guide:
- [alok-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/alok-execution-guide.md)
- [aayush-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/aayush-execution-guide.md)
- [monty-execution-guide.md](/Users/tanishgupta/Desktop/vacciguard/Project%20Planning/team-guides/monty-execution-guide.md)

The shared file explains **team coordination**.
The member files explain **individual execution**.
