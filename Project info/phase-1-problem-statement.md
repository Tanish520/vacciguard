# Phase 1: Problem Statement

## Project Title
VacciGuard: A Cloud Data Pipeline Case Study for Vaccine Cold-Chain Monitoring

## Plain-English Problem Statement
Vaccines must be stored within a safe temperature range from the time they leave a warehouse until they reach clinics and outreach points. In practice, storage units in different locations can face power issues, door misuse, battery problems, and delayed manual reporting. Because of this, staff may not know quickly enough when storage conditions become unsafe.

This project does not aim to build a full vaccine management system. Instead, it focuses on one practical problem: how to collect, process, store, and analyze storage-condition readings from many cold-chain units in a way that is fast, reliable, and affordable.

Each cold-chain unit produces readings such as temperature, door status, battery level, location, and time. These readings arrive continuously throughout the day. Some locations produce a steady flow, while others can become bursty when many devices reconnect or when a campaign increases activity. Alongside this live flow, the system also uses a small supporting lookup dataset and receives batch files such as daily operations or maintenance logs that support reporting and analysis.

The system needs to produce two kinds of value:

- fast detection of unsafe storage conditions so that alerts can be raised quickly
- historical analysis so that districts or states can review compliance trends, breach frequency, and service quality over time

If the system becomes slow, alerts may arrive too late to be useful. If the system fails, readings may pile up, reports may become incomplete, and operators may lose trust in the platform. For that reason, the project is really a study of how to build and evaluate a cloud pipeline that can handle mixed live and batch data under changing load and failure conditions.

## What Data Is Coming
- Live device readings: temperature, door status, battery level, location, device ID, and event time
- Batch files: daily operations logs, maintenance records, and summary logs
- Small supporting lookup data: facility list, district/state mapping, and storage threshold rules used to label and interpret incoming readings

### What Supporting Lookup Data Means
This is not another major data stream. It is a small table or file that helps the pipeline understand the live events.

Examples:

- `facility list`: maps a facility or fridge ID to a real facility name
- `district/state mapping`: helps group results for reporting by region
- `storage threshold rules`: defines what counts as safe or unsafe temperature for a given unit or vaccine category

Without this kind of lookup data, the pipeline can still move records, but enrichment, reporting, and breach classification become much weaker.

## How Fast It Comes
- Live readings arrive continuously
- Load is uneven across locations
- Traffic can become bursty during campaigns, reconnect events, or recovery after downtime
- Batch files arrive on a fixed schedule, such as once per day

## What Insights Are Needed
- Immediate detection of temperature breaches and related risk conditions
- Current status view of devices, locations, and active alerts
- Daily and weekly compliance summaries
- Historical analysis by district, state, facility, and time period
- Evidence of how the pipeline behaves under spike and failure scenarios

## What Happens If The System Is Slow Or Fails
- Unsafe conditions may be detected too late
- Operational staff may miss or delay intervention
- Backlog may grow and increase delay further
- Duplicate, missing, or late records may reduce report quality
- Confidence in the pipeline decreases

## Clear Success Metrics
1. End-to-end delay for live readings stays below 5 seconds during normal workload.
2. The pipeline continues operating during a 10x traffic spike and recovers from a single component failure within 2 minutes without intentional data loss.
3. The optimized pipeline reduces cost per GB processed compared with the baseline while still meeting the latency target.

## Scope Boundaries
### In Scope
- Simulated cold-chain readings
- Mixed live and batch data pipeline
- Breach detection and alert generation
- Historical reporting and analysis
- Controlled performance, reliability, and cost evaluation

### Out Of Scope
- Vaccine inventory management
- Appointment booking
- Patient records
- National immunization workflow management

## Working Assumptions
- The project will use simulated device data so experiments can be repeated in a controlled way.
- The first implementation will target one cloud region and a small deployment footprint.
- The final project will compare a baseline pipeline against one optimized version, not multiple unrelated architectures.
- The project will use exactly three logical datasets: a live telemetry stream, a small lookup dataset, and one independent batch dataset.
