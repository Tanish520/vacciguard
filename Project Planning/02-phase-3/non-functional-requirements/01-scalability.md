# Scalability

## Requirement
The project brief expects the pipeline to auto-scale for 10x traffic spikes.

## What This Means
When the incoming event rate increases sharply, the system should not stay fixed at the same capacity. It should grow to handle the extra load and shrink again when the load drops.

In this project, scalability must happen in two places:

- the `ingestion layer`
- the `processing layer`

## How The Architecture Addresses It
### Ingestion Layer
- `Kafka` buffers incoming events so a sudden spike does not immediately break downstream processing.
- `Kafka partitions` allow multiple consumers to process data in parallel.
- `KEDA` watches Kafka lag and increases or decreases relevant Kubernetes pods based on backlog.
- `HPA` can scale regular services using CPU or memory metrics when lag is not the right signal.

### Processing Layer
- `Spark Structured Streaming` processes stream data continuously in small intervals.
- `Spark dynamic allocation` increases the number of executors when the workload grows and reduces them when the workload becomes lighter.
- `Kubernetes` provides the environment in which these resources can scale.

## Why This Is A Good Fit
This gives a clear scaling story:

- more traffic creates more Kafka backlog
- KEDA reacts by adding ingestion-side workers
- Spark dynamic allocation reacts by adding processing-side compute
- once traffic falls, resources shrink again

## How We Will Measure It
We will run:

- normal load
- 5x spike
- 10x spike
- burst-and-recovery pattern

For each run, we will record:

- input rate
- throughput
- Kafka lag
- number of running pods
- number of Spark executors
- time needed to stabilize after the spike

## What We Can Honestly Claim
If the optimized version handles the 10x workload with acceptable lag growth and recovery, we can claim tested elastic scalability within the defined workload range.
