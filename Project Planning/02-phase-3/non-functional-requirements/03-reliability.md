# Reliability

## Requirement
The brief mentions a target of at least 99.9% pipeline uptime.

## What This Means
The system should keep operating during normal failures, recover automatically when components crash, and avoid losing or corrupting data during recovery.

## How The Architecture Addresses It
- `Kubernetes` can restart failed containers automatically.
- important stateless services can run with multiple replicas in the optimized setup.
- `Kafka` stores events durably so short downstream failures do not immediately cause data loss.
- `Spark checkpoints` preserve stream-processing progress and support restart recovery.
- `Airflow` retries failed batch jobs.
- `Prometheus + Grafana` help detect failures and measure recovery time.

## Failure Scenarios To Test
- stop a consumer or processing component
- kill a Spark pod or container
- overload the system with burst traffic
- restart a service while ingestion is active

## How We Will Measure It
We will record:

- failed job count
- restart count
- recovery time
- successful processing time versus failure time
- duplicate count after restart
- data loss count

## Important Academic Caution
For a student project, claiming true 99.9% uptime over a long production window would be too strong unless we actually operate and measure the system for that duration.

The safer academic claim is:

- the architecture is designed for high reliability
- controlled failure tests show automatic recovery within a measured time
- observed behavior supports the reliability goal
