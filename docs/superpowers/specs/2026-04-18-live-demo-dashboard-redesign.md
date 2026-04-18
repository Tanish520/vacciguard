# VacciGuard Live Demo Dashboard Redesign

**Date:** `2026-04-18`  
**Branch:** `baseline-spike-fix`  
**Owner:** `Codex + Tanish`

---

## Goal

Redesign the self-managed Grafana dashboard for the VacciGuard demo so it behaves like a true live wallboard during evaluation runs and looks visually strong enough for presentation. The redesigned dashboard must update visibly during active workload replay, clearly surface SLA and cold-chain risk state, and support the Phase 9.2 demo flow:

1. show data entering  
2. show metrics live  
3. trigger scaling or failure  
4. show recovery

The redesign should stay inside the existing self-managed observability path rather than creating a separate custom frontend.

---

## Problem Summary

The current Grafana dashboard has two shortcomings that make it weak for the demo:

### 1. Live panels feel stale

The current top-row KPI queries rely heavily on broad range aggregations such as:

- `avg_over_time(...[$__range])`
- `max_over_time(...[$__range])`

These are useful for report-style summaries, but they are not good for live-demo perception. During a replay run, they summarize the full selected time window instead of showing the freshest system state, so the panels feel delayed or sticky.

There is also a collection-side delay:

- Prometheus currently scrapes every `15s`
- alert rule evaluation also runs every `15s`
- the Grafana dashboard itself refreshes every `15s`

This stack is too slow for a convincing live demo.

### 2. Visual hierarchy is too plain

The current dashboard is KPI-first but still reads like an engineering baseline dashboard rather than an operations wallboard. It lacks:

- a bold top command row
- strong visual grouping
- domain-facing cold-chain status cards
- an incident or surge banner
- panels that clearly distinguish “pipeline healthy” from “vaccine safety at risk”

As a result, the dashboard does not create the same immediate impact as the reference wallboard style the user wants.

---

## Recommended Approach

The best-fit solution is to keep Grafana but redesign the dashboard into a hybrid operations wallboard backed by fresher Prometheus queries and a few additional stream metrics.

The redesigned solution will include:

- a new wallboard-style Grafana layout
- a hybrid top row showing both system health and cold-chain status
- shorter scrape/refresh settings for visible live movement
- top KPI queries based on fresh values instead of whole-window summaries
- small additions to stream metrics so Grafana can show:
  - active breaches
  - facilities at risk
  - dashboard freshness / last batch recency

### Why this approach

- It keeps the existing monitoring stack intact.
- It is much lower risk than building a custom UI before the demo.
- It directly addresses both real-time visibility and presentation quality.
- It aligns the dashboard with the project story: SLA-aware pipeline operations plus vaccine cold-chain safety monitoring.

### Alternatives considered

#### 1. Build a custom HTML dashboard

Pros:
- full visual control
- easier to mimic a polished presentation wallboard exactly

Cons:
- duplicates Grafana’s job
- higher implementation and debugging risk
- requires separate hosting and live data plumbing

#### 2. Do minor Grafana query tweaks only

Pros:
- smallest change set
- fastest to ship

Cons:
- does not solve the weak layout
- unlikely to look strong enough for the demo

This design chooses the stronger middle path: keep Grafana, but redesign it deliberately for live-demo use.

---

## Dashboard Direction

### Visual Style

The dashboard should follow an “operations wallboard” style:

- dark background
- bold, high-contrast KPI cards
- strong green / amber / red state coding
- clear section headers
- compact but dramatic visual hierarchy
- a bottom incident banner for current system state

The goal is not to copy the sample image exactly, but to match its strengths while fitting VacciGuard’s domain better.

### Top Command Row

The hero row should be hybrid, combining pipeline health with cold-chain impact:

- `P95 Latency`
- `Throughput`
- `Active Breaches`
- `Facilities at Risk`

This gives the viewer an immediate answer to two questions:

- is the pipeline healthy?
- is the vaccine cold-chain healthy?

### Middle Rows

The middle of the dashboard should balance trends and rankings.

#### Trend and health row

- latency trend
- throughput vs consumer lag
- hot vs cold batch duration
- query health / pod restart / freshness

#### Cold-chain insight row

- top risky facilities
- top risky devices
- processed vs invalid vs deduplicated volume
- breach rate distribution or quality breakdown

### Bottom Banner

A full-width status banner should show the current run condition:

- `NORMAL`
- `SURGE ACTIVE`
- `LAG BUILDING`
- `FAILURE / RECOVERING`
- `BREACH ALERT`

This banner is important for demo narration, because it lets the audience understand the current phase of the run at a glance.

---

## Metrics Strategy

### Existing metrics to keep using

The current stream processor metrics already support a large part of the wallboard:

- `vacciguard_stream_latest_batch_avg_latency_seconds`
- `vacciguard_stream_latest_batch_p95_latency_seconds`
- `vacciguard_stream_latest_batch_p99_latency_seconds`
- `vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds`
- `vacciguard_stream_observed_throughput_eps`
- `vacciguard_stream_hot_batch_duration_seconds`
- `vacciguard_stream_cold_batch_duration_seconds`
- `vacciguard_stream_queries_active`
- `vacciguard_stream_consumer_lag_records`
- `vacciguard_stream_processed_events_total`
- `vacciguard_stream_invalid_events_total`
- `vacciguard_stream_deduplicated_events_total`
- `vacciguard_stream_breach_events_total`
- `vacciguard_stream_latest_batch_timestamp_seconds`
- `vacciguard_stream_pod_restart_count`

### New metrics required

To support the hybrid command row properly, the stream processor should expose two additional live metrics:

- `vacciguard_stream_active_breaches`
  - current number of device IDs in the Redis active-breach set
- `vacciguard_stream_facilities_at_risk`
  - current count of distinct facilities represented in the active-breach set

These should be derived from the same Redis latest-state / active-breach ownership already used by the hot path, so they remain consistent with the live dashboard story.

### Optional freshness helper

The dashboard may also compute freshness directly from:

- `time() - vacciguard_stream_latest_batch_timestamp_seconds`

This can power a panel such as:

- `Last Batch Seen`
- `Metrics Freshness`

That is especially useful during failure/recovery demos.

---

## Query Strategy

The dashboard should split panels into two categories.

### Live KPI panels

These must feel immediate and should prefer current-state or short-window queries.

Use for:

- P95 latency
- throughput
- consumer lag
- active breaches
- facilities at risk
- query health
- freshness

Recommended query style:

- direct metric values when the metric already represents the latest state
- `last_over_time(metric[1m])` for resilience against scrape gaps
- short-window `max_over_time(...[1m])` only where “peak in the last minute” is the intended meaning

Avoid on the top row:

- `avg_over_time(...[$__range])`
- `max_over_time(...[$__range])`

because those hide recent changes during the demo.

### Trend panels

These should keep time-series history and therefore may use normal range behavior.

Use for:

- latency over time
- throughput vs lag trend
- hot vs cold batch duration trend
- processed / invalid / deduplicated trend

These panels are meant to explain movement, not just current state.

---

## Refresh Strategy

To make the dashboard visibly responsive during a live run, the observability cadence needs to tighten.

### Prometheus

Change:

- `scrape_interval`: `15s` -> `5s`
- `evaluation_interval`: `15s` -> `5s`

### Grafana dashboard

Change:

- dashboard refresh: `15s` -> `5s`

### Why this matters

With the current `15s` collection plus `15s` dashboard refresh, changes can take too long to appear and the wallboard feels unresponsive. Moving to `5s` gives a much better live-demo cadence without becoming overly aggressive for this project scale.

---

## Layout Plan

### Row 1: Command row

- `P95 Latency`
- `Throughput`
- `Active Breaches`
- `Facilities at Risk`

All four should be large stat cards with background color thresholds.

### Row 2: Live health row

- latency trend
- throughput vs lag trend
- hot batch duration
- cold batch duration

### Row 3: Risk and ranking row

- top risky facilities
- top risky devices
- processed vs invalid vs deduplicated
- query health / pod restarts / freshness

### Row 4: Incident banner

One full-width stat or text panel reflecting the current system state. Its color should change visibly during stress or failure demonstrations.

---

## Demo Flow Support

The redesigned dashboard should support the demo script directly.

### 1. Show data entering

Use:

- throughput
- processed volume trend
- latest batch freshness

### 2. Show metrics live

Use:

- top-row stat cards
- fast refresh cadence
- live trend movement

### 3. Trigger scaling or failure

Use:

- consumer lag
- query health
- incident banner
- active breaches / facilities at risk where relevant

### 4. Show recovery

Use:

- query count returning to healthy
- lag returning to zero
- freshness restoring
- banner state returning to normal

---

## Implementation Shape

The redesign should remain focused and modify only the self-managed observability path used for the demo.

### Files likely to change

- `infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml`
- `infra/monitoring/prometheus/configmap-prometheus.yaml`
- `infra/monitoring/README.md`
- `services/stream-processor/job.py`
- relevant monitoring / stream tests

### Boundaries

The redesign should not:

- build a separate custom frontend
- change the evaluation controller contract
- alter the core pipeline behavior just for presentation
- introduce a notification system beyond the current lightweight alert rules

Instead, it should improve the live observability surface and the dashboard’s ability to communicate pipeline state clearly.

---

## Reliability And Error Handling

The dashboard should remain useful even during partial outage conditions.

Design expectations:

- KPI cards should fall back to the last seen value rather than disappear immediately
- freshness panels should make stale metrics obvious
- the incident banner should reflect degraded state even when trend panels flatten

This is especially important for failure-recovery demos where the purpose is to show both disruption and restoration clearly.

---

## Testing Strategy

Implementation should include checks for:

- Grafana dashboard JSON remaining syntactically valid
- dashboard queries referencing only real metrics
- new stream metrics being emitted correctly
- Prometheus config still rendering after scrape interval changes
- dashboard docs reflecting the new wallboard layout and refresh behavior

It is not necessary to automate pixel-perfect dashboard appearance. The test goal is configuration correctness and metric availability, not visual snapshot testing.
