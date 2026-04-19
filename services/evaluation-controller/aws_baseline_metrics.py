from __future__ import annotations

import re


REPLAY_LOADED_RE = re.compile(r"Loaded (?P<input_events>\d+) events")
REPLAY_COMPLETE_RE = re.compile(
    r"Replay complete: (?P<input_events>\d+) events in (?P<duration_seconds>\d+(?:\.\d+)?)s  avg (?P<throughput_eps>\d+(?:\.\d+)?) eps"
)
STREAM_SUMMARY_RE = re.compile(
    r"Batch (?P<batch_id>\d+) summary valid=(?P<valid_count>\d+) invalid=(?P<invalid_count>\d+) "
    r"deduplicated=(?P<deduplicated_count>\d+) breach=(?P<breach_count>\d+) "
    r"processed=(?P<processed_count>\d+) avg_e2e_latency_s=(?P<avg_latency>n/a|\d+(?:\.\d+)?) "
    r"p95_e2e_latency_s=(?P<p95_latency>n/a|\d+(?:\.\d+)?)(?: "
    r"p99_e2e_latency_s=(?P<p99_latency>n/a|\d+(?:\.\d+)?))?"
)
PROMETHEUS_SAMPLE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)\s+(?P<value>-?\d+(?:\.\d+)?)$"
)
PROMETHEUS_STREAM_METRICS = {
    "vacciguard_stream_processed_events_total": "processed_events",
    "vacciguard_stream_invalid_events_total": "invalid_events",
    "vacciguard_stream_deduplicated_events_total": "deduplicated_events",
    "vacciguard_stream_breach_events_total": "breach_events",
    "vacciguard_stream_latest_batch_avg_latency_seconds": "avg_end_to_end_latency_seconds",
    "vacciguard_stream_latest_batch_p95_latency_seconds": "p95_end_to_end_latency_seconds",
    "vacciguard_stream_latest_batch_p99_latency_seconds": "p99_end_to_end_latency_seconds",
    "vacciguard_stream_latest_batch_event_time_lag_p95_seconds": "event_time_lag_p95_seconds",
    "vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds": "ingest_to_redis_p95_seconds",
    "vacciguard_stream_hot_batch_duration_seconds": "hot_batch_duration_seconds",
    "vacciguard_stream_cold_batch_duration_seconds": "cold_batch_duration_seconds",
    "vacciguard_stream_observed_throughput_eps": "observed_throughput_eps",
    "vacciguard_stream_pod_restart_count": "pod_restart_count",
    "vacciguard_stream_queries_active": "queries_active",
    "vacciguard_stream_cumulative_processed_events": "cumulative_processed_events",
    "vacciguard_stream_watermark_delay_seconds": "watermark_delay_seconds",
    "vacciguard_stream_consumer_lag_records": "consumer_lag_records",
}


def _base_metrics() -> dict[str, float | int | str | None]:
    return {
        "avg_end_to_end_latency_seconds": None,
        "p95_end_to_end_latency_seconds": None,
        "p99_end_to_end_latency_seconds": None,
        "throughput_eps": None,
        "input_events": None,
        "processed_events": 0,
        "cumulative_processed_events": 0,
        "invalid_events": 0,
        "deduplicated_events": 0,
        "breach_events": 0,
        "hot_batch_duration_seconds": None,
        "cold_batch_duration_seconds": None,
        "observed_throughput_eps": None,
        "pod_restart_count": None,
        "queries_active": None,
        "event_time_lag_p95_seconds": None,
        "ingest_to_redis_p95_seconds": None,
        "watermark_delay_seconds": None,
        "consumer_lag_records": None,
        "invalid_rate_pct": None,
        "deduplication_rate_pct": None,
        "processed_rate_pct": None,
        "stream_metrics_source": "unavailable",
        "spike_result": "Not run",
        "recovery_time_after_failure": "Not run",
        "cost_per_run": "Not run",
        "cost_per_gb_processed": "Not run",
    }


def _maybe_float(value: str | None) -> float | None:
    if value in (None, "n/a"):
        return None
    return float(value)


def _parse_replay_metrics(
    replay_logs: str,
    metrics: dict[str, float | int | str | None],
) -> None:
    loaded_match = REPLAY_LOADED_RE.search(replay_logs)
    if loaded_match:
        metrics["input_events"] = int(loaded_match.group("input_events"))

    complete_match = REPLAY_COMPLETE_RE.search(replay_logs)
    if complete_match:
        metrics["input_events"] = int(complete_match.group("input_events"))
        metrics["throughput_eps"] = float(complete_match.group("throughput_eps"))


def _parse_stream_summary_metrics(
    stream_logs: str,
) -> dict[str, float | int | str | None] | None:
    stream_matches = list(STREAM_SUMMARY_RE.finditer(stream_logs))
    if not stream_matches:
        return None

    parsed: dict[str, float | int | str | None] = {
        "processed_events": 0,
        "invalid_events": 0,
        "deduplicated_events": 0,
        "breach_events": 0,
        "avg_end_to_end_latency_seconds": None,
        "p95_end_to_end_latency_seconds": None,
        "p99_end_to_end_latency_seconds": None,
    }
    weighted_latency_sum = 0.0
    weighted_latency_count = 0
    p95_candidates: list[float] = []
    p99_candidates: list[float] = []

    for match in stream_matches:
        processed_count = int(match.group("processed_count"))
        parsed["processed_events"] += processed_count
        parsed["invalid_events"] += int(match.group("invalid_count"))
        parsed["deduplicated_events"] += int(match.group("deduplicated_count"))
        parsed["breach_events"] += int(match.group("breach_count"))

        avg_latency = _maybe_float(match.group("avg_latency"))
        if avg_latency is not None and processed_count > 0:
            weighted_latency_sum += avg_latency * processed_count
            weighted_latency_count += processed_count

        p95_latency = _maybe_float(match.group("p95_latency"))
        if p95_latency is not None:
            p95_candidates.append(p95_latency)

        p99_latency = _maybe_float(match.group("p99_latency"))
        if p99_latency is not None:
            p99_candidates.append(p99_latency)

    if weighted_latency_count > 0:
        parsed["avg_end_to_end_latency_seconds"] = round(
            weighted_latency_sum / weighted_latency_count, 2
        )
    if p95_candidates:
        parsed["p95_end_to_end_latency_seconds"] = round(max(p95_candidates), 2)
    if p99_candidates:
        parsed["p99_end_to_end_latency_seconds"] = round(max(p99_candidates), 2)
    return parsed


def _parse_stream_metrics_payload(
    stream_metrics_payload: str,
) -> dict[str, float | int | str | None] | None:
    if not stream_metrics_payload.strip():
        return None

    samples: dict[str, float] = {}
    for line in stream_metrics_payload.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = PROMETHEUS_SAMPLE_RE.match(stripped)
        if not match:
            continue
        samples[match.group("name")] = float(match.group("value"))

    mapped = {
        destination: samples[source]
        for source, destination in PROMETHEUS_STREAM_METRICS.items()
        if source in samples
    }
    if not mapped:
        return None

    metrics: dict[str, float | int | str | None] = {}
    for key, value in mapped.items():
        if key.endswith("_events") or key == "consumer_lag_records":
            metrics[key] = int(value)
        else:
            metrics[key] = round(value, 2)
    return metrics


def extract_metrics(
    replay_logs: str,
    stream_logs: str,
    *,
    stream_metrics_payload: str = "",
) -> dict[str, float | int | str | None]:
    metrics = _base_metrics()
    _parse_replay_metrics(replay_logs, metrics)

    stream_metrics = _parse_stream_metrics_payload(stream_metrics_payload)
    stream_summary_metrics = _parse_stream_summary_metrics(stream_logs)
    if stream_metrics is not None:
        metrics.update(stream_metrics)
        if stream_summary_metrics is not None:
            # Preserve endpoint latency values when the cold-path log summary only
            # reports "n/a". The hot path now owns the SLA metric, so the logs
            # should only override it when they carry an actual numeric summary.
            avg_latency = stream_summary_metrics.get("avg_end_to_end_latency_seconds")
            p95_latency = stream_summary_metrics.get("p95_end_to_end_latency_seconds")
            p99_latency = stream_summary_metrics.get("p99_end_to_end_latency_seconds")
            if avg_latency is not None:
                metrics["avg_end_to_end_latency_seconds"] = avg_latency
            if p95_latency is not None:
                metrics["p95_end_to_end_latency_seconds"] = p95_latency
            if p99_latency is not None:
                metrics["p99_end_to_end_latency_seconds"] = p99_latency
        metrics["stream_metrics_source"] = "metrics_endpoint"
    elif stream_summary_metrics is not None:
        metrics.update(stream_summary_metrics)
        metrics["stream_metrics_source"] = "stream_logs"

    input_events = metrics.get("input_events")
    if isinstance(input_events, int) and input_events > 0:
        metrics["invalid_rate_pct"] = round(
            (int(metrics.get("invalid_events", 0)) / input_events) * 100,
            2,
        )
        metrics["deduplication_rate_pct"] = round(
            (int(metrics.get("deduplicated_events", 0)) / input_events) * 100,
            2,
        )
        metrics["processed_rate_pct"] = round(
            (int(metrics.get("processed_events", 0)) / input_events) * 100,
            2,
        )

    return metrics


def _format_metric(value: float | int | str | None, suffix: str = "") -> str:
    if value is None:
        return "Not measured"
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return f"{value}{suffix}"
    return f"{value:.2f}{suffix}"


def render_markdown_table(metrics: dict[str, float | int | str | None]) -> str:
    rows = [
        ("Workload family version", _format_metric(metrics.get("workload_family_version"))),
        ("Scenario", _format_metric(metrics.get("scenario"))),
        ("Configured replay rate", _format_metric(metrics.get("configured_events_per_second"), " events/s")),
        ("Stream metrics source", _format_metric(metrics.get("stream_metrics_source", "unavailable"))),
        ("Avg end-to-end latency", _format_metric(metrics.get("avg_end_to_end_latency_seconds"), " s")),
        ("P95 latency", _format_metric(metrics.get("p95_end_to_end_latency_seconds"), " s")),
        ("P99 latency", _format_metric(metrics.get("p99_end_to_end_latency_seconds"), " s")),
        ("Throughput", _format_metric(metrics.get("throughput_eps"), " events/s")),
        ("Input events", _format_metric(metrics.get("input_events"))),
        ("Processed events", _format_metric(metrics.get("processed_events"))),
        ("Cumulative processed events", _format_metric(metrics.get("cumulative_processed_events"))),
        ("Processed rate", _format_metric(metrics.get("processed_rate_pct"), "%")),
        ("Invalid events", _format_metric(metrics.get("invalid_events"))),
        ("Invalid rate", _format_metric(metrics.get("invalid_rate_pct"), "%")),
        ("Deduplicated events", _format_metric(metrics.get("deduplicated_events"))),
        ("Deduplication rate", _format_metric(metrics.get("deduplication_rate_pct"), "%")),
        ("Breach events", _format_metric(metrics.get("breach_events"))),
        ("Pod restart count", _format_metric(metrics.get("pod_restart_count"))),
        ("Queries active", _format_metric(metrics.get("queries_active"))),
        ("Hot batch duration", _format_metric(metrics.get("hot_batch_duration_seconds"), " s")),
        ("Cold batch duration", _format_metric(metrics.get("cold_batch_duration_seconds"), " s")),
        ("Observed throughput", _format_metric(metrics.get("observed_throughput_eps"), " events/s")),
        ("Event-time lag P95", _format_metric(metrics.get("event_time_lag_p95_seconds"), " s")),
        ("Ingest-to-Redis P95", _format_metric(metrics.get("ingest_to_redis_p95_seconds"), " s")),
        ("Watermark delay", _format_metric(metrics.get("watermark_delay_seconds"), " s")),
        ("Consumer lag", _format_metric(metrics.get("consumer_lag_records"), " records")),
        ("Processed output objects", _format_metric(metrics.get("processed_output_objects"))),
        ("Invalid output objects", _format_metric(metrics.get("invalid_output_objects"))),
        ("Breach window output objects", _format_metric(metrics.get("breach_window_output_objects"))),
        ("Pipeline success", _format_metric(metrics.get("pipeline_success"))),
        ("Controller job success", _format_metric(metrics.get("controller_job_success"))),
        ("Replay job success", _format_metric(metrics.get("replay_job_success"))),
        ("10x spike success/failure", _format_metric(metrics.get("spike_result", "Not run"))),
        ("Recovery time after failure", _format_metric(metrics.get("recovery_time_after_failure", "Not run"))),
        ("Cost per run", _format_metric(metrics.get("cost_per_run", "Not run"))),
        ("Cost per GB processed", _format_metric(metrics.get("cost_per_gb_processed", "Not run"))),
    ]
    lines = ["| Metric | Baseline |", "|---|---:|"]
    lines.extend(f"| {label} | {value} |" for label, value in rows)
    return "\n".join(lines)
