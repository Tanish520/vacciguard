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
    r"p95_e2e_latency_s=(?P<p95_latency>n/a|\d+(?:\.\d+)?)"
)


def _maybe_float(value: str | None) -> float | None:
    if value in (None, "n/a"):
        return None
    return float(value)


def extract_metrics(
    replay_logs: str, stream_logs: str
) -> dict[str, float | int | str | None]:
    metrics: dict[str, float | int | str | None] = {
        "avg_end_to_end_latency_seconds": None,
        "p95_end_to_end_latency_seconds": None,
        "throughput_eps": None,
        "input_events": None,
        "processed_events": 0,
        "invalid_events": 0,
        "deduplicated_events": 0,
        "breach_events": 0,
        "spike_result": "Not run",
        "recovery_time_after_failure": "Not run",
        "cost_per_run": "Not run",
        "cost_per_gb_processed": "Not run",
    }

    loaded_match = REPLAY_LOADED_RE.search(replay_logs)
    if loaded_match:
        metrics["input_events"] = int(loaded_match.group("input_events"))

    complete_match = REPLAY_COMPLETE_RE.search(replay_logs)
    if complete_match:
        metrics["input_events"] = int(complete_match.group("input_events"))
        metrics["throughput_eps"] = float(complete_match.group("throughput_eps"))

    stream_matches = list(STREAM_SUMMARY_RE.finditer(stream_logs))
    if stream_matches:
        weighted_latency_sum = 0.0
        weighted_latency_count = 0
        p95_candidates: list[float] = []
        for match in stream_matches:
            processed_count = int(match.group("processed_count"))
            metrics["processed_events"] += processed_count
            metrics["invalid_events"] += int(match.group("invalid_count"))
            metrics["deduplicated_events"] += int(match.group("deduplicated_count"))
            metrics["breach_events"] += int(match.group("breach_count"))

            avg_latency = _maybe_float(match.group("avg_latency"))
            if avg_latency is not None and processed_count > 0:
                weighted_latency_sum += avg_latency * processed_count
                weighted_latency_count += processed_count

            p95_latency = _maybe_float(match.group("p95_latency"))
            if p95_latency is not None:
                p95_candidates.append(p95_latency)

        if weighted_latency_count > 0:
            metrics["avg_end_to_end_latency_seconds"] = round(
                weighted_latency_sum / weighted_latency_count, 2
            )
        if p95_candidates:
            metrics["p95_end_to_end_latency_seconds"] = round(max(p95_candidates), 2)

    return metrics
