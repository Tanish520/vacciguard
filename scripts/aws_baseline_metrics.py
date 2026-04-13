#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


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


def _maybe_float(value):
    if value in (None, "n/a"):
        return None
    return float(value)


def extract_metrics(replay_logs: str, stream_logs: str) -> dict[str, float | int | str | None]:
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
        p95_candidates = []
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


def _format_metric(value, suffix=""):
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
        ("Avg end-to-end latency", _format_metric(metrics.get("avg_end_to_end_latency_seconds"), " s")),
        ("P95 latency", _format_metric(metrics.get("p95_end_to_end_latency_seconds"), " s")),
        ("Throughput", _format_metric(metrics.get("throughput_eps"), " events/s")),
        ("10x spike success/failure", _format_metric(metrics.get("spike_result", "Not run"))),
        ("Recovery time after failure", _format_metric(metrics.get("recovery_time_after_failure", "Not run"))),
        ("Input events", _format_metric(metrics.get("input_events"))),
        ("Processed events", _format_metric(metrics.get("processed_events"))),
        ("Invalid events", _format_metric(metrics.get("invalid_events"))),
        ("Deduplicated events", _format_metric(metrics.get("deduplicated_events"))),
        ("Breach events", _format_metric(metrics.get("breach_events"))),
        ("Cost per run", _format_metric(metrics.get("cost_per_run", "Not run"))),
        ("Cost per GB processed", _format_metric(metrics.get("cost_per_gb_processed", "Not run"))),
    ]
    lines = ["| Metric | Baseline |", "|---|---:|"]
    lines.extend(f"| {label} | {value} |" for label, value in rows)
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-logs", required=True)
    parser.add_argument("--stream-logs", required=True)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args()

    replay_logs = Path(args.replay_logs).read_text(encoding="utf-8")
    stream_logs = Path(args.stream_logs).read_text(encoding="utf-8")
    metrics = extract_metrics(replay_logs, stream_logs)

    if args.format == "markdown":
        print(render_markdown_table(metrics))
    else:
        print(json.dumps(metrics, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
