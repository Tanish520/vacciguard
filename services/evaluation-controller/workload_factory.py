from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone


TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

DEFAULT_METADATA = {
    "workload_family_version": "evaluation-workload-v1",
    "devices": 30,
    "duration_minutes": 12,
    "normal_eps": 100.0,
    "spike_eps": 1000.0,
    "seed": 20260408,
}

SCENARIO_DEFAULTS = {
    "normal": {
        "target_eps": 100.0,
        "mix_targets": {"duplicates_pct": 0.05, "late_pct": 0.03, "invalid_pct": 0.02},
    },
    "spike": {
        "target_eps": 1000.0,
        "mix_targets": {"duplicates_pct": 0.05, "late_pct": 0.03, "invalid_pct": 0.02},
    },
    "failure-recovery": {
        "target_eps": 100.0,
        "mix_targets": {"duplicates_pct": 0.05, "late_pct": 0.03, "invalid_pct": 0.02},
        "fault_model": {"type": "stream-processor-restart", "offset_minutes": 6},
    },
}

BASE_START_TIME = datetime(2026, 4, 8, 10, 0, 0, tzinfo=timezone.utc)
FACILITY_IDS = [f"FAC-{index:04d}" for index in range(1, 11)]


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)


def parse_timestamp(value: str) -> datetime:
    return datetime.strptime(value, TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)


def build_workload_family_metadata() -> dict[str, object]:
    return dict(DEFAULT_METADATA)


def build_scenario_manifest(
    scenario: str, metadata: dict[str, object], event_count: int
) -> dict[str, object]:
    scenario_defaults = SCENARIO_DEFAULTS[scenario]
    manifest = {
        "workload_family_version": metadata["workload_family_version"],
        "scenario": scenario,
        "devices": metadata["devices"],
        "duration_minutes": metadata["duration_minutes"],
        "target_eps": scenario_defaults["target_eps"],
        "mix_targets": dict(scenario_defaults["mix_targets"]),
        "event_count": event_count,
    }
    if "fault_model" in scenario_defaults:
        manifest["fault_model"] = dict(scenario_defaults["fault_model"])
    return manifest


def workload_metadata(*, duration_minutes: int | None = None) -> dict[str, object]:
    metadata = build_workload_family_metadata()
    if duration_minutes is not None:
        metadata["duration_minutes"] = duration_minutes
    return metadata


def _device_profile(device_number: int) -> dict[str, object]:
    facility_index = (device_number - 1) // 3
    return {
        "device_id": f"FR-{100 + device_number:04d}",
        "facility_id": FACILITY_IDS[facility_index],
        "location_lat": 24.0 + device_number / 1000.0,
        "location_lon": 73.0 + device_number / 1000.0,
        "min_temp_c": 2.0,
        "max_temp_c": 8.0,
    }


def _base_event(
    index: int, total_events: int, eps: float, metadata: dict[str, object], scenario: str
) -> dict[str, object]:
    device_number = (index % int(metadata["devices"])) + 1
    profile = _device_profile(device_number)
    event_time = BASE_START_TIME + timedelta(seconds=index / eps)
    cycle_position = (index // int(metadata["devices"])) % 120
    in_breach_window = device_number in {7, 14, 21} and 24 <= cycle_position < 42

    temperature = 5.0 + ((device_number % 5) - 2) * 0.2
    if in_breach_window:
        temperature = 9.2 + (device_number % 3) * 0.4

    return {
        "event_id": f"{scenario}-evt-{index + 1:06d}",
        "device_id": profile["device_id"],
        "event_time": format_timestamp(event_time),
        "temperature_c": round(temperature, 1),
        "door_open": device_number in {7, 14, 21} and 26 <= cycle_position < 39,
        "battery_pct": max(
            12,
            92 - ((index // int(metadata["devices"])) % 75) - (device_number % 4),
        ),
        "location_lat": profile["location_lat"],
        "location_lon": profile["location_lon"],
        "facility_id": profile["facility_id"],
    }


def _drop_helper_fields(event: dict[str, object]) -> dict[str, object]:
    event = dict(event)
    event.pop("facility_id", None)
    return event


def generate_scenario(scenario: str, metadata: dict[str, object]) -> list[dict[str, object]]:
    random.seed(f"{metadata['seed']}:{scenario}")
    total_seconds = int(metadata["duration_minutes"]) * 60
    eps = float(SCENARIO_DEFAULTS[scenario]["target_eps"])
    base_unique_count = int(total_seconds * eps)
    base_events = [
        _drop_helper_fields(_base_event(index, base_unique_count, eps, metadata, scenario))
        for index in range(base_unique_count)
    ]

    mix = SCENARIO_DEFAULTS[scenario]["mix_targets"]
    duplicate_count = int(base_unique_count * mix["duplicates_pct"])
    late_count = int(base_unique_count * mix["late_pct"])
    invalid_count = int(base_unique_count * mix["invalid_pct"])

    events = list(base_events)

    for duplicate_index in random.sample(range(base_unique_count), duplicate_count):
        duplicate_event = dict(base_events[duplicate_index])
        duplicate_event["replay_label"] = "duplicate"
        events.append(duplicate_event)

    for offset, late_index in enumerate(random.sample(range(base_unique_count), late_count), start=1):
        late_event = dict(base_events[late_index])
        late_event["event_id"] = f"{late_event['event_id']}-late-{offset:04d}"
        late_event["event_time"] = format_timestamp(
            parse_timestamp(str(late_event["event_time"])) - timedelta(minutes=12)
        )
        late_event["replay_label"] = "late"
        events.append(late_event)

    for offset, invalid_index in enumerate(
        random.sample(range(base_unique_count), invalid_count), start=1
    ):
        invalid_event = dict(base_events[invalid_index])
        invalid_event["event_id"] = f"{invalid_event['event_id']}-invalid-{offset:04d}"
        invalid_event["temperature_c"] = None
        invalid_event["replay_label"] = "invalid"
        events.append(invalid_event)

    return events


def shift_event_times(
    events: list[dict[str, object]], target_start_time: datetime
) -> list[dict[str, object]]:
    earliest_event_time = min(
        parse_timestamp(str(event["event_time"]))
        for event in events
        if event.get("event_time")
    )
    delta = target_start_time - earliest_event_time

    shifted_events: list[dict[str, object]] = []
    for event in events:
        shifted_event = dict(event)
        if shifted_event.get("event_time"):
            shifted_event["event_time"] = format_timestamp(
                parse_timestamp(str(shifted_event["event_time"])) + delta
            )
        shifted_events.append(shifted_event)
    return shifted_events


def build_run_scoped_workload(
    *,
    scenario: str,
    target_start_time: datetime | None = None,
    duration_minutes: int | None = None,
) -> tuple[str, dict[str, object]]:
    metadata = workload_metadata(duration_minutes=duration_minutes)
    events = generate_scenario(scenario, metadata)
    manifest = build_scenario_manifest(scenario, metadata, len(events))
    shifted_events = shift_event_times(
        events,
        target_start_time or datetime.now(timezone.utc),
    )
    workload_ndjson = (
        "\n".join(json.dumps(event, separators=(",", ":")) for event in shifted_events) + "\n"
    )
    return workload_ndjson, manifest
