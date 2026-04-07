#!/usr/bin/env python3
"""
Generate the dev workload file for VacciGuard Phase 4 smoke testing.

Output: data/workloads/dev/events.ndjson
Events: 300 (60 seconds of data at 5 eps, 3 devices in round-robin)

Scenarios baked in so the stream processor has something interesting to detect:
  - FR-0101: safe temperature throughout, door opens briefly at t=15s and t=45s
  - FR-0102: temperature breach window from t=20s to t=40s (9-11°C)
  - FR-0103: safe temperature throughout, battery drops below 20% after t=50s

Run from the project root:
  python3 scripts/generate-dev-workload.py
"""

import json
import os
import random
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
TOTAL_EVENTS = 300
EPS          = 5.0
START_TIME   = datetime(2026, 4, 5, 10, 0, 0, tzinfo=timezone.utc)
RANDOM_SEED  = 42  # fixed so the file is reproducible across runs

DEVICES = [
    {
        "device_id":   "FR-0101",
        "lat":         26.9124,
        "lon":         75.7873,
        "breach":      False,
        "door_times":  [(15, 17), (45, 47)],  # (open_start_s, open_end_s)
        "low_battery": False,
    },
    {
        "device_id":   "FR-0102",
        "lat":         24.5854,
        "lon":         73.7125,
        "breach":      True,
        "breach_start": 20,
        "breach_end":   40,
        "door_times":  [(20, 38)],  # door left open during breach
        "low_battery": False,
    },
    {
        "device_id":   "FR-0103",
        "lat":         25.2138,
        "lon":         75.8648,
        "breach":      False,
        "door_times":  [],
        "low_battery": True,     # battery drops below threshold after t=50s
        "low_battery_start": 50,
    },
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def temperature(device, t):
    if device["breach"] and device["breach_start"] <= t <= device["breach_end"]:
        return round(random.uniform(9.0, 11.5), 1)
    return round(random.uniform(2.8, 7.4), 1)


def door_open(device, t):
    for start, end in device["door_times"]:
        if start <= t <= end:
            return True
    return False


def battery(device, t):
    if device["low_battery"] and t >= device.get("low_battery_start", 9999):
        return random.randint(8, 18)
    return random.randint(68, 94)


# ── Generate ──────────────────────────────────────────────────────────────────

random.seed(RANDOM_SEED)

events = []
for i in range(TOTAL_EVENTS):
    t      = i / EPS                        # seconds since start
    device = DEVICES[i % len(DEVICES)]
    ts     = START_TIME + timedelta(seconds=t)

    events.append({
        "event_id":      f"evt-{i + 1:06d}",
        "device_id":     device["device_id"],
        "event_time":    ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "temperature_c": temperature(device, t),
        "door_open":     door_open(device, t),
        "battery_pct":   battery(device, t),
        "location_lat":  device["lat"],
        "location_lon":  device["lon"],
    })

# ── Write ──────────────────────────────────────────────────────────────────────

script_dir = os.path.dirname(os.path.abspath(__file__))
out_path   = os.path.join(script_dir, "..", "data", "workloads", "dev", "events.ndjson")
out_path   = os.path.normpath(out_path)

os.makedirs(os.path.dirname(out_path), exist_ok=True)

with open(out_path, "w", encoding="utf-8") as f:
    for event in events:
        f.write(json.dumps(event) + "\n")

# ── Summary ───────────────────────────────────────────────────────────────────

breach_events = [e for e in events if e["device_id"] == "FR-0102"
                 and e["temperature_c"] > 8.0]
door_events   = [e for e in events if e["door_open"]]
low_bat       = [e for e in events if e["battery_pct"] < 20]

print(f"Generated {len(events)} events → {out_path}")
print(f"  Temperature breach events (FR-0102): {len(breach_events)}")
print(f"  Door-open events:                    {len(door_events)}")
print(f"  Low-battery events (FR-0103):        {len(low_bat)}")
print(f"  Duration: {TOTAL_EVENTS / EPS:.0f}s at {EPS} eps")
