#!/usr/bin/env python3
from __future__ import annotations

import argparse

import pandas as pd


COMPLIANCE_COLUMNS = [
    "event_date",
    "facility_id",
    "facility_name",
    "total_processed_events",
    "safe_events",
    "breach_events",
    "breach_rate_pct",
    "avg_temperature_c",
    "min_temperature_c",
    "max_temperature_c",
    "unique_devices_seen",
]

AUDIT_COLUMNS = [
    "event_date",
    "invalid_events_total",
    "invalid_unknown_device",
    "invalid_corrupt_payload",
    "invalid_missing_fields",
    "breach_window_count",
    "facilities_with_breaches",
    "devices_with_repeated_breaches",
]


def build_daily_compliance_summary(processed: pd.DataFrame) -> pd.DataFrame:
    if processed.empty:
        return pd.DataFrame(columns=COMPLIANCE_COLUMNS)

    summary = (
        processed.groupby(["event_date", "facility_id", "facility_name"], dropna=False)
        .agg(
            total_processed_events=("device_id", "size"),
            safe_events=("breach_status", lambda values: int((values == "safe").sum())),
            breach_events=("breach_status", lambda values: int((values == "breach").sum())),
            avg_temperature_c=("temperature_c", "mean"),
            min_temperature_c=("temperature_c", "min"),
            max_temperature_c=("temperature_c", "max"),
            unique_devices_seen=("device_id", "nunique"),
        )
        .reset_index()
    )
    summary["breach_rate_pct"] = (
        summary["breach_events"] / summary["total_processed_events"] * 100.0
    ).round(2)
    return summary[COMPLIANCE_COLUMNS]


def build_daily_audit_summary(
    invalid: pd.DataFrame, breach_windows: pd.DataFrame
) -> pd.DataFrame:
    if invalid.empty and breach_windows.empty:
        return pd.DataFrame(columns=AUDIT_COLUMNS)

    invalid_summary = (
        invalid.groupby("event_date", dropna=False)["invalid_reason"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    for reason in ("unknown_device", "corrupt_payload", "missing_fields"):
        if reason not in invalid_summary.columns:
            invalid_summary[reason] = 0
    invalid_summary["invalid_events_total"] = invalid_summary[
        ["unknown_device", "corrupt_payload", "missing_fields"]
    ].sum(axis=1)

    if breach_windows.empty:
        breach_summary = pd.DataFrame(
            columns=[
                "event_date",
                "breach_window_count",
                "facilities_with_breaches",
                "devices_with_repeated_breaches",
            ]
        )
    else:
        repeated_devices = (
            breach_windows.groupby(["event_date", "device_id"], dropna=False)
            .size()
            .reset_index(name="occurrences")
            .groupby("event_date", dropna=False)["occurrences"]
            .apply(lambda values: int((values > 1).sum()))
            .reset_index(name="devices_with_repeated_breaches")
        )
        breach_summary = (
            breach_windows.groupby("event_date", dropna=False)
            .agg(
                breach_window_count=("device_id", "size"),
                facilities_with_breaches=("facility_id", "nunique"),
            )
            .reset_index()
            .merge(repeated_devices, on="event_date", how="left")
        )

    merged = invalid_summary.merge(breach_summary, on="event_date", how="outer").fillna(0)
    renamed = merged.rename(
        columns={
            "unknown_device": "invalid_unknown_device",
            "corrupt_payload": "invalid_corrupt_payload",
            "missing_fields": "invalid_missing_fields",
        }
    )
    return renamed[AUDIT_COLUMNS]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    raise SystemExit("CLI wiring comes later")


if __name__ == "__main__":
    main()
