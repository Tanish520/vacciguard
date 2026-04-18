#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

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

    if not invalid.empty and "event_date" in invalid.columns:
        invalid = invalid.copy()
        invalid["event_date"] = invalid["event_date"].astype("string").fillna("")

    if not breach_windows.empty and "event_date" in breach_windows.columns:
        breach_windows = breach_windows.copy()
        breach_windows["event_date"] = breach_windows["event_date"].astype("string").fillna("")

    if invalid.empty or {"event_date", "invalid_reason"} - set(invalid.columns):
        invalid_summary = pd.DataFrame(
            columns=[
                "event_date",
                "unknown_device",
                "corrupt_payload",
                "missing_fields",
                "invalid_events_total",
            ]
        )
    else:
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

    merged = invalid_summary.merge(breach_summary, on="event_date", how="outer")
    merged = merged.fillna(0).infer_objects(copy=False)
    renamed = merged.rename(
        columns={
            "unknown_device": "invalid_unknown_device",
            "corrupt_payload": "invalid_corrupt_payload",
            "missing_fields": "invalid_missing_fields",
        }
    )
    return renamed[AUDIT_COLUMNS]


def read_processed_input(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


def read_json_lines_input(path: str) -> pd.DataFrame:
    input_path = Path(path)
    if input_path.is_dir():
        frames = [
            pd.read_json(json_path, lines=True)
            for json_path in sorted(input_path.glob("*.json"))
        ]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    return pd.read_json(input_path, lines=True)


def write_summary_output(frame: pd.DataFrame, output_path: str) -> None:
    destination = Path(output_path)
    destination.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(destination / "summary.parquet", index=False)


def run_batch_job(
    *,
    processed_input: str,
    invalid_input: str,
    breach_windows_input: str,
    compliance_output: str,
    audit_output: str,
) -> None:
    processed = read_processed_input(processed_input)
    invalid = read_json_lines_input(invalid_input)
    breach_windows = read_json_lines_input(breach_windows_input)

    compliance_summary = build_daily_compliance_summary(processed)
    audit_summary = build_daily_audit_summary(invalid, breach_windows)

    write_summary_output(compliance_summary, compliance_output)
    write_summary_output(audit_summary, audit_output)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed-input", required=True)
    parser.add_argument("--invalid-input", required=True)
    parser.add_argument("--breach-windows-input", required=True)
    parser.add_argument("--compliance-output", required=True)
    parser.add_argument("--audit-output", required=True)
    args = parser.parse_args()
    run_batch_job(
        processed_input=args.processed_input,
        invalid_input=args.invalid_input,
        breach_windows_input=args.breach_windows_input,
        compliance_output=args.compliance_output,
        audit_output=args.audit_output,
    )


if __name__ == "__main__":
    main()
