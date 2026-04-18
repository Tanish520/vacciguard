#!/usr/bin/env python3
from __future__ import annotations

import argparse
from io import BytesIO
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyarrow import fs
from pyarrow import parquet as pq


COMPLIANCE_COLUMNS = [
    "event_date",
    "facility_id",
    "facility_name",
    "device_id",
    "temperature_c",
    "breach_status",
]

COMPLIANCE_OUTPUT_COLUMNS = [
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

AUDIT_INVALID_COLUMNS = [
    "event_date",
    "invalid_reason",
]

AUDIT_BREACH_COLUMNS = [
    "event_date",
    "facility_id",
    "device_id",
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

AUDIT_COUNT_COLUMNS = [
    "invalid_events_total",
    "invalid_unknown_device",
    "invalid_corrupt_payload",
    "invalid_missing_fields",
    "breach_window_count",
    "facilities_with_breaches",
    "devices_with_repeated_breaches",
]


def is_s3_uri(path: str) -> bool:
    return path.startswith("s3://")


def filesystem_from_uri(path: str) -> tuple[fs.FileSystem, str]:
    return fs.FileSystem.from_uri(path)


def ensure_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.Series(dtype="object")
    return frame[columns]


def build_daily_compliance_summary(processed: pd.DataFrame) -> pd.DataFrame:
    processed = ensure_columns(processed.copy(), COMPLIANCE_COLUMNS)

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
    return summary[COMPLIANCE_OUTPUT_COLUMNS]


def build_daily_audit_summary(
    invalid: pd.DataFrame, breach_windows: pd.DataFrame
) -> pd.DataFrame:
    invalid = ensure_columns(invalid.copy(), AUDIT_INVALID_COLUMNS)
    breach_windows = ensure_columns(breach_windows.copy(), AUDIT_BREACH_COLUMNS)

    invalid["event_date"] = invalid["event_date"].astype("string").fillna("")
    breach_windows["event_date"] = breach_windows["event_date"].astype("string").fillna("")

    invalid_summary = (
        invalid.groupby("event_date", dropna=False)["invalid_reason"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    reason_columns = [column for column in invalid_summary.columns if column != "event_date"]
    for reason in ("unknown_device", "corrupt_payload", "missing_fields"):
        if reason not in invalid_summary.columns:
            invalid_summary[reason] = 0
    invalid_summary["invalid_events_total"] = invalid_summary[reason_columns].sum(axis=1)

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
    renamed[AUDIT_COUNT_COLUMNS] = renamed[AUDIT_COUNT_COLUMNS].astype("int64")
    return renamed[AUDIT_COLUMNS]


def read_processed_input(path: str) -> pd.DataFrame:
    if is_s3_uri(path):
        filesystem, resolved_path = filesystem_from_uri(path)
        table = pq.read_table(resolved_path, filesystem=filesystem)
        return table.to_pandas()
    return pd.read_parquet(path)


def read_json_lines_input(path: str) -> pd.DataFrame:
    if is_s3_uri(path):
        filesystem, resolved_path = filesystem_from_uri(path)
        selector = fs.FileSelector(resolved_path, recursive=True)
        infos = [
            info
            for info in filesystem.get_file_info(selector)
            if info.is_file and info.path.endswith(".json")
        ]
        frames = []
        for info in sorted(infos, key=lambda item: item.path):
            with filesystem.open_input_file(info.path) as stream:
                frames.append(pd.read_json(BytesIO(stream.read()), lines=True))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    input_path = Path(path)
    if input_path.is_dir():
        frames = [
            pd.read_json(json_path, lines=True)
            for json_path in sorted(input_path.rglob("*.json"))
        ]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    return pd.read_json(input_path, lines=True)


def write_summary_output(frame: pd.DataFrame, output_path: str) -> None:
    if is_s3_uri(output_path):
        filesystem, resolved_path = filesystem_from_uri(output_path)
        target_path = resolved_path.rstrip("/") + "/summary.parquet"
        table = pa.Table.from_pandas(frame, preserve_index=False)
        with filesystem.open_output_stream(target_path) as stream:
            pq.write_table(table, stream)
        return

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
