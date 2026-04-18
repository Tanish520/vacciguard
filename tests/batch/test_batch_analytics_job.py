import importlib.util
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[2] / "services" / "batch-analytics" / "job.py"
SPEC = importlib.util.spec_from_file_location("batch_analytics_job", MODULE_PATH)
batch_job = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(batch_job)


def test_build_daily_compliance_summary():
    processed = pd.DataFrame(
        [
            {
                "event_date": "2026-04-17",
                "facility_id": "fac-1",
                "facility_name": "Clinic A",
                "device_id": "dev-1",
                "temperature_c": 4.0,
                "breach_status": "safe",
            },
            {
                "event_date": "2026-04-17",
                "facility_id": "fac-1",
                "facility_name": "Clinic A",
                "device_id": "dev-2",
                "temperature_c": 9.2,
                "breach_status": "breach",
            },
        ]
    )

    summary = batch_job.build_daily_compliance_summary(processed)

    assert list(summary.columns) == [
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
    assert summary.iloc[0]["total_processed_events"] == 2
    assert summary.iloc[0]["safe_events"] == 1
    assert summary.iloc[0]["breach_events"] == 1
    assert summary.iloc[0]["unique_devices_seen"] == 2


def test_build_daily_audit_summary():
    invalid = pd.DataFrame(
        [
            {"event_date": "2026-04-17", "invalid_reason": "unknown_device"},
            {"event_date": "2026-04-17", "invalid_reason": "missing_fields"},
        ]
    )
    breach_windows = pd.DataFrame(
        [
            {"event_date": "2026-04-17", "facility_id": "fac-1", "device_id": "dev-1"},
            {"event_date": "2026-04-17", "facility_id": "fac-1", "device_id": "dev-1"},
            {"event_date": "2026-04-17", "facility_id": "fac-2", "device_id": "dev-2"},
        ]
    )

    summary = batch_job.build_daily_audit_summary(invalid, breach_windows)

    assert list(summary.columns) == [
        "event_date",
        "invalid_events_total",
        "invalid_unknown_device",
        "invalid_corrupt_payload",
        "invalid_missing_fields",
        "breach_window_count",
        "facilities_with_breaches",
        "devices_with_repeated_breaches",
    ]
    assert summary.iloc[0]["invalid_events_total"] == 2
    assert summary.iloc[0]["invalid_unknown_device"] == 1
    assert summary.iloc[0]["invalid_corrupt_payload"] == 0
    assert summary.iloc[0]["invalid_missing_fields"] == 1
    assert summary.iloc[0]["breach_window_count"] == 3
    assert summary.iloc[0]["facilities_with_breaches"] == 2
    assert summary.iloc[0]["devices_with_repeated_breaches"] == 1


def test_build_daily_audit_summary_handles_empty_invalid_frame_with_breaches():
    invalid = pd.DataFrame()
    breach_windows = pd.DataFrame(
        [
            {"event_date": "2026-04-17", "facility_id": "fac-1", "device_id": "dev-1"},
        ]
    )

    summary = batch_job.build_daily_audit_summary(invalid, breach_windows)

    assert list(summary.columns) == [
        "event_date",
        "invalid_events_total",
        "invalid_unknown_device",
        "invalid_corrupt_payload",
        "invalid_missing_fields",
        "breach_window_count",
        "facilities_with_breaches",
        "devices_with_repeated_breaches",
    ]
    assert summary.iloc[0]["event_date"] == "2026-04-17"
    assert summary.iloc[0]["invalid_events_total"] == 0
    assert summary.iloc[0]["invalid_unknown_device"] == 0
    assert summary.iloc[0]["invalid_corrupt_payload"] == 0
    assert summary.iloc[0]["invalid_missing_fields"] == 0
    assert summary.iloc[0]["breach_window_count"] == 1
    assert summary.iloc[0]["facilities_with_breaches"] == 1
    assert summary.iloc[0]["devices_with_repeated_breaches"] == 0


def test_build_daily_audit_summary_handles_missing_event_date():
    invalid = pd.DataFrame(
        [{"event_date": None, "invalid_reason": "unknown_device"}]
    )
    breach_windows = pd.DataFrame(
        [{"event_date": "2026-04-17", "facility_id": "fac-1", "device_id": "dev-1"}]
    )

    summary = batch_job.build_daily_audit_summary(invalid, breach_windows)

    assert list(summary.columns) == [
        "event_date",
        "invalid_events_total",
        "invalid_unknown_device",
        "invalid_corrupt_payload",
        "invalid_missing_fields",
        "breach_window_count",
        "facilities_with_breaches",
        "devices_with_repeated_breaches",
    ]
    assert len(summary) == 2
    assert set(summary["event_date"]) == {"", "2026-04-17"}


def test_run_batch_job_writes_summary_outputs(tmp_path):
    processed = pd.DataFrame(
        [
            {
                "event_date": "2026-04-17",
                "facility_id": "fac-1",
                "facility_name": "Clinic A",
                "device_id": "dev-1",
                "temperature_c": 4.0,
                "breach_status": "safe",
            }
        ]
    )
    invalid = pd.DataFrame(
        [{"event_date": "2026-04-17", "invalid_reason": "missing_fields"}]
    )
    breach_windows = pd.DataFrame(
        [{"event_date": "2026-04-17", "facility_id": "fac-1", "device_id": "dev-1"}]
    )

    processed_path = tmp_path / "processed.parquet"
    invalid_path = tmp_path / "invalid.json"
    breach_path = tmp_path / "breach.json"
    compliance_output = tmp_path / "compliance"
    audit_output = tmp_path / "audit"

    processed.to_parquet(processed_path, index=False)
    invalid.to_json(invalid_path, orient="records", lines=True)
    breach_windows.to_json(breach_path, orient="records", lines=True)

    batch_job.run_batch_job(
        processed_input=str(processed_path),
        invalid_input=str(invalid_path),
        breach_windows_input=str(breach_path),
        compliance_output=str(compliance_output),
        audit_output=str(audit_output),
    )

    compliance_summary = pd.read_parquet(compliance_output / "summary.parquet")
    audit_summary = pd.read_parquet(audit_output / "summary.parquet")

    assert list(compliance_summary.columns) == [
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
    assert compliance_summary.iloc[0]["facility_id"] == "fac-1"
    assert compliance_summary.iloc[0]["total_processed_events"] == 1
    assert compliance_summary.iloc[0]["safe_events"] == 1
    assert compliance_summary.iloc[0]["breach_events"] == 0
    assert compliance_summary.iloc[0]["unique_devices_seen"] == 1

    assert list(audit_summary.columns) == [
        "event_date",
        "invalid_events_total",
        "invalid_unknown_device",
        "invalid_corrupt_payload",
        "invalid_missing_fields",
        "breach_window_count",
        "facilities_with_breaches",
        "devices_with_repeated_breaches",
    ]
    assert audit_summary.iloc[0]["invalid_events_total"] == 1
    assert audit_summary.iloc[0]["invalid_missing_fields"] == 1
    assert audit_summary.iloc[0]["breach_window_count"] == 1
    assert audit_summary.iloc[0]["facilities_with_breaches"] == 1
    assert audit_summary.iloc[0]["devices_with_repeated_breaches"] == 0
