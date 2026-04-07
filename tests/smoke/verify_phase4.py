#!/usr/bin/env python3
import json
import subprocess
import sys
import time
from pathlib import Path


def fail(message: str) -> int:
    print(message, file=sys.stderr)
    return 1


def wait_for_files(directory: Path, pattern: str, timeout_seconds: int) -> list[Path]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if directory.exists():
            files = sorted(path for path in directory.glob(pattern) if path.is_file())
            if files:
                return files
        time.sleep(2)
    return []


def read_redis_key(key: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "redis", "redis-cli", "GET", key],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def wait_for_redis_key(key: str, timeout_seconds: int) -> str:
    deadline = time.time() + timeout_seconds
    last_error = ""

    while time.time() < deadline:
        try:
            payload = read_redis_key(key)
        except RuntimeError as exc:
            last_error = str(exc)
            time.sleep(2)
            continue

        if payload:
            return payload
        time.sleep(2)

    if last_error:
        raise RuntimeError(last_error)
    return ""


def main() -> int:
    project_root = Path(__file__).resolve().parents[2]
    workload = project_root / "data" / "workloads" / "dev" / "events.ndjson"
    processed_dir = project_root / "data" / "output" / "processed"
    invalid_dir = project_root / "data" / "output" / "invalid"

    if not workload.exists():
        return fail(f"Missing workload file: {workload}")

    processed_files = wait_for_files(processed_dir, "*.parquet", timeout_seconds=30)
    if not processed_files:
        return fail(f"No processed parquet files found in {processed_dir}")

    try:
        redis_payload = wait_for_redis_key("device:status:FR-0102", timeout_seconds=30)
    except RuntimeError as exc:
        return fail(f"Unable to read Redis key device:status:FR-0102: {exc}")

    if not redis_payload:
        return fail("Redis key device:status:FR-0102 is empty or missing")

    try:
        payload = json.loads(redis_payload)
    except json.JSONDecodeError as exc:
        return fail(f"Redis payload is not valid JSON: {exc}")

    if payload.get("device_id") != "FR-0102":
        return fail(f"Unexpected Redis payload device_id: {payload}")

    invalid_files = []
    if invalid_dir.exists():
        invalid_files = sorted(path for path in invalid_dir.glob("*.json") if path.is_file())

    print(f"Smoke verification passed. Processed files: {len(processed_files)}")
    print("Redis payload verified for device:status:FR-0102")
    print(f"Invalid files present: {len(invalid_files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
