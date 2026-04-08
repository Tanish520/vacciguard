from __future__ import annotations

from dataclasses import asdict, dataclass
import re


VALID_PIPELINE_TARGETS = {"baseline", "optimized"}
VALID_SCENARIOS = {"normal", "spike", "failure-recovery"}
RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
RESERVED_REPORT_KEYS = {
    "pipeline_target",
    "scenario",
    "run_id",
    "workload_family_version",
    "bucket_name",
    "kafka_topic",
    "s3_prefix",
    "report_markdown_s3_uri",
    "report_json_s3_uri",
    "status",
    "failure_reason",
}


@dataclass(frozen=True)
class RunContract:
    pipeline_target: str
    scenario: str
    run_id: str
    workload_family_version: str
    bucket_name: str
    kafka_topic: str
    s3_prefix: str
    report_markdown_s3_uri: str
    report_json_s3_uri: str


def normalize_run_id(run_id: str) -> str:
    normalized = run_id.strip().lower()
    if not normalized or not RUN_ID_PATTERN.fullmatch(normalized):
        raise ValueError(f"Invalid run_id: {run_id}")
    return normalized


def resolve_run_contract(
    *,
    pipeline_target: str,
    scenario: str,
    run_id: str,
    workload_family_version: str,
    bucket_name: str,
) -> RunContract:
    if pipeline_target not in VALID_PIPELINE_TARGETS:
        raise ValueError(f"Unsupported pipeline target: {pipeline_target}")
    if scenario not in VALID_SCENARIOS:
        raise ValueError(f"Unsupported scenario: {scenario}")
    run_id = normalize_run_id(run_id)

    s3_prefix = f"evaluations/{pipeline_target}/{scenario}/{run_id}"
    return RunContract(
        pipeline_target=pipeline_target,
        scenario=scenario,
        run_id=run_id,
        workload_family_version=workload_family_version,
        bucket_name=bucket_name,
        kafka_topic=f"vacciguard-eval-{run_id}",
        s3_prefix=s3_prefix,
        report_markdown_s3_uri=f"s3://{bucket_name}/{s3_prefix}/report.md",
        report_json_s3_uri=f"s3://{bucket_name}/{s3_prefix}/report.json",
    )


def build_report_payload(
    *,
    contract: RunContract,
    metrics: dict[str, object],
    status: str,
    failure_reason: str | None,
) -> dict[str, object]:
    payload = asdict(contract)
    colliding_keys = RESERVED_REPORT_KEYS.intersection(metrics)
    if colliding_keys:
        raise ValueError(
            f"Metrics contain reserved report keys: {sorted(colliding_keys)}"
        )
    payload.update(metrics)
    payload["status"] = status
    payload["failure_reason"] = failure_reason
    return payload
