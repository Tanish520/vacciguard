from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re
import sys


CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

import aws_baseline_metrics


VALID_PIPELINE_TARGETS = {"baseline", "optimized"}
VALID_SCENARIOS = {"normal", "spike", "failure-recovery"}
RUN_ID_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
MAX_WORKLOAD_CONFIGMAP_BYTES = 900 * 1024
MAX_KUBERNETES_NAME_LENGTH = 63
WORKLOAD_CONFIGMAP_BASE_NAME = "vacciguard-workload"
REPLAY_JOB_BASE_NAME = "replay-producer"
REPLAY_JOB_METRICS_PORT = 9109
MAX_RUN_ID_LENGTH = min(
    MAX_KUBERNETES_NAME_LENGTH,
    MAX_KUBERNETES_NAME_LENGTH - len(f"{WORKLOAD_CONFIGMAP_BASE_NAME}-"),
    MAX_KUBERNETES_NAME_LENGTH - len(f"{REPLAY_JOB_BASE_NAME}-"),
)
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
    if (
        not normalized
        or len(normalized) > MAX_RUN_ID_LENGTH
        or not RUN_ID_PATTERN.fullmatch(normalized)
    ):
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


def extract_metrics_from_logs(
    replay_logs: str,
    stream_logs: str,
    stream_metrics_payload: str,
    artifact_summary: dict[str, object],
    metadata: dict[str, object],
) -> dict[str, object]:
    metrics = aws_baseline_metrics.extract_metrics(
        replay_logs,
        stream_logs,
        stream_metrics_payload=stream_metrics_payload,
    )
    metrics.update(artifact_summary)
    metrics.update(metadata)
    return metrics


def build_workload_configmap_manifest(
    *, contract: RunContract, workload_ndjson: str
) -> dict[str, object]:
    workload_size = len(workload_ndjson.encode("utf-8"))
    if workload_size > MAX_WORKLOAD_CONFIGMAP_BYTES:
        raise ValueError(
            "Workload ConfigMap content is too large: "
            f"{workload_size} bytes exceeds the {MAX_WORKLOAD_CONFIGMAP_BYTES} byte limit"
        )

    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"{WORKLOAD_CONFIGMAP_BASE_NAME}-{contract.run_id}",
            "namespace": "vacciguard",
        },
        "data": {"events.ndjson": workload_ndjson},
    }


def build_pipeline_config_patch(
    *,
    contract: RunContract,
    app_name: str,
    kafka_bootstrap_servers: str,
    kafka_topic_partitions: str,
    pipeline_mode: str,
    trigger_interval: str,
    watermark_delay: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
) -> dict[str, object]:
    prefix = f"s3a://{contract.bucket_name}/{contract.s3_prefix}"
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "vacciguard-pipeline-config",
            "namespace": "vacciguard",
        },
        "data": {
            "APP_NAME": app_name,
            "KAFKA_TOPIC": contract.kafka_topic,
            "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
            "KAFKA_TOPIC_PARTITIONS": kafka_topic_partitions,
            "KAFKA_STARTING_OFFSETS": "earliest",
            "PIPELINE_MODE": pipeline_mode,
            "TRIGGER_INTERVAL": trigger_interval,
            "WATERMARK_DELAY": watermark_delay,
            "REDIS_HOST": redis_host,
            "REDIS_PORT": redis_port,
            "REDIS_DB": redis_db,
            "PROCESSED_OUTPUT_PATH": f"{prefix}/processed",
            "INVALID_OUTPUT_PATH": f"{prefix}/invalid",
            "BREACH_WINDOW_OUTPUT_PATH": f"{prefix}/breach_windows",
            "CHECKPOINT_ROOT": f"{prefix}/checkpoints",
        },
    }


def build_replay_job_manifest(
    *,
    contract: RunContract,
    replay_image: str,
    kafka_bootstrap_servers: str,
    workload_configmap_name: str | None,
    workload_runtime_path: str = "/data/workloads/evaluation/events.ndjson",
    target_eps: float,
) -> dict[str, object]:
    container_spec = {
        "name": REPLAY_JOB_BASE_NAME,
        "image": replay_image,
        "imagePullPolicy": "Always",
        "ports": [
            {
                "name": "metrics",
                "containerPort": REPLAY_JOB_METRICS_PORT,
            }
        ],
        "env": [
            {
                "name": "KAFKA_BOOTSTRAP_SERVERS",
                "value": kafka_bootstrap_servers,
            },
            {"name": "KAFKA_TOPIC", "value": contract.kafka_topic},
            {
                "name": "WORKLOAD_FILE",
                "value": workload_runtime_path,
            },
            {
                "name": "EVENTS_PER_SECOND",
                "value": str(target_eps),
            },
            {"name": "LOOP", "value": "false"},
        ],
    }

    template_spec: dict[str, object] = {
        "serviceAccountName": "vacciguard-pipeline",
        "restartPolicy": "Never",
        "containers": [container_spec],
    }
    if workload_configmap_name:
        container_spec["volumeMounts"] = [
            {
                "name": "workload",
                "mountPath": "/data/workloads/evaluation",
            }
        ]
        template_spec["volumes"] = [
            {
                "name": "workload",
                "configMap": {"name": workload_configmap_name},
            }
        ]

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{REPLAY_JOB_BASE_NAME}-{contract.run_id}",
            "namespace": "vacciguard",
        },
        "spec": {
            "backoffLimit": 0,
            "template": {
                "metadata": {
                    "labels": {
                        "app": REPLAY_JOB_BASE_NAME,
                        "job_name": REPLAY_JOB_BASE_NAME,
                        "run_id": contract.run_id,
                    }
                },
                "spec": template_spec,
            },
        },
    }


def render_markdown_report(report_payload: dict[str, object]) -> str:
    lines = [
        f"# Evaluation Report: {report_payload['run_id']}",
        "",
        "## Summary",
        f"- pipeline target: {report_payload['pipeline_target']}",
        f"- scenario: {report_payload['scenario']}",
        f"- status: {report_payload['status']}",
        f"- workload family version: {report_payload['workload_family_version']}",
        f"- report markdown: {report_payload['report_markdown_s3_uri']}",
        f"- report json: {report_payload['report_json_s3_uri']}",
        "",
        "## Metrics",
        aws_baseline_metrics.render_markdown_table(report_payload),
    ]
    failure_reason = report_payload.get("failure_reason")
    if failure_reason:
        lines.extend(
            [
                "",
                "## Failure",
                str(failure_reason),
            ]
        )
    return "\n".join(lines) + "\n"
