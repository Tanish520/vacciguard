from __future__ import annotations

import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
import sys


CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

import controller
import workload_factory


NAMESPACE = os.environ.get("KUBERNETES_NAMESPACE", "vacciguard")
PIPELINE_CONFIG_NAME = os.environ.get("PIPELINE_CONFIG_NAME", "vacciguard-pipeline-config")
STREAM_DEPLOYMENT_NAME = os.environ.get("STREAM_DEPLOYMENT_NAME", "stream-processor")
REDIS_ACTIVE_BREACHES_KEY = os.environ.get("REDIS_ACTIVE_BREACHES_KEY", "active_breaches")
STREAM_READY_TIMEOUT_SECONDS = int(os.environ.get("STREAM_READY_TIMEOUT_SECONDS", "180"))
STREAM_READY_GRACE_SECONDS = int(os.environ.get("STREAM_READY_GRACE_SECONDS", "20"))
REPLAY_TIMEOUT_BUFFER_SECONDS = int(os.environ.get("REPLAY_TIMEOUT_BUFFER_SECONDS", "180"))
POST_REPLAY_SETTLE_TIMEOUT_SECONDS = int(
    os.environ.get("POST_REPLAY_SETTLE_TIMEOUT_SECONDS", "120")
)
POST_REPLAY_SETTLE_POLL_SECONDS = int(
    os.environ.get("POST_REPLAY_SETTLE_POLL_SECONDS", "5")
)
POST_REPLAY_STABLE_POLLS = int(os.environ.get("POST_REPLAY_STABLE_POLLS", "3"))
REPLAY_IMAGE = os.environ.get(
    "REPLAY_IMAGE",
    "347038623570.dkr.ecr.ap-south-1.amazonaws.com/vacciguard-minimal-replay-producer:latest",
)
READINESS_LOG_RE = re.compile(r"Stream processor is running with [0-9]+ active queries")
STREAM_METRICS_PORT = int(os.environ.get("STREAM_METRICS_PORT", "9108"))
STREAM_METRICS_TIMEOUT_SECONDS = int(
    os.environ.get("STREAM_METRICS_TIMEOUT_SECONDS", "5")
)


def load_kubernetes_dependencies():
    from kubernetes import client, config
    from kubernetes.client.exceptions import ApiException

    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

    return client, ApiException


def core_api():
    client, _ = load_kubernetes_dependencies()
    return client.CoreV1Api()


def apps_api():
    client, _ = load_kubernetes_dependencies()
    return client.AppsV1Api()


def batch_api():
    client, _ = load_kubernetes_dependencies()
    return client.BatchV1Api()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def read_pipeline_config():
    return core_api().read_namespaced_config_map(PIPELINE_CONFIG_NAME, NAMESPACE)


def latest_stream_pod():
    pods = core_api().list_namespaced_pod(
        NAMESPACE, label_selector="app=stream-processor"
    ).items
    if not pods:
        raise RuntimeError("No stream-processor pods found")
    return max(pods, key=lambda pod: pod.metadata.creation_timestamp or datetime.min.replace(tzinfo=timezone.utc))


def replay_job_name(contract: controller.RunContract) -> str:
    return f"{controller.REPLAY_JOB_BASE_NAME}-{contract.run_id}"


def workload_configmap_name(contract: controller.RunContract) -> str:
    return f"{controller.WORKLOAD_CONFIGMAP_BASE_NAME}-{contract.run_id}"


def load_workload_inputs(contract: controller.RunContract) -> tuple[str, dict[str, object]]:
    duration_minutes = os.environ.get("WORKLOAD_DURATION_MINUTES")
    return workload_factory.build_run_scoped_workload(
        scenario=contract.scenario,
        target_start_time=now_utc(),
        duration_minutes=(
            None if duration_minutes in (None, "") else int(duration_minutes)
        ),
    )


def load_runtime_inputs() -> controller.RunContract:
    return controller.resolve_run_contract(
        pipeline_target=os.environ["PIPELINE_TARGET"],
        scenario=os.environ["SCENARIO"],
        run_id=os.environ["RUN_ID"],
        workload_family_version=os.environ["WORKLOAD_FAMILY_VERSION"],
        bucket_name=os.environ["S3_BUCKET_NAME"],
    )


def is_bootstrap_metrics(metrics: dict[str, object]) -> bool:
    return (
        metrics.get("input_events") is None
        and metrics.get("throughput_eps") is None
        and metrics.get("avg_end_to_end_latency_seconds") is None
        and metrics.get("p95_end_to_end_latency_seconds") is None
        and metrics.get("processed_events") == 0
        and metrics.get("invalid_events") == 0
        and metrics.get("deduplicated_events") == 0
        and metrics.get("breach_events") == 0
    )


def bootstrap_metrics() -> dict[str, object]:
    return {
        "processed_events": 0,
        "invalid_events": 0,
        "deduplicated_events": 0,
        "breach_events": 0,
        "input_events": None,
        "throughput_eps": None,
        "avg_end_to_end_latency_seconds": None,
        "p95_end_to_end_latency_seconds": None,
    }


def controller_mode() -> str:
    return os.environ.get("EVALUATION_CONTROLLER_MODE", "bootstrap").strip().lower()


def reset_redis_state() -> None:
    import redis

    config_map = read_pipeline_config()
    data = config_map.data or {}
    client = redis.Redis(
        host=data["REDIS_HOST"],
        port=int(data["REDIS_PORT"]),
        db=int(data["REDIS_DB"]),
        decode_responses=True,
    )
    keys = list(client.scan_iter(match="device:status:*"))
    if keys:
        client.delete(*keys)
    client.delete(REDIS_ACTIVE_BREACHES_KEY)


def patch_pipeline_config(contract: controller.RunContract) -> None:
    config_map = read_pipeline_config()
    data = config_map.data or {}
    patch_body = controller.build_pipeline_config_patch(
        contract=contract,
        app_name=data["APP_NAME"],
        kafka_bootstrap_servers=data["KAFKA_BOOTSTRAP_SERVERS"],
        kafka_topic_partitions=data.get("KAFKA_TOPIC_PARTITIONS", "1"),
        trigger_interval=data["TRIGGER_INTERVAL"],
        watermark_delay=data["WATERMARK_DELAY"],
        redis_host=data["REDIS_HOST"],
        redis_port=data["REDIS_PORT"],
        redis_db=data["REDIS_DB"],
    )
    core_api().patch_namespaced_config_map(PIPELINE_CONFIG_NAME, NAMESPACE, patch_body)


def restart_stream_processor(contract: controller.RunContract) -> None:
    apps_api().patch_namespaced_deployment(
        STREAM_DEPLOYMENT_NAME,
        NAMESPACE,
        {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": now_utc().isoformat()
                        }
                    }
                }
            }
        },
    )


def wait_for_stream_ready(contract: controller.RunContract) -> None:
    deadline = time.time() + STREAM_READY_TIMEOUT_SECONDS
    ready_since = None
    last_logs = ""

    while time.time() < deadline:
        deployment = apps_api().read_namespaced_deployment(
            STREAM_DEPLOYMENT_NAME,
            NAMESPACE,
        )
        available = deployment.status.available_replicas or 0
        pod = latest_stream_pod()
        ready = any(
            condition.type == "Ready" and condition.status == "True"
            for condition in (pod.status.conditions or [])
        )
        if available >= 1 and ready:
            logs = core_api().read_namespaced_pod_log(
                pod.metadata.name,
                NAMESPACE,
                tail_lines=200,
            )
            last_logs = logs
            if READINESS_LOG_RE.search(logs):
                return
            if ready_since is None:
                ready_since = time.time()
            elif time.time() - ready_since >= STREAM_READY_GRACE_SECONDS:
                return
        time.sleep(5)

    raise RuntimeError(
        "Stream processor did not become ready in time. "
        f"Latest logs:\n{last_logs}"
    )


def launch_replay_job(contract: controller.RunContract) -> None:
    _, ApiException = load_kubernetes_dependencies()
    config_map = read_pipeline_config()
    pipeline_data = config_map.data or {}
    workload_ndjson, workload_manifest = load_workload_inputs(contract)
    target_eps = float(workload_manifest["target_eps"])
    runtime_path = "/data/workloads/evaluation/events.ndjson"
    workload_cm_name = None

    if len(workload_ndjson.encode("utf-8")) <= controller.MAX_WORKLOAD_CONFIGMAP_BYTES:
        workload_manifest_body = controller.build_workload_configmap_manifest(
            contract=contract,
            workload_ndjson=workload_ndjson,
        )
        workload_cm_name = workload_manifest_body["metadata"]["name"]
        try:
            core_api().delete_namespaced_config_map(workload_cm_name, NAMESPACE)
        except ApiException as exc:
            if exc.status != 404:
                raise
        core_api().create_namespaced_config_map(NAMESPACE, workload_manifest_body)
    else:
        import boto3

        runtime_path = (
            f"s3://{contract.bucket_name}/{contract.s3_prefix}/workloads/"
            f"{contract.scenario}.events.ndjson"
        )
        boto3.client("s3").put_object(
            Bucket=contract.bucket_name,
            Key=f"{contract.s3_prefix}/workloads/{contract.scenario}.events.ndjson",
            Body=workload_ndjson.encode("utf-8"),
            ContentType="application/x-ndjson",
        )

    manifest = controller.build_replay_job_manifest(
        contract=contract,
        replay_image=REPLAY_IMAGE,
        kafka_bootstrap_servers=pipeline_data["KAFKA_BOOTSTRAP_SERVERS"],
        workload_configmap_name=workload_cm_name,
        workload_runtime_path=runtime_path,
        target_eps=target_eps,
    )
    try:
        batch_api().delete_namespaced_job(
            replay_job_name(contract),
            NAMESPACE,
            propagation_policy="Background",
        )
    except ApiException as exc:
        if exc.status != 404:
            raise
    batch_api().create_namespaced_job(NAMESPACE, manifest)


def wait_for_replay_completion(contract: controller.RunContract) -> None:
    job_timeout = max(
        180,
        int(
            (
                float(load_workload_inputs(contract)[1]["event_count"])
                / float(load_workload_inputs(contract)[1]["target_eps"])
            )
            + REPLAY_TIMEOUT_BUFFER_SECONDS
        ),
    )
    deadline = time.time() + job_timeout

    while time.time() < deadline:
        job = batch_api().read_namespaced_job(replay_job_name(contract), NAMESPACE)
        status = job.status
        if status.succeeded:
            return
        if status.failed and status.failed > 0:
            raise RuntimeError(
                f"Replay job {replay_job_name(contract)} failed"
            )
        time.sleep(5)

    raise RuntimeError(
        f"Replay job {replay_job_name(contract)} did not complete within {job_timeout}s"
    )


def collect_replay_logs(contract: controller.RunContract) -> str:
    pods = core_api().list_namespaced_pod(
        NAMESPACE,
        label_selector=f"job-name={replay_job_name(contract)}",
    ).items
    if not pods:
        return ""
    pod = max(
        pods,
        key=lambda item: item.metadata.creation_timestamp or datetime.min.replace(tzinfo=timezone.utc),
    )
    return core_api().read_namespaced_pod_log(pod.metadata.name, NAMESPACE)


def collect_stream_logs(contract: controller.RunContract) -> str:
    pod = latest_stream_pod()
    return core_api().read_namespaced_pod_log(
        pod.metadata.name,
        NAMESPACE,
    )


def collect_stream_metrics_payload(contract: controller.RunContract) -> str:
    pod = latest_stream_pod()
    pod_ip = getattr(pod.status, "pod_ip", None)
    if not pod_ip:
        return ""

    request = urllib.request.Request(
        f"http://{pod_ip}:{STREAM_METRICS_PORT}/metrics",
        headers={"Accept": "text/plain"},
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=STREAM_METRICS_TIMEOUT_SECONDS,
        ) as response:
            return response.read().decode("utf-8")
    except Exception:
        return ""


def wait_for_stream_metrics_settlement(
    contract: controller.RunContract,
    replay_logs: str,
) -> str:
    deadline = time.time() + POST_REPLAY_SETTLE_TIMEOUT_SECONDS
    expected_input_events = controller.aws_baseline_metrics.extract_metrics(
        replay_logs,
        "",
    ).get("input_events")
    last_signature = None
    stable_polls = 0
    latest_payload = ""

    while time.time() < deadline:
        latest_payload = collect_stream_metrics_payload(contract)
        metrics = controller.aws_baseline_metrics.extract_metrics(
            replay_logs,
            "",
            stream_metrics_payload=latest_payload,
        )
        signature = (
            int(metrics.get("processed_events") or 0),
            int(metrics.get("invalid_events") or 0),
            int(metrics.get("deduplicated_events") or 0),
            int(metrics.get("breach_events") or 0),
        )
        if signature == last_signature and any(signature):
            stable_polls += 1
        else:
            stable_polls = 0
            last_signature = signature

        total_accounted_events = signature[0] + signature[1] + signature[2]
        if stable_polls >= POST_REPLAY_STABLE_POLLS:
            return latest_payload
        if (
            expected_input_events is not None
            and total_accounted_events >= int(expected_input_events)
            and stable_polls >= 1
        ):
            return latest_payload

        time.sleep(POST_REPLAY_SETTLE_POLL_SECONDS)

    return latest_payload


def summarize_s3_outputs(contract: controller.RunContract) -> dict[str, int]:
    import boto3

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    summary = {
        "processed_output_objects": 0,
        "invalid_output_objects": 0,
        "breach_window_output_objects": 0,
    }
    prefixes = {
        "processed_output_objects": f"{contract.s3_prefix}/processed/",
        "invalid_output_objects": f"{contract.s3_prefix}/invalid/",
        "breach_window_output_objects": f"{contract.s3_prefix}/breach_windows/",
    }

    for metric_key, prefix in prefixes.items():
        for page in paginator.paginate(Bucket=contract.bucket_name, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                basename = key.rsplit("/", 1)[-1]
                if not basename or basename.startswith("_") or basename.startswith("."):
                    continue
                summary[metric_key] += 1

    return summary


def run_orchestration(contract: controller.RunContract) -> dict[str, object]:
    reset_redis_state()
    patch_pipeline_config(contract)
    restart_stream_processor(contract)
    wait_for_stream_ready(contract)
    launch_replay_job(contract)
    wait_for_replay_completion(contract)
    replay_logs = collect_replay_logs(contract)
    stream_metrics_payload = wait_for_stream_metrics_settlement(contract, replay_logs)
    stream_logs = collect_stream_logs(contract)
    artifact_summary = summarize_s3_outputs(contract)
    return controller.extract_metrics_from_logs(
        replay_logs,
        stream_logs,
        stream_metrics_payload,
        artifact_summary,
        {
            "pipeline_target": contract.pipeline_target,
            "scenario": contract.scenario,
            "workload_family_version": contract.workload_family_version,
            "configured_events_per_second": load_workload_inputs(contract)[1][
                "target_eps"
            ],
            "workload_duration_minutes": load_workload_inputs(contract)[1][
                "duration_minutes"
            ],
            "pipeline_success": True,
            "controller_job_success": True,
            "replay_job_success": True,
        },
    )


def upload_reports(
    *, contract: controller.RunContract, report_payload: dict[str, object]
) -> None:
    import boto3

    client = boto3.client("s3")
    markdown_body = controller.render_markdown_report(report_payload)
    json_body = json.dumps(report_payload, indent=2, sort_keys=True)
    client.put_object(
        Bucket=contract.bucket_name,
        Key=f"{contract.s3_prefix}/report.md",
        Body=markdown_body.encode("utf-8"),
        ContentType="text/markdown",
    )
    client.put_object(
        Bucket=contract.bucket_name,
        Key=f"{contract.s3_prefix}/report.json",
        Body=json_body.encode("utf-8"),
        ContentType="application/json",
    )


def main() -> None:
    contract = load_runtime_inputs()
    if controller_mode() == "orchestrate":
        metrics = run_orchestration(contract)
    else:
        metrics = bootstrap_metrics()
    status = str(metrics.pop("_status", "succeeded"))
    failure_reason = metrics.pop("_failure_reason", None)
    if is_bootstrap_metrics(metrics):
        status = "bootstrap"
        metrics["controller_mode"] = "bootstrap"
    metrics.pop("pipeline_target", None)
    metrics.pop("scenario", None)
    metrics.pop("workload_family_version", None)
    report_payload = controller.build_report_payload(
        contract=contract,
        metrics=metrics,
        status=status,
        failure_reason=(
            None if failure_reason is None else str(failure_reason)
        ),
    )
    upload_reports(contract=contract, report_payload=report_payload)


if __name__ == "__main__":
    main()
