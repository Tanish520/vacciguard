from __future__ import annotations

import json
import os
from pathlib import Path
import sys


CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

import controller


def load_runtime_inputs() -> controller.RunContract:
    return controller.resolve_run_contract(
        pipeline_target=os.environ["PIPELINE_TARGET"],
        scenario=os.environ["SCENARIO"],
        run_id=os.environ["RUN_ID"],
        workload_family_version=os.environ["WORKLOAD_FAMILY_VERSION"],
        bucket_name=os.environ["S3_BUCKET_NAME"],
    )


def run_orchestration(contract: controller.RunContract) -> dict[str, object]:
    return {
        "_status": "bootstrap",
        "controller_mode": "bootstrap",
        "processed_events": 0,
    }


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
    metrics = run_orchestration(contract)
    status = str(metrics.pop("_status", "succeeded"))
    failure_reason = metrics.pop("_failure_reason", None)
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
