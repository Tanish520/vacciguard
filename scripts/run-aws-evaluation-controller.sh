#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PIPELINE_TARGET="${1:-baseline}"
SCENARIO="${2:-normal}"
RUN_ID="${3:-$(date -u +%Y%m%dT%H%M%SZ)}"
JOB_NAME="evaluation-controller-$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]')"
POD_RUNNING_TIMEOUT="${POD_RUNNING_TIMEOUT:-5m}"

eval "$(aws configure export-credentials --format env)"

TEMPLATE_JSON="$(kubectl create --dry-run=client -f infra/kubernetes/base/job-evaluation-controller.yaml -o json)"

python3 - "$TEMPLATE_JSON" "$JOB_NAME" "$PIPELINE_TARGET" "$SCENARIO" "$RUN_ID" <<'PY' | kubectl apply -f - >/dev/null
import json
import os
import sys

template = json.loads(sys.argv[1])
job_name = sys.argv[2]
pipeline_target = sys.argv[3]
scenario = sys.argv[4]
run_id = sys.argv[5]

template["metadata"]["name"] = job_name
template["metadata"]["namespace"] = "vacciguard"
template["spec"]["suspend"] = False

for item in template["spec"]["template"]["spec"]["containers"][0]["env"]:
    if item["name"] == "PIPELINE_TARGET":
        item["value"] = pipeline_target
    elif item["name"] == "SCENARIO":
        item["value"] = scenario
    elif item["name"] == "RUN_ID":
        item["value"] = run_id

env = template["spec"]["template"]["spec"]["containers"][0]["env"]
optional_env_names = (
    "S3_BUCKET_NAME",
    "WORKLOAD_FAMILY_VERSION",
    "WORKLOAD_DURATION_MINUTES",
)

for name in optional_env_names:
    value = os.environ.get(name)
    if not value:
        continue
    for item in env:
        if item["name"] == name:
            item["value"] = value
            break
    else:
        env.append({"name": name, "value": value})

for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
    value = os.environ.get(name)
    if value:
        env.append({"name": name, "value": value})

print(json.dumps(template))
PY

kubectl wait \
  --for=condition=Ready \
  "pod" \
  -l "job-name=${JOB_NAME}" \
  -n vacciguard \
  --timeout="$POD_RUNNING_TIMEOUT" >/dev/null || true

kubectl logs "job/${JOB_NAME}" -n vacciguard -f --pod-running-timeout="$POD_RUNNING_TIMEOUT"
