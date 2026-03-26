#!/bin/bash
set -euo pipefail

# Full-cloud deployment for VacciGuard using ECR + ECS/Fargate.
# This script:
# 1. Builds and pushes a linux/amd64 image to ECR
# 2. Creates/updates the ECS cluster, IAM roles, and log groups
# 3. Registers task definitions for the pipeline service and simulator task
# 4. Creates or updates the long-running Flink pipeline service
#
# Run from the repo root:
#   bash infra/ecs/deploy.sh

REGION="${AWS_DEFAULT_REGION:-ap-south-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
REPO_NAME="${VACCIGUARD_ECR_REPO:-vacciguard}"
IMAGE_TAG="${VACCIGUARD_IMAGE_TAG:-$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d%H%M%S)}"
CLUSTER_NAME="${VACCIGUARD_ECS_CLUSTER:-vacciguard-cluster}"
PIPELINE_SERVICE_NAME="${VACCIGUARD_PIPELINE_SERVICE:-vacciguard-flink-pipeline}"
PIPELINE_TASK_FAMILY="${VACCIGUARD_PIPELINE_TASK_FAMILY:-vacciguard-flink-pipeline}"
SIMULATOR_TASK_FAMILY="${VACCIGUARD_SIMULATOR_TASK_FAMILY:-vacciguard-simulator}"
LOG_GROUP_PIPELINE="${VACCIGUARD_PIPELINE_LOG_GROUP:-/ecs/vacciguard/flink-pipeline}"
LOG_GROUP_SIMULATOR="${VACCIGUARD_SIMULATOR_LOG_GROUP:-/ecs/vacciguard/simulator}"
TASK_CPU="${VACCIGUARD_TASK_CPU:-1024}"
TASK_MEMORY="${VACCIGUARD_TASK_MEMORY:-2048}"
SIMULATOR_CPU="${VACCIGUARD_SIMULATOR_CPU:-1024}"
SIMULATOR_MEMORY="${VACCIGUARD_SIMULATOR_MEMORY:-2048}"
PIPELINE_DESIRED_COUNT="${VACCIGUARD_PIPELINE_DESIRED_COUNT:-1}"
ENABLE_ALERTS="${VACCIGUARD_ENABLE_ALERTS:-false}"
SNS_ALERT_TOPIC_ARN="${VACCIGUARD_SNS_ALERT_TOPIC_ARN:-}"
KINESIS_STREAM_NAME="${VACCIGUARD_KINESIS_STREAM_NAME:-vacciguard-stream}"
DYNAMO_TABLE_NAME="${VACCIGUARD_DYNAMO_TABLE_NAME:-VacciguardFridgeState}"
METRICS_NAMESPACE="${VACCIGUARD_METRICS_NAMESPACE:-VacciGuard/BaselinePipeline}"
SIMULATOR_ACTIVE_FRIDGES="${VACCIGUARD_SIMULATOR_ACTIVE_FRIDGES:-500}"
SIMULATOR_RECORDS_PER_SECOND="${VACCIGUARD_SIMULATOR_RECORDS_PER_SECOND:-500}"
SIMULATOR_DURATION_SECONDS="${VACCIGUARD_SIMULATOR_DURATION_SECONDS:-300}"
SIMULATOR_RUN_LABEL="${VACCIGUARD_SIMULATOR_RUN_LABEL:-baseline}"
PIPELINE_COMMAND='["python","flink/pipeline.py"]'
SIMULATOR_COMMAND='["python","simulator/simulator.py"]'
ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}"
IMAGE_URI="${ECR_URI}:${IMAGE_TAG}"
EXECUTION_ROLE_NAME="${VACCIGUARD_EXECUTION_ROLE_NAME:-vacciguard-ecs-execution-role}"
TASK_ROLE_NAME="${VACCIGUARD_TASK_ROLE_NAME:-vacciguard-ecs-task-role}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[deploy] ERROR: required command not found: $1"
    exit 1
  }
}

require_cmd aws
require_cmd docker

echo "[deploy] Deploying VacciGuard to ECS/Fargate in ${REGION} (account ${ACCOUNT_ID})"

ensure_ecr_repo() {
  if ! aws ecr describe-repositories --repository-names "${REPO_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    echo "[deploy] Creating ECR repository ${REPO_NAME}"
    aws ecr create-repository --repository-name "${REPO_NAME}" --region "${REGION}" >/dev/null
  fi
}

ensure_execution_role() {
  if aws iam get-role --role-name "${EXECUTION_ROLE_NAME}" >/dev/null 2>&1; then
    return
  fi

  echo "[deploy] Creating ECS execution role ${EXECUTION_ROLE_NAME}"
  local assume_policy
  assume_policy="$(mktemp)"
  cat > "${assume_policy}" <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
  aws iam create-role \
    --role-name "${EXECUTION_ROLE_NAME}" \
    --assume-role-policy-document "file://${assume_policy}" >/dev/null
  rm -f "${assume_policy}"
  aws iam attach-role-policy \
    --role-name "${EXECUTION_ROLE_NAME}" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy" >/dev/null
}

ensure_task_role() {
  if ! aws iam get-role --role-name "${TASK_ROLE_NAME}" >/dev/null 2>&1; then
    echo "[deploy] Creating ECS task role ${TASK_ROLE_NAME}"
    local assume_policy
    assume_policy="$(mktemp)"
    cat > "${assume_policy}" <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
    aws iam create-role \
      --role-name "${TASK_ROLE_NAME}" \
      --assume-role-policy-document "file://${assume_policy}" >/dev/null
    rm -f "${assume_policy}"
  fi

  local policy_file
  policy_file="$(mktemp)"
  cat > "${policy_file}" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:DescribeStreamSummary",
        "kinesis:GetRecords",
        "kinesis:GetShardIterator",
        "kinesis:ListShards",
        "kinesis:PutRecord",
        "kinesis:PutRecords"
      ],
      "Resource": "arn:aws:kinesis:${REGION}:${ACCOUNT_ID}:stream/${KINESIS_STREAM_NAME}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem"
      ],
      "Resource": "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${DYNAMO_TABLE_NAME}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sns:Publish"
      ],
      "Resource": "${SNS_ALERT_TOPIC_ARN:-*}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "cloudwatch:namespace": "${METRICS_NAMESPACE}"
        }
      }
    }
  ]
}
EOF
  aws iam put-role-policy \
    --role-name "${TASK_ROLE_NAME}" \
    --policy-name "vacciguard-ecs-task-policy" \
    --policy-document "file://${policy_file}" >/dev/null
  rm -f "${policy_file}"
}

create_log_group() {
  local name="$1"
  if ! aws logs describe-log-groups --log-group-name-prefix "${name}" --region "${REGION}" --query 'logGroups[?logGroupName==`'"${name}"'`].logGroupName' --output text | grep -q "${name}"; then
    echo "[deploy] Creating CloudWatch log group ${name}"
    aws logs create-log-group --log-group-name "${name}" --region "${REGION}" >/dev/null
  fi
}

push_image() {
  echo "[deploy] Logging into ECR ${ECR_URI}"
  aws ecr get-login-password --region "${REGION}" \
    | docker login --username AWS --password-stdin "${ECR_URI}"

  echo "[deploy] Building and pushing linux/amd64 image ${IMAGE_URI}"
  docker buildx build \
    --platform linux/amd64 \
    --tag "${IMAGE_URI}" \
    --push \
    .
}

register_task_definition() {
  local family="$1"
  local log_group="$2"
  local cpu="$3"
  local memory="$4"
  local command_json="$5"
  local output_file="$6"

  local execution_role_arn task_role_arn
  execution_role_arn="$(aws iam get-role --role-name "${EXECUTION_ROLE_NAME}" --query 'Role.Arn' --output text)"
  task_role_arn="$(aws iam get-role --role-name "${TASK_ROLE_NAME}" --query 'Role.Arn' --output text)"

  cat > "${output_file}" <<EOF
{
  "family": "${family}",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "${cpu}",
  "memory": "${memory}",
  "executionRoleArn": "${execution_role_arn}",
  "taskRoleArn": "${task_role_arn}",
  "containerDefinitions": [
    {
      "name": "${family}",
      "image": "${IMAGE_URI}",
      "essential": true,
      "command": ${command_json},
      "environment": [
        {"name": "AWS_DEFAULT_REGION", "value": "${REGION}"},
        {"name": "VACCIGUARD_REGION", "value": "${REGION}"},
        {"name": "VACCIGUARD_KINESIS_STREAM_NAME", "value": "${KINESIS_STREAM_NAME}"},
        {"name": "VACCIGUARD_DYNAMO_TABLE_NAME", "value": "${DYNAMO_TABLE_NAME}"},
        {"name": "VACCIGUARD_ENABLE_ALERTS", "value": "${ENABLE_ALERTS}"},
        {"name": "VACCIGUARD_SNS_ALERT_TOPIC_ARN", "value": "${SNS_ALERT_TOPIC_ARN}"},
        {"name": "VACCIGUARD_METRICS_NAMESPACE", "value": "${METRICS_NAMESPACE}"},
        {"name": "VACCIGUARD_SIMULATOR_ACTIVE_FRIDGES", "value": "${SIMULATOR_ACTIVE_FRIDGES}"},
        {"name": "VACCIGUARD_SIMULATOR_RECORDS_PER_SECOND", "value": "${SIMULATOR_RECORDS_PER_SECOND}"},
        {"name": "VACCIGUARD_SIMULATOR_DURATION_SECONDS", "value": "${SIMULATOR_DURATION_SECONDS}"},
        {"name": "VACCIGUARD_SIMULATOR_RUN_LABEL", "value": "${SIMULATOR_RUN_LABEL}"}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "${log_group}",
          "awslogs-region": "${REGION}",
          "awslogs-stream-prefix": "${family}"
        }
      }
    }
  ]
}
EOF

  aws ecs register-task-definition --cli-input-json "file://${output_file}" --region "${REGION}" >/dev/null
}

discover_network() {
  DEFAULT_VPC_ID="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --region "${REGION}" --query 'Vpcs[0].VpcId' --output text)"
  if [[ -z "${DEFAULT_VPC_ID}" || "${DEFAULT_VPC_ID}" == "None" ]]; then
    echo "[deploy] ERROR: no default VPC found in ${REGION}. Set up networking first."
    exit 1
  fi

  SUBNETS="$(aws ec2 describe-subnets --filters Name=vpc-id,Values="${DEFAULT_VPC_ID}" --region "${REGION}" --query 'Subnets[].SubnetId' --output text | tr '\t' ',')"
  SECURITY_GROUP_ID="$(aws ec2 describe-security-groups --filters Name=vpc-id,Values="${DEFAULT_VPC_ID}" Name=group-name,Values=default --region "${REGION}" --query 'SecurityGroups[0].GroupId' --output text)"
}

ensure_cluster() {
  if ! aws ecs describe-clusters --clusters "${CLUSTER_NAME}" --region "${REGION}" --query 'clusters[0].clusterName' --output text 2>/dev/null | grep -q "${CLUSTER_NAME}"; then
    echo "[deploy] Creating ECS cluster ${CLUSTER_NAME}"
    aws ecs create-cluster --cluster-name "${CLUSTER_NAME}" --region "${REGION}" >/dev/null
  fi
}

ensure_service() {
  local current_status
  current_status="$(aws ecs describe-services --cluster "${CLUSTER_NAME}" --services "${PIPELINE_SERVICE_NAME}" --region "${REGION}" --query 'services[0].status' --output text 2>/dev/null || true)"
  local task_def_arn
  task_def_arn="$(aws ecs describe-task-definition --task-definition "${PIPELINE_TASK_FAMILY}" --region "${REGION}" --query 'taskDefinition.taskDefinitionArn' --output text)"

  if [[ "${current_status}" == "ACTIVE" ]]; then
    echo "[deploy] Updating ECS service ${PIPELINE_SERVICE_NAME}"
    aws ecs update-service \
      --cluster "${CLUSTER_NAME}" \
      --service "${PIPELINE_SERVICE_NAME}" \
      --task-definition "${task_def_arn}" \
      --desired-count "${PIPELINE_DESIRED_COUNT}" \
      --force-new-deployment \
      --region "${REGION}" >/dev/null
  else
    echo "[deploy] Creating ECS service ${PIPELINE_SERVICE_NAME}"
    aws ecs create-service \
      --cluster "${CLUSTER_NAME}" \
      --service-name "${PIPELINE_SERVICE_NAME}" \
      --task-definition "${task_def_arn}" \
      --desired-count "${PIPELINE_DESIRED_COUNT}" \
      --launch-type FARGATE \
      --network-configuration "awsvpcConfiguration={subnets=[${SUBNETS}],securityGroups=[${SECURITY_GROUP_ID}],assignPublicIp=ENABLED}" \
      --region "${REGION}" >/dev/null
  fi
}

main() {
  ensure_ecr_repo
  ensure_execution_role
  ensure_task_role
  create_log_group "${LOG_GROUP_PIPELINE}"
  create_log_group "${LOG_GROUP_SIMULATOR}"
  push_image
  ensure_cluster
  discover_network

  local pipeline_td simulator_td
  pipeline_td="$(mktemp)"
  simulator_td="$(mktemp)"
  register_task_definition "${PIPELINE_TASK_FAMILY}" "${LOG_GROUP_PIPELINE}" "${TASK_CPU}" "${TASK_MEMORY}" "${PIPELINE_COMMAND}" "${pipeline_td}"
  register_task_definition "${SIMULATOR_TASK_FAMILY}" "${LOG_GROUP_SIMULATOR}" "${SIMULATOR_CPU}" "${SIMULATOR_MEMORY}" "${SIMULATOR_COMMAND}" "${simulator_td}"
  ensure_service
  rm -f "${pipeline_td}" "${simulator_td}"

  echo "[deploy] Full-cloud pipeline deployed."
  echo "[deploy] Pipeline service: ${PIPELINE_SERVICE_NAME}"
  echo "[deploy] Cluster: ${CLUSTER_NAME}"
  echo "[deploy] Image: ${IMAGE_URI}"
  echo "[deploy] To run the simulator in ECS next, use:"
  echo "  bash infra/ecs/run_simulator.sh"
}

main "$@"
