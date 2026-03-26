#!/bin/bash
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-ap-south-1}"
CLUSTER_NAME="${VACCIGUARD_ECS_CLUSTER:-vacciguard-cluster}"
SIMULATOR_TASK_FAMILY="${VACCIGUARD_SIMULATOR_TASK_FAMILY:-vacciguard-simulator}"
SIMULATOR_ACTIVE_FRIDGES="${VACCIGUARD_SIMULATOR_ACTIVE_FRIDGES:-500}"
SIMULATOR_RECORDS_PER_SECOND="${VACCIGUARD_SIMULATOR_RECORDS_PER_SECOND:-500}"
SIMULATOR_DURATION_SECONDS="${VACCIGUARD_SIMULATOR_DURATION_SECONDS:-300}"
SIMULATOR_RUN_LABEL="${VACCIGUARD_SIMULATOR_RUN_LABEL:-baseline}"

DEFAULT_VPC_ID="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --region "${REGION}" --query 'Vpcs[0].VpcId' --output text)"
SUBNETS="$(aws ec2 describe-subnets --filters Name=vpc-id,Values="${DEFAULT_VPC_ID}" --region "${REGION}" --query 'Subnets[].SubnetId' --output text | tr '\t' ',')"
SECURITY_GROUP_ID="$(aws ec2 describe-security-groups --filters Name=vpc-id,Values="${DEFAULT_VPC_ID}" Name=group-name,Values=default --region "${REGION}" --query 'SecurityGroups[0].GroupId' --output text)"
TASK_DEF_ARN="$(aws ecs describe-task-definition --task-definition "${SIMULATOR_TASK_FAMILY}" --region "${REGION}" --query 'taskDefinition.taskDefinitionArn' --output text)"
OVERRIDES_FILE="$(mktemp)"

cat > "${OVERRIDES_FILE}" <<EOF
{
  "containerOverrides": [
    {
      "name": "vacciguard-simulator",
      "environment": [
        {"name": "VACCIGUARD_SIMULATOR_ACTIVE_FRIDGES", "value": "${SIMULATOR_ACTIVE_FRIDGES}"},
        {"name": "VACCIGUARD_SIMULATOR_RECORDS_PER_SECOND", "value": "${SIMULATOR_RECORDS_PER_SECOND}"},
        {"name": "VACCIGUARD_SIMULATOR_DURATION_SECONDS", "value": "${SIMULATOR_DURATION_SECONDS}"},
        {"name": "VACCIGUARD_SIMULATOR_RUN_LABEL", "value": "${SIMULATOR_RUN_LABEL}"}
      ]
    }
  ]
}
EOF

echo "[run-simulator] Starting one ECS/Fargate simulator task on cluster ${CLUSTER_NAME}"
echo "[run-simulator] Workload: ${SIMULATOR_ACTIVE_FRIDGES} fridges, ${SIMULATOR_RECORDS_PER_SECOND} records/sec, ${SIMULATOR_DURATION_SECONDS}s, label=${SIMULATOR_RUN_LABEL}"
aws ecs run-task \
  --cluster "${CLUSTER_NAME}" \
  --task-definition "${TASK_DEF_ARN}" \
  --launch-type FARGATE \
  --overrides "file://${OVERRIDES_FILE}" \
  --network-configuration "awsvpcConfiguration={subnets=[${SUBNETS}],securityGroups=[${SECURITY_GROUP_ID}],assignPublicIp=ENABLED}" \
  --region "${REGION}"

rm -f "${OVERRIDES_FILE}"
