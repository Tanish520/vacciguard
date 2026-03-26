#!/bin/bash
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-ap-south-1}"
CLUSTER_NAME="${VACCIGUARD_ECS_CLUSTER:-vacciguard-cluster}"
SIMULATOR_TASK_FAMILY="${VACCIGUARD_SIMULATOR_TASK_FAMILY:-vacciguard-simulator}"

DEFAULT_VPC_ID="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --region "${REGION}" --query 'Vpcs[0].VpcId' --output text)"
SUBNETS="$(aws ec2 describe-subnets --filters Name=vpc-id,Values="${DEFAULT_VPC_ID}" --region "${REGION}" --query 'Subnets[].SubnetId' --output text | tr '\t' ',')"
SECURITY_GROUP_ID="$(aws ec2 describe-security-groups --filters Name=vpc-id,Values="${DEFAULT_VPC_ID}" Name=group-name,Values=default --region "${REGION}" --query 'SecurityGroups[0].GroupId' --output text)"
TASK_DEF_ARN="$(aws ecs describe-task-definition --task-definition "${SIMULATOR_TASK_FAMILY}" --region "${REGION}" --query 'taskDefinition.taskDefinitionArn' --output text)"

echo "[run-simulator] Starting one ECS/Fargate simulator task on cluster ${CLUSTER_NAME}"
aws ecs run-task \
  --cluster "${CLUSTER_NAME}" \
  --task-definition "${TASK_DEF_ARN}" \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[${SUBNETS}],securityGroups=[${SECURITY_GROUP_ID}],assignPublicIp=ENABLED}" \
  --region "${REGION}"
