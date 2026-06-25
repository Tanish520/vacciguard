#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

AWS_REGION="${AWS_REGION:-ap-south-1}"
REPOSITORY_NAME="${REPOSITORY_NAME:-vacciguard-evaluation-controller}"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"
TAG="${1:-$(date -u +reporting-%Y%m%dt%H%M%Sz-amd64)}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd aws
require_cmd docker

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPOSITORY_NAME}:${TAG}"

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com" >/dev/null

docker buildx build \
  --platform "$DOCKER_PLATFORM" \
  -t "$IMAGE_URI" \
  -f services/evaluation-controller/Dockerfile \
  services/evaluation-controller \
  --push

printf '%s\n' "$IMAGE_URI"
