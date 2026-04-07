#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cleanup_output() {
  mkdir -p data/output/processed data/output/invalid data/output/checkpoints
  find data/output/processed -mindepth 1 ! -name '.gitkeep' -delete
  find data/output/invalid -mindepth 1 ! -name '.gitkeep' -delete
  find data/output/checkpoints -mindepth 1 ! -name '.gitkeep' -delete
}

shutdown() {
  echo "Shutting down services"
  docker compose down
}

trap shutdown EXIT

echo "[1/5] Generating workload"
python3 scripts/generate-dev-workload.py

echo "[2/5] Cleaning prior local output"
cleanup_output

echo "[3/5] Starting Kafka, Redis, and stream processor"
docker compose up -d --build kafka redis stream-processor

echo "[4/5] Replaying workload"
docker compose run --rm replay-producer

echo "[5/5] Running smoke verification"
python3 tests/smoke/verify_phase4.py

echo "Inspect Redis:"
docker compose exec -T redis redis-cli GET device:status:FR-0102

echo "Inspect processed output:"
find data/output/processed -maxdepth 1 -name '*.parquet' -type f | sort

echo "Inspect stream batch summaries:"
docker compose logs stream-processor | grep "Batch .* summary" || true
