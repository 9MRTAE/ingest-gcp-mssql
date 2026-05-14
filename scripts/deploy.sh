#!/usr/bin/env bash
# scripts/deploy.sh — Build Docker image and register all Prefect v3 deployments
# Usage:
#   CI_COMMIT_BRANCH=main \
#   IMAGE_TAG=asia-southeast1-docker.pkg.dev/infra-thelivingos/thelivingos/data/prefect_v3/ingest/ingest_gcp_mssql_popcorn:main-<sha> \
#   bash scripts/deploy.sh

set -euo pipefail

: "${CI_COMMIT_BRANCH:?CI_COMMIT_BRANCH is required}"
: "${IMAGE_TAG:?IMAGE_TAG is required}"
: "${PREFECT_WORK_POOL:=kubernetes-pool}"
: "${PREFECT_WORK_QUEUE:=default}"

echo "▶ deploy | branch=${CI_COMMIT_BRANCH} image=${IMAGE_TAG}"

echo "── Building Docker image ──"
docker build -t "${IMAGE_TAG}" .
docker push "${IMAGE_TAG}"

echo "── Registering Prefect deployments ──"
PREFECT_DEPLOY_MODE=1 \
CI_COMMIT_BRANCH="${CI_COMMIT_BRANCH}" \
IMAGE_TAG="${IMAGE_TAG}" \
PREFECT_WORK_POOL="${PREFECT_WORK_POOL}" \
PREFECT_WORK_QUEUE="${PREFECT_WORK_QUEUE}" \
  prefect deploy --all --prefect-file prefect.yaml

echo "✓ All deployments registered"
