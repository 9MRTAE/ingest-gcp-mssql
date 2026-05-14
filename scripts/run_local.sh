#!/usr/bin/env bash
# scripts/run_local.sh — wrapper to run a flow locally
#
# Usage:
#   bash scripts/run_local.sh ecom
#   bash scripts/run_local.sh ecom --table tmAttribute
#   bash scripts/run_local.sh ecom --backdate -3
#   bash scripts/run_local.sh ecom --startdate 2024-01-01 --enddate 2024-01-31

set -euo pipefail

FLOW="${1:?Usage: $0 <flow_name> [options]}"
shift

export CI_COMMIT_BRANCH="${CI_COMMIT_BRANCH:-develop}"

echo "▶ run_local | flow=${FLOW} env=${CI_COMMIT_BRANCH}"
PYTHONPATH="${PYTHONPATH:-$(pwd)}" python run_local.py --flow "$FLOW" "$@"
