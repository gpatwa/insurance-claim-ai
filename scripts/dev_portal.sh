#!/bin/bash
# Dev portal stack: Temporal worker + API with mock LLMs and seeded reference data.
# Prereq: `make up` (Temporal + Postgres + MinIO). Portal at http://localhost:8123/portal
set -e
cd "$(dirname "$0")/.."
export CLAIMPIPE_USE_MOCK_LLM=1
export CLAIMPIPE_REFDATA_FILE="$PWD/scripts/dev-refdata.json"
export CLAIMPIPE_API_PORT=8123
uv run python -m claimpipe.temporal.worker &
WORKER=$!
trap 'kill $WORKER 2>/dev/null' EXIT
uv run python -m claimpipe.api.main
