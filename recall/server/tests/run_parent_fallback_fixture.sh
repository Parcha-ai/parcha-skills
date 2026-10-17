#!/usr/bin/env bash
# Local callers must use greppy-batch; GitHub runs this on its isolated runner.
set -euo pipefail
cd "$(dirname "$0")/../.."
fixture_python="${RECALL_TEST_PYTHON:-python}"
fixture_container="recall-parent-fallback-${GITHUB_RUN_ID:-$$}"
fixture_group=()
if [[ -d /sys/fs/cgroup/greppy.slice/greppy-batch.slice || -d /sys/fs/cgroup/greppy-batch.slice ]]; then
  fixture_group=(--cgroup-parent=greppy-batch.slice)
fi
trap 'docker rm -f "$fixture_container" >/dev/null 2>&1 || true' EXIT
docker run -d --name "$fixture_container" "${fixture_group[@]}" --memory=1g --memory-swap=1g --cpus=2 --pids-limit=256 \
  -e POSTGRES_PASSWORD=synthetic-ci-password \
  -p 127.0.0.1::5432 pgvector/pgvector:0.8.2-pg16 >/dev/null
for fixture_attempt in {1..30}; do
  if docker exec "$fixture_container" pg_isready -U postgres >/dev/null; then break; fi
  sleep 1
done
fixture_port=$(docker port "$fixture_container" 5432/tcp | cut -d: -f2)
export RECALL_DATABASE_URL="postgresql://postgres:synthetic-ci-password@127.0.0.1:$fixture_port/postgres"
export PYTHONPATH=.:server:server/tests
# Keep the verifier and its 20% threshold intact. Other E2E tenants must not
# contribute common join/scan work to this fixture's total-buffer comparison.
"$fixture_python" server/tests/e2e_logical_parent_fallback.py
