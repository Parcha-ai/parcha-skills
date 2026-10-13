#!/usr/bin/env bash
# Local callers must use greppy-batch; GitHub runs this on its isolated runner.
set -euo pipefail
cd "$(dirname "$0")/../.."
fixture_python="${RECALL_TEST_PYTHON:-python}"
fixture_container="recall-job-status-index-${GITHUB_RUN_ID:-$$}"
fixture_group=()
if [[ -d /sys/fs/cgroup/greppy-batch.slice ]]; then
  fixture_group=(--cgroup-parent=greppy-batch.slice)
fi
trap 'docker rm -f "$fixture_container" >/dev/null 2>&1 || true' EXIT
docker run -d --name "$fixture_container" "${fixture_group[@]}" --memory=1g --memory-swap=1g --cpus=2 --pids-limit=256 \
  -e POSTGRES_USER=fixture_bootstrap -e POSTGRES_DB=postgres -e POSTGRES_PASSWORD=synthetic-ci-password \
  -p 127.0.0.1::5432 pgvector/pgvector:0.8.2-pg17 >/dev/null
for fixture_attempt in {1..30}; do
  if docker exec "$fixture_container" pg_isready -U fixture_bootstrap >/dev/null; then break; fi
  sleep 1
done
fixture_port=$(docker port "$fixture_container" 5432/tcp | cut -d: -f2)
export RECALL_DATABASE_URL="postgresql://postgres:synthetic-ci-password@127.0.0.1:$fixture_port/postgres"
export PYTHONPATH=.:server:server/tests
"$fixture_python" - <<'PY'
import os,psycopg
with psycopg.connect(os.environ['RECALL_DATABASE_URL'].replace('://postgres:', '://fixture_bootstrap:'),autocommit=True) as connection:
    connection.execute("CREATE ROLE postgres LOGIN SUPERUSER PASSWORD 'synthetic-ci-password'")
PY
"$fixture_python" server/tests/e2e_job_status_index.py
"$fixture_python" - <<'PY'
import os,unittest,psycopg
from recall_server.db import BrainStore
with psycopg.connect(os.environ['RECALL_DATABASE_URL'],autocommit=True) as connection:
    connection.execute('DROP SCHEMA public CASCADE')
    connection.execute('CREATE SCHEMA public AUTHORIZATION postgres')
original = BrainStore.migrate
def migrate_without_status_index(self, **kwargs):
    result = original(self, **kwargs)
    with self.connect() as connection:
        self._migrate_concurrently(connection, 'DROP INDEX CONCURRENTLY IF EXISTS public.canonical_jobs_status_idx;')
    return result
BrainStore.migrate = migrate_without_status_index
import e2e_compact_ingest_metadata,e2e_slack_deletion_ordering,e2e_v2_lifecycle
suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromModule(module)
    for module in (e2e_compact_ingest_metadata,e2e_slack_deletion_ordering))
if not unittest.TextTestRunner().run(suite).wasSuccessful():
    raise SystemExit(1)
e2e_v2_lifecycle.main()
PY
