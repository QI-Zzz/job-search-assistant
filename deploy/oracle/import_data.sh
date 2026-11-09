#!/usr/bin/env bash
# Run on the VM, from the repo root, after copying jobs_raw.dump there:
#     bash deploy/oracle/import_data.sh
# Replaces the (empty) raw tables with the Mac's data, then rebuilds staging and mart with dbt.
# Run it before the first pipeline run on the VM.
set -euo pipefail

DUMP="jobs_raw.dump"
DBT="cd /opt/airflow/dbt_project && /home/airflow/dbt_venv/bin/dbt"

[ -f "$DUMP" ] || { echo "No $DUMP in $(pwd); copy it from the Mac first (README.md, step 5)"; exit 1; }

echo "== Restoring the raw schema"
# --clean --if-exists drops the empty tables created by db/03_create_raw_tables.sql first
docker compose exec -T postgres sh -c \
    'pg_restore -U "$POSTGRES_USER" -d jobs_db --clean --if-exists --no-owner' < "$DUMP"

echo "== Rows per table (should match the Mac's numbers)"
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d jobs_db -At -F " " -c "
    SELECT '"'"'apify_jobs'"'"', COUNT(*) FROM raw.apify_jobs
    UNION ALL SELECT '"'"'job_extractions'"'"', COUNT(*) FROM raw.job_extractions
    UNION ALL SELECT '"'"'job_scores'"'"', COUNT(*) FROM raw.job_scores
    UNION ALL SELECT '"'"'sent_jobs'"'"', COUNT(*) FROM raw.sent_jobs"'

echo "== Rebuilding staging and mart with dbt (a few minutes)"
docker compose exec -T airflow-scheduler bash -c "$DBT run"

echo "Done."
