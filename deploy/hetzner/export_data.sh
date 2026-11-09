#!/usr/bin/env bash
# Run on the Mac, from the repo root, after the last run there:
#     bash deploy/hetzner/export_data.sh
# Dumps the pipeline's raw schema (fetched jobs, pass 1 extractions, scores and the list of
# sent jobs) to jobs_raw.dump in the repo root. staging and mart are not dumped: dbt rebuilds
# them from raw. The dump is gitignored; it is copied to the VM with scp (README.md, step 5).
set -euo pipefail

OUT="jobs_raw.dump"

docker compose exec -T postgres sh -c \
    'pg_dump -U "$POSTGRES_USER" -d jobs_db --schema=raw --format=custom --no-owner' > "$OUT"

echo "Wrote $OUT ($(du -h "$OUT" | cut -f1))"
echo "Rows per table, to compare with the VM after the import:"
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d jobs_db -At -F " " -c "
    SELECT '"'"'apify_jobs'"'"', COUNT(*) FROM raw.apify_jobs
    UNION ALL SELECT '"'"'job_extractions'"'"', COUNT(*) FROM raw.job_extractions
    UNION ALL SELECT '"'"'job_scores'"'"', COUNT(*) FROM raw.job_scores
    UNION ALL SELECT '"'"'sent_jobs'"'"', COUNT(*) FROM raw.sent_jobs"'
