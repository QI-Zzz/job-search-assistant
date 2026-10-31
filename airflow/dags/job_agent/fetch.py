"""Fetch newly posted LinkedIn jobs via Apify and land them unchanged in raw.apify_jobs.

Run by hand from airflow/dags:  python -m job_agent.fetch
"""
import os
from datetime import datetime, timedelta, timezone

from apify_client import ApifyClient
from psycopg2.extras import Json

from job_agent import config
from job_agent.db import get_connection


def run_search(client, keyword, skip_ids):
    run_input = {
        "keywords": keyword,
        "location": config.SEARCH_LOCATION,
        "datePosted": config.DATE_POSTED,
        "limit": config.LIMIT_PER_SEARCH,
        # Apify does not return (or charge for) these jobs
        "skipJobId": sorted(skip_ids),
    }
    run = client.actor(config.APIFY_ACTOR_ID).call(run_input=run_input, timeout_secs=config.RUN_TIMEOUT_SECS)
    if run is None or run.get("status") != "SUCCEEDED":
        status = run.get("status") if run else "no run returned"
        raise RuntimeError(f"Apify run for '{keyword}' did not succeed: {status}")

    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    return run["id"], items


def land_raw(conn, run_id, keyword, items, fetched_at):
    inserted = 0
    with conn.cursor() as cur:
        for item in items:
            cur.execute(
                """
                INSERT INTO raw.apify_jobs (run_id, search_keyword, job_id, payload, fetched_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (run_id, job_id) DO NOTHING
                """,
                (run_id, keyword, str(item["id"]), Json(item), fetched_at),
            )
            inserted += cur.rowcount
    # one commit per Apify run, so a failure never leaves half a run behind
    conn.commit()
    return inserted


def recently_seen(conn):
    """Job ids fetched in the last SKIP_SEEN_DAYS days."""
    since = datetime.now(timezone.utc) - timedelta(days=config.SKIP_SEEN_DAYS)
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT job_id FROM raw.apify_jobs WHERE fetched_at >= %s", (since,))
        return {row[0] for row in cur.fetchall()}


def main():
    client = ApifyClient(os.environ["APIFY_TOKEN"])
    conn = get_connection()
    total = 0
    failed = []
    try:
        seen = recently_seen(conn)
        print(f"skipping {len(seen)} jobs fetched in the last {config.SKIP_SEEN_DAYS} days")
        # one keyword at a time, so each search skips what the earlier ones found;
        # one failed keyword does not lose the others
        for keyword in config.SEARCH_KEYWORDS:
            fetched_at = datetime.now(timezone.utc)
            try:
                run_id, items = run_search(client, keyword, seen)
            except Exception as exc:
                failed.append(keyword)
                print(f"'{keyword}': FAILED: {exc}")
                continue
            inserted = land_raw(conn, run_id, keyword, items, fetched_at)
            seen.update(str(item["id"]) for item in items)
            print(f"'{keyword}': run {run_id}, {len(items)} fetched, {inserted} landed")
            total += inserted
    finally:
        conn.close()
    if failed:
        raise RuntimeError(f"{len(failed)} of {len(config.SEARCH_KEYWORDS)} searches failed: {failed}")
    return total


if __name__ == "__main__":
    main()
