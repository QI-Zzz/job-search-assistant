"""Fetch newly posted LinkedIn jobs via Apify and land them unchanged in raw.apify_jobs.

Run by hand from airflow/dags:  python -m job_agent.fetch
"""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from apify_client import ApifyClient
from psycopg2.extras import Json

from job_agent import config
from job_agent.db import get_connection


def run_search(client, keyword):
    run_input = {
        "keywords": keyword,
        "location": config.SEARCH_LOCATION,
        "datePosted": config.DATE_POSTED,
        "limit": config.LIMIT_PER_SEARCH,
    }
    run = client.actor(config.APIFY_ACTOR_ID).call(run_input=run_input)
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


def search_keyword(client, keyword):
    fetched_at = datetime.now(timezone.utc)
    run_id, items = run_search(client, keyword)
    return run_id, items, fetched_at


def main():
    client = ApifyClient(os.environ["APIFY_TOKEN"])
    conn = get_connection()
    total = 0
    failed = []
    try:
        with ThreadPoolExecutor(max_workers=config.MAX_PARALLEL_SEARCHES) as pool:
            futures = {pool.submit(search_keyword, client, kw): kw for kw in config.SEARCH_KEYWORDS}
            # land each run as it finishes; one failed keyword does not lose the others
            for future in as_completed(futures):
                keyword = futures[future]
                try:
                    run_id, items, fetched_at = future.result()
                except Exception as exc:
                    failed.append(keyword)
                    print(f"'{keyword}': FAILED: {exc}")
                    continue
                inserted = land_raw(conn, run_id, keyword, items, fetched_at)
                print(f"'{keyword}': run {run_id}, {len(items)} fetched, {inserted} landed")
                total += inserted
    finally:
        conn.close()
    if failed:
        raise RuntimeError(f"{len(failed)} of {len(config.SEARCH_KEYWORDS)} searches failed: {failed}")
    return total


if __name__ == "__main__":
    main()
