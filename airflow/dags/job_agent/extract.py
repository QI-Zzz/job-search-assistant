"""Pass 1: a cheap LLM extracts facts from each job that passed the free rules.

Only extracts; the eligibility decision is made in dbt (jobs_eligibility).
Runs in the LLM virtualenv, from airflow/dags:
    /home/airflow/llm_venv/bin/python -m job_agent.extract [--limit N]
"""
import argparse
from datetime import datetime, timezone
from typing import Literal, Optional

from psycopg2.extras import Json
from pydantic import BaseModel, Field

from job_agent.db import get_connection
from job_agent.llm import call_structured, format_posting, get_client, report, run_parallel

MODEL = "claude-haiku-5-5"
# bump when the prompt or the schema changes; jobs are re-extracted per version
PROMPT_VERSION = "v1"

SYSTEM_PROMPT = """You extract facts from a job posting. Only report what the posting states; do not guess.

- posting_language: the language the posting is mainly written in.
- years_required_min: the minimum years of work experience the posting requires. For a range \
("3-5 years") use the lower bound; for "5+ years" use 5. If several are listed, use the one for the \
overall role. null if no number of years is stated.
- dutch_requirement: "required" if the candidate must speak or write Dutch (e.g. "fluent Dutch", \
"Dutch is a must", "native Dutch speaker"); "nice_to_have" if it is only a plus or preferred; \
"not_mentioned" otherwise.
- leads_team: true only if the role manages people or leads a team (direct reports, team lead, \
people management). Mentoring juniors or leading a project does not count.
- tech_stack: tools, languages and platforms named in the posting (e.g. SQL, Python, dbt, Snowflake, \
Power BI, AWS), short names, no duplicates. Empty list if none.
- evidence: short quotes from the posting supporting years_required_min, dutch_requirement and \
leads_team. Empty string when there is nothing to quote."""


class Evidence(BaseModel):
    years: str
    dutch: str
    leads_team: str


class JobFacts(BaseModel):
    posting_language: Literal["english", "dutch", "german", "french", "other"]
    years_required_min: Optional[int] = Field(description="null if no number of years is stated")
    dutch_requirement: Literal["required", "nice_to_have", "not_mentioned"]
    leads_team: bool
    tech_stack: list[str]
    evidence: Evidence


def jobs_to_extract(conn, limit=None):
    """Jobs that passed the free rules and have no successful extraction for this prompt version."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT j.job_id, j.title, j.company_name, j.location, j.description
            FROM staging.jobs_prefiltered AS p
            JOIN staging.stg_jobs AS j USING (job_id)
            WHERE p.drop_reason IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM raw.job_extractions AS e
                  WHERE e.job_id = j.job_id AND e.prompt_version = %s AND e.status = 'ok'
              )
            ORDER BY j.first_fetched_at DESC
            LIMIT %s
            """,
            (PROMPT_VERSION, limit),
        )
        return cur.fetchall()


def extract_one(client, job):
    job_id, title, company, location, description = job
    row = call_structured(
        client,
        JobFacts,
        model=MODEL,
        max_tokens=2000,
        thinking={"type": "disabled"},
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": format_posting(title, company, location, description)}],
    )
    return {**row, "job_id": job_id}


def save(conn, rows):
    extracted_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for r in rows:
            # a retry replaces an earlier failed row for the same version
            cur.execute(
                """
                INSERT INTO raw.job_extractions
                    (job_id, prompt_version, model, status, extraction, raw_response,
                     error, input_tokens, output_tokens, extracted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id, prompt_version) DO UPDATE SET
                    model = EXCLUDED.model, status = EXCLUDED.status,
                    extraction = EXCLUDED.extraction, raw_response = EXCLUDED.raw_response,
                    error = EXCLUDED.error, input_tokens = EXCLUDED.input_tokens,
                    output_tokens = EXCLUDED.output_tokens, extracted_at = EXCLUDED.extracted_at
                """,
                (r["job_id"], PROMPT_VERSION, MODEL, r["status"],
                 Json(r["result"]) if r["result"] else None,
                 Json(r["raw_response"]) if r["raw_response"] else None,
                 r["error"], r["input_tokens"], r["output_tokens"], extracted_at),
            )
    conn.commit()


def main(limit=None):
    client = get_client()
    conn = get_connection()
    try:
        rows = run_parallel(lambda job: extract_one(client, job), jobs_to_extract(conn, limit))
        save(conn, rows)
    finally:
        conn.close()
    report("pass 1", rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="extract at most N jobs (for testing)")
    main(parser.parse_args().limit)
