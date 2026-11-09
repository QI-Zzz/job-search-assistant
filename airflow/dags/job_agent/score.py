"""Pass 2: role insight and fit for each eligible job.

Role insight (no score) describes what the job would really be like, written with Amy's job matching
framework (profile/job_matching_framework.md) in mind; it does not use the CV. Fit compares the job
with the candidate profile and gives a 1-10 score. Ordering is done in dbt.
Runs in the LLM virtualenv, from airflow/dags:
    /home/airflow/llm_venv/bin/python -m job_agent.score [--limit N]
"""
import argparse
import hashlib
import os
from datetime import datetime, timezone
from typing import Literal

from psycopg2.extras import Json
from pydantic import BaseModel, Field

from job_agent import config
from job_agent.db import get_connection
from job_agent.llm import call_structured, format_posting, get_client, report, run_parallel

MODEL = "claude-sonnet-5-5"
EFFORT = "low"
# bump when a prompt or schema changes; jobs are rescored per version
# Role insight replaced career value on 2026-11-05: no scores, a fuller picture of the job written
# with Amy's job matching framework (profile/job_matching_framework.md) in mind.
# Career value (prompts v1-v3) rows stay in raw.job_scores under score_type 'career_value'.
INSIGHT_PROMPT_VERSION = "v1"
FIT_PROMPT_VERSION = "v3"  # v2: one overall score instead of four weighted dimensions; v3: written to her as "you"
PROFILE_PATH = os.getenv("PROFILE_PATH", "/opt/airflow/profile/candidate_profile.md")
# Amy's job matching framework, used by role insight; editing it redoes the insight for recent jobs
FRAMEWORK_PATH = os.getenv("FRAMEWORK_PATH", "/opt/airflow/profile/job_matching_framework.md")

INSIGHT_PROMPT = """You help the person described in the job matching framework below understand what \
a job would really be like, beyond the wording of the posting. Do not score the job, and do not judge \
whether she would get it; that is done separately.

She reads your answer in her daily job email, so write to her as "you" and "your", never "she", "her" \
or "the user". Be concrete and candid; do not repeat the posting's selling points or requirements. \
Separate what the posting says from what you infer ("probably", "likely"), and say when the posting is \
too vague to tell. Use the framework's sections 1, 3 and 9 for what matters to her; ignore its scoring, \
tiers and report format.

- company_summary: one or two sentences on what the company does (product or service, customers, size \
or stage if stated). If the posting does not say, write "The posting does not describe the company."
- main_mode: what the job is mostly: building, advising, coordinating, operating, maintaining or selling.
- day_to_day: three to five sentences on what you would actually spend your week on, what you would own \
(decisions, projects, systems, outcomes) versus support, who you would work with, and how the time \
splits between building or improving things and routine operations, reporting and maintenance.
- what_it_builds: two or three sentences on the skills and experience the job would give you, how \
portable they are across employers and countries, and the realistic next roles after three to five years. \
Do not overstate what it can lead to.
- watch_outs: up to three short, specific red flags for you, judged against the framework's dislikes and \
constraints (for example mostly routine reporting or maintenance, vague ownership, a narrow or \
proprietary system, hidden sales targets, a local language in practice, a much more senior or junior \
level than the title suggests). Empty list if there are none worth mentioning.

The job matching framework:

{framework}"""

FIT_PROMPT = """You judge how well a job matches a candidate's background: could she realistically \
get this job? Do not judge whether the job is good for her career; that is scored separately.

She reads your answer in her daily email, so write the reason, match_reasons and gaps to her \
directly, as "you" and "your", never "she", "her" or "the candidate".

Give one overall fit score from 1 to 10, as a recruiter reading her CV against this posting would. \
Weigh together whatever matters most for this job: her skills and tools, her level of experience \
(about 2 years), how close the work is to what she has done, and anything that would block her \
(a required language, degree or certification). Write the reason first, then the score.
   2 = she would not get an interview: core skills or a hard requirement missing, or a much more \
senior role; 5 = a stretch: about half matches and the rest is learnable, worth applying if she \
wants it; 9 = a strong match: she does most of this work already at the right level.

match_reasons: 2-4 short, specific reasons this job matches her background \
("Your dbt migration work at Coolblue..."). Only real matches; fewer reasons if the match is weak.
gaps: the main things the job asks for that her background does not show. Empty list if none.

The candidate's profile:

{profile}"""


class RoleInsight(BaseModel):
    company_summary: str
    main_mode: Literal["building", "advising", "coordinating", "operating", "maintaining", "selling"]
    day_to_day: str
    what_it_builds: str
    watch_outs: list[str] = Field(max_length=3)


class Fit(BaseModel):
    reason: str
    score: int = Field(ge=1, le=10)
    match_reasons: list[str]
    gaps: list[str]


def load_text(path):
    """File text plus a short hash, stored as profile_version so an edit rescores recent jobs."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def jobs_to_score(conn, score_type, prompt_version, profile_version, limit=None):
    """Recent eligible jobs without a successful score of this type and version."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT e.job_id, j.title, j.company_name, j.location, j.description
            FROM staging.jobs_eligibility AS e
            JOIN staging.stg_jobs AS j USING (job_id)
            WHERE e.drop_reason IS NULL
              AND j.first_fetched_at >= NOW() - MAKE_INTERVAL(days => %(recent_days)s)
              AND NOT EXISTS (
                  SELECT 1 FROM raw.job_scores AS s
                  WHERE s.job_id = e.job_id AND s.score_type = %(score_type)s
                    AND s.prompt_version = %(prompt_version)s
                    AND s.profile_version = %(profile_version)s AND s.status = 'ok'
              )
            ORDER BY j.first_fetched_at DESC
            LIMIT %(limit)s
            """,
            {"score_type": score_type, "prompt_version": prompt_version,
             "profile_version": profile_version, "recent_days": config.SCORE_RECENT_DAYS,
             "limit": limit},
        )
        return cur.fetchall()


def score_one(client, system_prompt, output_format, job):
    job_id, title, company, location, description = job
    row = call_structured(
        client,
        output_format,
        model=MODEL,
        max_tokens=8000,
        output_config={"effort": EFFORT},
        # the rubric (and profile) is identical for every job, so cache it
        system=[{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": format_posting(title, company, location, description)}],
    )
    return {**row, "job_id": job_id}


def save(conn, score_type, prompt_version, profile_version, rows):
    scored_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for r in rows:
            # a retry replaces an earlier failed row for the same versions
            cur.execute(
                """
                INSERT INTO raw.job_scores
                    (job_id, score_type, prompt_version, profile_version, model, status,
                     result, raw_response, error, input_tokens, output_tokens, scored_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id, score_type, prompt_version, profile_version) DO UPDATE SET
                    model = EXCLUDED.model, status = EXCLUDED.status, result = EXCLUDED.result,
                    raw_response = EXCLUDED.raw_response, error = EXCLUDED.error,
                    input_tokens = EXCLUDED.input_tokens, output_tokens = EXCLUDED.output_tokens,
                    scored_at = EXCLUDED.scored_at
                """,
                (r["job_id"], score_type, prompt_version, profile_version, MODEL, r["status"],
                 Json(r["result"]) if r["result"] else None,
                 Json(r["raw_response"]) if r["raw_response"] else None,
                 r["error"], r["input_tokens"], r["output_tokens"], scored_at),
            )
    conn.commit()


def main(limit=None):
    client = get_client()
    profile, profile_version = load_text(PROFILE_PATH)
    framework, framework_version = load_text(FRAMEWORK_PATH)
    conn = get_connection()
    try:
        insight_prompt = INSIGHT_PROMPT.format(framework=framework)
        jobs = jobs_to_score(conn, "role_insight", INSIGHT_PROMPT_VERSION, framework_version, limit)
        rows = run_parallel(lambda job: score_one(client, insight_prompt, RoleInsight, job), jobs)
        save(conn, "role_insight", INSIGHT_PROMPT_VERSION, framework_version, rows)
        report(f"role insight (framework {framework_version})", rows)

        fit_prompt = FIT_PROMPT.format(profile=profile)
        jobs = jobs_to_score(conn, "fit", FIT_PROMPT_VERSION, profile_version, limit)
        rows = run_parallel(lambda job: score_one(client, fit_prompt, Fit, job), jobs)
        save(conn, "fit", FIT_PROMPT_VERSION, profile_version, rows)
        report(f"fit (profile {profile_version})", rows)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="score at most N jobs per step (for testing)")
    main(parser.parse_args().limit)
