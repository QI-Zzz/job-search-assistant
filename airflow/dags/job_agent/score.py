"""Pass 2: score eligible jobs for career value and fit.

Career value runs first and never sees the CV. Fit compares the job with the candidate
profile and is skipped for Tier 4 jobs. Weighted scores and selection are done in dbt.
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

from job_agent.db import get_connection
from job_agent.llm import call_structured, format_posting, get_client, report, run_parallel

MODEL = "claude-sonnet-5-5"
EFFORT = "low"
# bump when a prompt or schema changes; jobs are rescored per version
CAREER_PROMPT_VERSION = "v1"
FIT_PROMPT_VERSION = "v1"
PROFILE_PATH = os.getenv("PROFILE_PATH", "/opt/airflow/profile/candidate_profile.md")

CAREER_PROMPT = """You judge how good a job is for the long-term career of an early-career professional \
(second year of her career, based in Amsterdam). Judge the job itself, not whether this person would get it.

Base every judgement on the actual responsibilities and likely career outcomes, never on the title alone. \
Score each dimension 1-10 and give a one-sentence reason before the score.

1. salary_ceiling (30%): long-term earning potential, salary progression, access to high-paying \
employers and markets, paths into leadership or other lucrative tracks.
   2 = low-paid track with a low ceiling; 5 = average market pay and progression; \
9 = leads into top-paying roles, employers or markets.
2. geographic_mobility (25%): international demand, cross-border skill recognition, remote work, \
feasibility of working in the Netherlands, UAE, Singapore or the US.
   2 = only usable in one country; 5 = transferable to some markets with effort; \
9 = in demand in all four markets.
3. skill_transferability (25%): whether the skills and experience built in this role transfer \
across industries, companies and functions, including pivots into other careers.
   2 = narrow skills useful only in this niche; 5 = partly transferable; \
9 = broadly valued skills that open many doors.
4. low_lock_in_risk (20%): dependence on local regulations, niche industry knowledge, proprietary \
systems, country-specific qualifications or a narrow track. Higher score = LOWER risk.
   2 = heavily tied to a proprietary system, local rules or a niche; 5 = some lock-in; \
9 = no meaningful lock-in.

technical_advantage (bonus, not part of the weighted score): how much technical depth, AI exposure, \
coding or data skills in this role create additional career options. A role is not better just \
because it involves more coding, and a technical role with poor salary, mobility or lock-in is not good.
   2 = no technical growth; 5 = some; 9 = strong technical growth that widens options.

Non-technical roles (strategy, commercial, operations, product, consulting, management) can score highly.

tier:
   1 = Excellent: strong earning potential, international mobility, transferable skills, low lock-in.
   2 = Good: attractive overall, with one meaningful compromise.
   3 = Conditional: limited upside in one or more important dimensions; only worth it if the trade-offs are justified.
   4 = Poor: several serious constraints on earning potential, mobility or career freedom.

summary: one sentence on the main reason for the tier."""

FIT_PROMPT = """You judge how well a job matches a candidate's background: could she realistically \
get this job? Do not judge whether the job is good for her career; that is scored separately.

Score each dimension 1-10 and give a one-sentence reason before the score.

1. skills_match (35%): do her tools and skills cover what the job requires?
   2 = core stack missing (e.g. Java, Spark); 5 = about half covered, the rest learnable; \
9 = most of it covered (e.g. SQL, Python, dbt, Airflow, BigQuery).
2. experience_level (25%): does about 2 years of experience fit what the job asks?
   2 = asks 5+ years or a senior scope; 5 = asks 3-4 years, she could stretch; \
9 = junior to medior, 1-3 years.
3. role_domain_match (25%): is the work similar to what she has done?
   2 = unrelated (e.g. sales quotas, clinical, legal); 5 = adjacent (e.g. finance controlling, \
BI in a new industry); 9 = analytics or data pipelines, forecasting, retail or operations.
4. other_must_haves (15%): anything else that blocks her?
   2 = a required degree, certification or language she lacks (e.g. German, CISSP); \
5 = one soft gap; 9 = nothing blocking.

match_reasons: 2-4 short, specific reasons this job matches her background, written to her \
("Your dbt migration work at Coolblue..."). Only real matches; fewer reasons if the match is weak.
gaps: the main things the job asks for that her background does not show. Empty list if none.

The candidate's profile:

{profile}"""


class Dimension(BaseModel):
    reason: str
    score: int = Field(ge=1, le=10)


class CareerValue(BaseModel):
    salary_ceiling: Dimension
    geographic_mobility: Dimension
    skill_transferability: Dimension
    low_lock_in_risk: Dimension
    technical_advantage: Dimension
    tier: Literal[1, 2, 3, 4]
    summary: str


class Fit(BaseModel):
    skills_match: Dimension
    experience_level: Dimension
    role_domain_match: Dimension
    other_must_haves: Dimension
    match_reasons: list[str]
    gaps: list[str]


def load_profile():
    with open(PROFILE_PATH, encoding="utf-8") as f:
        text = f.read()
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def jobs_to_score(conn, score_type, prompt_version, profile_version, limit=None):
    """Eligible jobs without a successful score of this type and version.

    Fit only considers jobs whose current career value score is not Tier 4.
    """
    tier_filter = """
        AND EXISTS (
            SELECT 1 FROM raw.job_scores AS c
            WHERE c.job_id = e.job_id AND c.score_type = 'career_value'
              AND c.prompt_version = %(career_version)s AND c.status = 'ok'
              AND (c.result ->> 'tier')::INT < 4
        )""" if score_type == "fit" else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT e.job_id, j.title, j.company_name, j.location, j.description
            FROM staging.jobs_eligibility AS e
            JOIN staging.stg_jobs AS j USING (job_id)
            WHERE e.drop_reason IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM raw.job_scores AS s
                  WHERE s.job_id = e.job_id AND s.score_type = %(score_type)s
                    AND s.prompt_version = %(prompt_version)s
                    AND s.profile_version = %(profile_version)s AND s.status = 'ok'
              ){tier_filter}
            ORDER BY j.first_fetched_at DESC
            LIMIT %(limit)s
            """,
            {"score_type": score_type, "prompt_version": prompt_version,
             "profile_version": profile_version, "career_version": CAREER_PROMPT_VERSION,
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
    profile, profile_version = load_profile()
    conn = get_connection()
    try:
        jobs = jobs_to_score(conn, "career_value", CAREER_PROMPT_VERSION, "-", limit)
        rows = run_parallel(lambda job: score_one(client, CAREER_PROMPT, CareerValue, job), jobs)
        save(conn, "career_value", CAREER_PROMPT_VERSION, "-", rows)
        report("career value", rows)

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
