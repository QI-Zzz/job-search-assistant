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

from job_agent import config
from job_agent.db import get_connection
from job_agent.llm import call_structured, format_posting, get_client, report, run_parallel

MODEL = "claude-haiku-5-5"
# bump when the prompt or the schema changes; jobs are re-extracted per version
PROMPT_VERSION = "v6"  # v2: adds role_family; v3: sharper family boundaries; v4: marketing, GTM, title fallback; v5: visa_sponsorship; v6: data_science split from data_ml_ai_engineering

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
- visa_sponsorship: what the posting says about sponsoring a work visa for someone from outside the EU. \
"offered" if it says it sponsors visas or work permits, supports the highly skilled migrant \
(kennismigrant) route, or is a recognised sponsor (erkend referent). "not_offered" if it says it does \
not sponsor, or that candidates must already have the right to work in the Netherlands or the EU \
(e.g. "no visa sponsorship", "EU work permit required", "must be eligible to work in the Netherlands \
without sponsorship", "valid work permit required"). "not_mentioned" otherwise; a relocation package \
alone, without visa support, is "not_mentioned".
- tech_stack: tools, languages and platforms named in the posting (e.g. SQL, Python, dbt, Snowflake, \
Power BI, AWS), short names, no duplicates. Empty list if none.
- role_family: the kind of work the role mainly does day to day, judged from the responsibilities, \
not the title alone. Pick one. Each family says what does NOT belong in it; when a role is \
borderline, pick the family of the work it spends most time on. If the posting does not describe \
the responsibilities, judge from the title.
  - data_analytics: the main work is analysing data and building reporting: SQL, Python or BI \
tools (Power BI, Looker, Tableau), dashboards, insights, experimentation, decision science. \
Analysts in pricing, marketing, product or operations count when most of their time is data analysis. \
NOT: merchandising, allocation, demand or supply planning (operations_supply_chain); financial \
planning, budgeting, FP&A (finance_controlling); market, consumer or competitor research \
(other); audit, risk or compliance reporting (finance_controlling); marketing campaign and paid \
media management, even when it includes performance reporting (other); administering a platform or \
system with some reporting on the side (operations_supply_chain).
  - data_ml_ai_engineering: building data pipelines, warehouses and data platforms, analytics \
engineering, ML and AI platforms, MLOps, and AI / LLM engineering where the core work is data \
or models in production. Also building data, CRM and automation systems for business teams (GTM engineering, \
revenue operations engineering). NOT: data science (data_science); DevOps, SRE or cloud infrastructure work (it_infrastructure_security); full-stack, backend or product software that calls an AI API \
(software_engineering); embedded or control software (software_engineering); security engineering \
(it_infrastructure_security).
  - data_science: the main work is data science: statistical modelling, predictive or \
forecasting models, machine learning model development and experimentation, causal inference, \
quantitative research. Titles like data scientist, applied scientist, ML scientist or research \
scientist. NOT: dashboards and reporting (data_analytics); pipelines, platforms or putting models \
into production (data_ml_ai_engineering).
  - software_engineering: building software products: backend, frontend, full stack, mobile, \
embedded.
  - business_analysis_consulting: business or systems analysis, requirements, process analysis and \
improvement, implementing business systems, technology, data or strategy consulting. NOT: \
operations roles such as client onboarding, warehouse or logistics engineering \
(operations_supply_chain); internal audit, IT audit, risk and compliance (finance_controlling); \
HR and recruiting (other).
  - presales_solutions: technical presales for SOFTWARE, DATA, CLOUD or AI products only \
(solutions engineer or architect, sales engineer, technical consultant supporting sales). The job \
is technical; selling is supported, not owned. NOT: application, field or service engineers for \
hardware, devices, sensors, robotics, clinical, lab, energy, industrial or food products (other).
  - sales_account_management: selling or managing accounts: account executive, business development, \
SDR, account manager, sales associate, broker, trader.
  - finance_controlling: accounting, controlling, FP&A, payroll, tax, internal and IT audit, risk and \
compliance, financial reporting, banking and investment roles.
  - operations_supply_chain: operations, logistics, procurement, demand and supply planning, \
merchandising and allocation, supply chain, customer onboarding, service and support, coordination.
  - it_infrastructure_security: IT support and help desk, networks, systems administration, \
DevOps, SRE, cloud infrastructure, cybersecurity, field service and hardware installation.
  - other: anything else (market and consumer research, marketing execution, design, HR and \
recruiting, legal, clinical, manufacturing, mechanical, electrical or hardware engineering, \
hardware or device application engineering, retail, AI-training gig work, ...).
- role_family_reason: one short sentence on why.
- evidence: short quotes from the posting supporting years_required_min, dutch_requirement, \
leads_team and visa_sponsorship. Empty string when there is nothing to quote."""

RoleFamily = Literal[
    "data_analytics",
    "data_ml_ai_engineering",
    "data_science",
    "software_engineering",
    "business_analysis_consulting",
    "presales_solutions",
    "sales_account_management",
    "finance_controlling",
    "operations_supply_chain",
    "it_infrastructure_security",
    "other",
]


class Evidence(BaseModel):
    years: str
    dutch: str
    leads_team: str
    visa: str


class JobFacts(BaseModel):
    posting_language: Literal["english", "dutch", "german", "french", "other"]
    years_required_min: Optional[int] = Field(description="null if no number of years is stated")
    dutch_requirement: Literal["required", "nice_to_have", "not_mentioned"]
    leads_team: bool
    visa_sponsorship: Literal["offered", "not_offered", "not_mentioned"]
    tech_stack: list[str]
    role_family: RoleFamily
    role_family_reason: str
    evidence: Evidence


def jobs_to_extract(conn, limit=None):
    """Recent jobs that passed the free rules and have no successful extraction for this prompt version."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT j.job_id, j.title, j.company_name, j.location, j.description
            FROM staging.jobs_prefiltered AS p
            JOIN staging.stg_jobs AS j USING (job_id)
            WHERE p.drop_reason IS NULL
              AND j.first_fetched_at >= NOW() - MAKE_INTERVAL(days => %s)
              AND NOT EXISTS (
                  SELECT 1 FROM raw.job_extractions AS e
                  WHERE e.job_id = j.job_id AND e.prompt_version = %s AND e.status = 'ok'
              )
            ORDER BY j.first_fetched_at DESC
            LIMIT %s
            """,
            (config.EXTRACT_RECENT_DAYS, PROMPT_VERSION, limit),
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
