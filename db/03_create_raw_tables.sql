-- init scripts run in separate psql sessions, so connect explicitly
\c jobs_db;

-- one row per job as returned by one Apify run; payload is stored unchanged
CREATE TABLE IF NOT EXISTS raw.apify_jobs (
    run_id TEXT NOT NULL,
    search_keyword TEXT NOT NULL,
    job_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (run_id, job_id)
);

-- pass 1: facts extracted from each job by a cheap LLM; one row per job per prompt version
CREATE TABLE IF NOT EXISTS raw.job_extractions (
    job_id TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    model TEXT NOT NULL,
    status TEXT NOT NULL,             -- 'ok' or 'failed'
    extraction JSONB,                 -- validated output; NULL when failed
    raw_response JSONB,               -- full API response as received
    error TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (job_id, prompt_version)
);

-- pass 2: LLM scores, one row per job per score type, prompt version and profile version
-- (profile_version is '-' for career value, which does not use the CV)
CREATE TABLE IF NOT EXISTS raw.job_scores (
    job_id TEXT NOT NULL,
    score_type TEXT NOT NULL,         -- 'career_value' or 'fit'
    prompt_version TEXT NOT NULL,
    profile_version TEXT NOT NULL,
    model TEXT NOT NULL,
    status TEXT NOT NULL,             -- 'ok' or 'failed'
    result JSONB,                     -- validated output; NULL when failed
    raw_response JSONB,
    error TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    scored_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (job_id, score_type, prompt_version, profile_version)
);
