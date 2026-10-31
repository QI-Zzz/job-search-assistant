-- "Can I apply?": pass 1 facts for jobs that passed the free rules.
-- One row per job; drop_reason is NULL for jobs that go on to scoring.
-- Thresholds live in dbt_project.yml (vars.eligibility).

{% set rules = var('eligibility') %}

WITH passed_free_rules AS (
    SELECT job_id
    FROM {{ ref('jobs_prefiltered') }}
    WHERE drop_reason IS NULL
),

latest_extraction AS (
    SELECT DISTINCT ON (job_id)
        job_id,
        prompt_version,
        extraction
    FROM {{ source('raw', 'job_extractions') }}
    WHERE status = 'ok'
    ORDER BY job_id, extracted_at DESC
),

facts AS (
    SELECT
        p.job_id,
        e.prompt_version,
        e.extraction ->> 'posting_language' AS posting_language,
        (e.extraction ->> 'years_required_min')::INT AS years_required_min,
        e.extraction ->> 'dutch_requirement' AS dutch_requirement,
        (e.extraction ->> 'leads_team')::BOOLEAN AS leads_team,
        ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(e.extraction -> 'tech_stack')) AS tech_stack,
        e.extraction ->> 'role_family' AS role_family,
        e.extraction ->> 'role_family_reason' AS role_family_reason,
        e.extraction -> 'evidence' AS evidence
    FROM passed_free_rules AS p
    LEFT JOIN latest_extraction AS e USING (job_id)
)

SELECT
    f.*,
    j.title,
    j.company_name,
    j.location,
    j.job_url,
    CASE
        WHEN f.prompt_version IS NULL THEN 'not extracted yet'
        WHEN f.posting_language <> 'english' THEN 'posting not in English'
        WHEN f.dutch_requirement = 'required' THEN 'Dutch required'
        WHEN f.years_required_min > {{ rules.max_years_required }} THEN 'more than {{ rules.max_years_required }} years required'
        WHEN f.leads_team THEN 'leads a team'
        -- extractions from before prompt v2 have no role family
        WHEN f.role_family IS NULL THEN 'no role family yet'
        WHEN f.role_family NOT IN ({{ "'" ~ rules.keep_role_families | join("', '") ~ "'" }}) THEN 'role family: ' || f.role_family
    END AS drop_reason
FROM facts AS f
JOIN {{ ref('stg_jobs') }} AS j USING (job_id)
