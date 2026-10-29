-- Free, rule-based filters that run before any LLM call.
-- One row per job; drop_reason is NULL for jobs that go on to pass 1.
-- Rule patterns live in dbt_project.yml (vars.prefilter).

{% set rules = var('prefilter') %}

WITH jobs AS (
    SELECT
        *,
        LOWER(title) AS title_lower,
        -- count common Dutch vs English function words to detect the description's language
        (
            SELECT COUNT(*)
            FROM REGEXP_MATCHES(LOWER(description), '\m(de|het|een|en|van|je|wij|voor|met|zijn)\M', 'g')
        ) AS dutch_word_count,
        (
            SELECT COUNT(*)
            FROM REGEXP_MATCHES(LOWER(description), '\m(the|and|you|we|for|with|our|are|of|to)\M', 'g')
        ) AS english_word_count
    FROM {{ ref('stg_jobs') }}
)

SELECT
    job_id,
    title,
    company_name,
    location,
    experience_level,
    contract_type,
    dutch_word_count,
    english_word_count,
    CASE
        WHEN title_lower ~ '{{ rules.pool_title }}' THEN 'talent pool, not a vacancy'
        WHEN experience_level IN ({{ "'" ~ rules.excluded_experience_levels | join("', '") ~ "'" }}) THEN 'LinkedIn level: ' || experience_level
        WHEN contract_type IN ({{ "'" ~ rules.excluded_contract_types | join("', '") ~ "'" }}) THEN 'contract type: ' || contract_type
        WHEN title_lower ~ '{{ rules.lead_title }}' THEN 'lead or manager title'
        WHEN title_lower ~ '{{ rules.senior_title }}' THEN 'senior title'
        WHEN title_lower ~ '{{ rules.junior_track_title }}' THEN 'intern or trainee title'
        WHEN title_lower ~ '{{ rules.freelance_title }}' THEN 'freelance title'
        WHEN title_lower ~ '{{ rules.other_language_title }}' THEN 'title asks for another language'
        WHEN LOWER(location) !~ '{{ rules.allowed_location }}' THEN 'outside Amsterdam / Utrecht'
        WHEN dutch_word_count > english_word_count THEN 'description in Dutch'
    END AS drop_reason
FROM jobs
