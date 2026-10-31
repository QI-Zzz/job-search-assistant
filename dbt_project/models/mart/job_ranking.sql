-- One row per scored eligible job first fetched in the last few days, in the order the email lists them. Every scored job is sent
-- (since 2026-10-31: few jobs reach pass 2, so there is no cut). Weights live in dbt_project.yml (vars.ranking).

{% set r = var('ranking') %}

WITH first_fetched AS (
    -- straight from the raw table: joining the stg_jobs view here made the model take minutes
    SELECT job_id, MIN(fetched_at) AS first_fetched_at
    FROM {{ source('raw', 'apify_jobs') }}
    GROUP BY job_id
),

latest_scores AS (
    SELECT DISTINCT ON (job_id, score_type)
        job_id,
        score_type,
        profile_version,
        result
    FROM {{ source('raw', 'job_scores') }}
    WHERE status = 'ok'
    ORDER BY job_id, score_type, scored_at DESC
),

career AS (
    SELECT
        job_id,
        (result ->> 'tier')::INT AS tier,
        ROUND((
            {{ r.career_weights.salary_ceiling }} * (result -> 'salary_ceiling' ->> 'score')::NUMERIC
            + {{ r.career_weights.geographic_mobility }} * (result -> 'geographic_mobility' ->> 'score')::NUMERIC
            + {{ r.career_weights.skill_transferability }} * (result -> 'skill_transferability' ->> 'score')::NUMERIC
            + {{ r.career_weights.low_lock_in_risk }} * (result -> 'low_lock_in_risk' ->> 'score')::NUMERIC
        ), 1) AS career_value_score,
        (result -> 'technical_advantage' ->> 'score')::INT AS technical_advantage,
        result ->> 'summary' AS career_summary,
        result AS career_detail
    FROM latest_scores
    WHERE score_type = 'career_value'
),

fit AS (
    SELECT
        job_id,
        profile_version,
        -- fit v2 is one overall score; v1 had four weighted dimensions
        (result ->> 'score')::NUMERIC AS fit_score,
        result ->> 'reason' AS fit_reason,
        ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(result -> 'match_reasons')) AS match_reasons,
        ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(result -> 'gaps')) AS gaps,
        result AS fit_detail
    FROM latest_scores
    WHERE score_type = 'fit'
),

scored AS (
    SELECT
        e.job_id,
        e.title,
        e.company_name,
        e.location,
        e.job_url,
        e.years_required_min,
        e.dutch_requirement,
        e.tech_stack,
        c.tier,
        c.career_value_score,
        c.technical_advantage,
        c.career_summary,
        f.fit_score,
        f.fit_reason,
        f.match_reasons,
        f.gaps,
        f.profile_version,
        ROUND((c.career_value_score + f.fit_score) / 2, 1) AS overall_score,
        c.career_detail,
        f.fit_detail
    FROM {{ ref('jobs_eligibility') }} AS e
    JOIN first_fetched AS j USING (job_id)
    JOIN career AS c USING (job_id)
    LEFT JOIN fit AS f USING (job_id)
    WHERE e.drop_reason IS NULL
      AND j.first_fetched_at >= NOW() - INTERVAL '{{ r.recent_days }} days'
),

ranked AS (
    SELECT
        *,
        -- technical advantage only breaks ties
        ROW_NUMBER() OVER (
            ORDER BY overall_score DESC NULLS LAST, technical_advantage DESC
        ) AS rank
    FROM scored
)

SELECT
    *,
    TRUE AS selected
FROM ranked
