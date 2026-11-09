-- One row per scored eligible job first fetched in the last few days, in the order the email lists them. Every scored job is sent
-- (since 2026-10-31: few jobs reach pass 2, so there is no cut). Settings live in dbt_project.yml (vars.ranking).

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

insight AS (
    -- role insight (since 2026-11-05, replacing career value): a description of the job, no score
    SELECT
        job_id,
        result ->> 'company_summary' AS company_summary,
        result ->> 'main_mode' AS main_mode,
        result ->> 'day_to_day' AS day_to_day,
        result ->> 'what_it_builds' AS what_it_builds,
        ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(result -> 'watch_outs')) AS watch_outs,
        result AS insight_detail
    FROM latest_scores
    WHERE score_type = 'role_insight'
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
        e.visa_sponsorship,
        e.tech_stack,
        i.company_summary,
        i.main_mode,
        i.day_to_day,
        i.what_it_builds,
        i.watch_outs,
        f.fit_score,
        f.fit_reason,
        f.match_reasons,
        f.gaps,
        f.profile_version,
        i.insight_detail,
        f.fit_detail
    FROM {{ ref('jobs_eligibility') }} AS e
    JOIN first_fetched AS j USING (job_id)
    JOIN insight AS i USING (job_id)
    LEFT JOIN fit AS f USING (job_id)
    WHERE e.drop_reason IS NULL
      AND j.first_fetched_at >= NOW() - INTERVAL '{{ r.recent_days }} days'
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            -- by fit (can I get it?); newest job first on a tie. Career value was dropped on 2026-11-05
            ORDER BY fit_score DESC NULLS LAST, job_id DESC
        ) AS rank
    FROM scored
)

SELECT
    *,
    TRUE AS selected
FROM ranked
