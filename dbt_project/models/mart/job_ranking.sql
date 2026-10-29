-- One row per eligible job with a career value score, ranked for the daily email.
-- Weights and selection rules live in dbt_project.yml (vars.ranking).

{% set r = var('ranking') %}

WITH latest_scores AS (
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
        ROUND((
            {{ r.fit_weights.skills_match }} * (result -> 'skills_match' ->> 'score')::NUMERIC
            + {{ r.fit_weights.experience_level }} * (result -> 'experience_level' ->> 'score')::NUMERIC
            + {{ r.fit_weights.role_domain_match }} * (result -> 'role_domain_match' ->> 'score')::NUMERIC
            + {{ r.fit_weights.other_must_haves }} * (result -> 'other_must_haves' ->> 'score')::NUMERIC
        ), 1) AS fit_score,
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
        f.match_reasons,
        f.gaps,
        f.profile_version,
        ROUND((c.career_value_score + f.fit_score) / 2, 1) AS overall_score,
        c.tier <= {{ r.max_tier }} AND f.fit_score >= {{ r.min_fit_score }} AS meets_bar,
        c.career_detail,
        f.fit_detail
    FROM {{ ref('jobs_eligibility') }} AS e
    JOIN career AS c USING (job_id)
    LEFT JOIN fit AS f USING (job_id)
    WHERE e.drop_reason IS NULL
),

ranked AS (
    SELECT
        *,
        -- technical advantage only breaks ties
        ROW_NUMBER() OVER (
            ORDER BY meets_bar DESC, overall_score DESC NULLS LAST, technical_advantage DESC
        ) AS rank
    FROM scored
)

SELECT
    *,
    meets_bar AND rank <= {{ r.max_jobs_per_email }} AS selected
FROM ranked
