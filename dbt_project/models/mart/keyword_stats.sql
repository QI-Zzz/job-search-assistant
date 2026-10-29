-- What each search keyword adds, per fetch day: one row per day x keyword.
-- "only this keyword" = found by no other keyword that day; low values mark keywords to merge or drop.

WITH found AS (
    SELECT DISTINCT
        (fetched_at AT TIME ZONE 'Europe/Amsterdam')::DATE AS fetch_date,
        search_keyword AS keyword,
        job_id
    FROM {{ source('raw', 'apify_jobs') }}
),

keywords_per_job AS (
    SELECT fetch_date, job_id, COUNT(*) AS keyword_count
    FROM found
    GROUP BY fetch_date, job_id
),

outcomes AS (
    SELECT
        f.fetch_date,
        f.keyword,
        f.job_id,
        k.keyword_count = 1 AS only_this_keyword,
        p.drop_reason IS NULL AS passed_free_rules,
        e.job_id IS NOT NULL AND e.drop_reason IS NULL AS eligible,
        COALESCE(r.selected, FALSE) AS selected
    FROM found AS f
    JOIN keywords_per_job AS k USING (fetch_date, job_id)
    LEFT JOIN {{ ref('jobs_prefiltered') }} AS p USING (job_id)
    LEFT JOIN {{ ref('jobs_eligibility') }} AS e USING (job_id)
    LEFT JOIN {{ ref('job_ranking') }} AS r USING (job_id)
)

SELECT
    fetch_date,
    keyword,
    COUNT(*) AS jobs_found,
    COUNT(*) FILTER (WHERE only_this_keyword) AS only_this_keyword,
    COUNT(*) FILTER (WHERE passed_free_rules) AS passed_free_rules,
    COUNT(*) FILTER (WHERE passed_free_rules AND only_this_keyword) AS passed_free_rules_only_this_keyword,
    COUNT(*) FILTER (WHERE eligible) AS eligible,
    COUNT(*) FILTER (WHERE selected) AS selected,
    COUNT(*) FILTER (WHERE selected AND only_this_keyword) AS selected_only_this_keyword
FROM outcomes
GROUP BY fetch_date, keyword
