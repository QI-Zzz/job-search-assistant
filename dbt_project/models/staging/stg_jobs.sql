-- One row per unique LinkedIn job: fields from the most recent fetch,
-- plus when the job was first and last seen.

WITH fetches AS (
    SELECT
        job_id,
        search_keyword,
        fetched_at,
        payload
    FROM {{ source('raw', 'apify_jobs') }}
),

seen AS (
    SELECT
        job_id,
        MIN(fetched_at) AS first_fetched_at,
        MAX(fetched_at) AS last_fetched_at,
        ARRAY_AGG(DISTINCT search_keyword) AS search_keywords
    FROM fetches
    GROUP BY job_id
),

latest AS (
    SELECT DISTINCT ON (job_id)
        job_id,
        payload
    FROM fetches
    ORDER BY job_id, fetched_at DESC
)

SELECT
    latest.job_id,
    latest.payload ->> 'title' AS title,
    latest.payload ->> 'companyName' AS company_name,
    latest.payload ->> 'location' AS location,
    latest.payload ->> 'url' AS job_url,
    latest.payload ->> 'companyUrl' AS company_url,
    latest.payload ->> 'experienceLevel' AS experience_level,
    latest.payload ->> 'contractType' AS contract_type,
    latest.payload ->> 'workType' AS work_type,
    latest.payload ->> 'sector' AS sector,
    latest.payload ->> 'salary' AS salary,
    latest.payload ->> 'applyType' AS apply_type,
    NULLIF(latest.payload ->> 'postedDate', '')::DATE AS posted_date,
    latest.payload ->> 'applicationsCount' AS applications_count,
    latest.payload ->> 'description' AS description,
    seen.search_keywords,
    seen.first_fetched_at,
    seen.last_fetched_at
FROM latest
JOIN seen USING (job_id)
