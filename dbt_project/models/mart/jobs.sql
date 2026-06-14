SELECT 
    job_id,
    title, 
    company,
    location,
    description,
    linkedin_url,
    source,
    collected_at
FROM {{ ref('stg_jobs') }}
