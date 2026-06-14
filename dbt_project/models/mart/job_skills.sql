SELECT 
    job_id,
    skill
FROM {{ ref('stg_job_skills') }} 