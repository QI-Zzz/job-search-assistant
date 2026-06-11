from fastapi import APIRouter, Depends
from app.database import get_db 
from app.models import Job

router = APIRouter()

@router.post("/jobs/save")
def save_job(job: Job, con=Depends(get_db)):
    cursor = con.cursor()

    # 1. check if job_id already exists
    cursor.execute(
        "SELECT job_id FROM raw.jobs WHERE job_id = %s",
        (job.job_id,)
    )
    existing = cursor.fetchone()

    if existing:
        return {"status": "already_exists", "job_id": job.job_id}

    # 2. insert the new job
    cursor.execute("""
        INSERT INTO raw.jobs (
            job_id, title, company, location,
            description, linkedin_url, source, collected_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        job.job_id, job.title, job.company, job.location,
        job.description, job.linkedin_url, job.source, job.collected_at
    ))
    
    con.commit()

    return {"status": "saved", "job_id": job.job_id}
