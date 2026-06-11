from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime

class Job(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="jobID")
    title: str 
    company: str 
    location: str | None = None
    description: str 
    linkedin_url: str 
    source: str 
    collected_at: datetime 
