CREATE TABLE IF NOT EXISTS raw.jobs (
    job_id TEXT PRIMARY KEY,
    title TEXT,
    company TEXT,
    location TEXT,
    description TEXT,
    linkedin_url TEXT,
    source TEXT,
    collected_at TIMESTAMP WITH TIME ZONE,
    saved_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);