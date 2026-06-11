-- create the database for the saved jobs
SELECT 'CREATE DATABASE jobs_db' 
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'jobs_db'
)\gexec




-- create the database for Airflow
SELECT 'CREATE DATABASE airflow_db' 
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow_db'
)\gexec