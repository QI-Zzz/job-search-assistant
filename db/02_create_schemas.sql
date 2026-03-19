-- switch to right database to make sure the schemas are created in the right place
\c jobs_db;

-- create the schema for the saved jobs
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;