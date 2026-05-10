USE ${hiveconf:HIVE_DB};

SHOW TABLES;

SELECT 'linkedin_job_postings_parquet' AS table_name, COUNT(*) AS rows_count
FROM linkedin_job_postings_parquet;

SELECT 'job_skills_parquet' AS table_name, COUNT(*) AS rows_count
FROM job_skills_parquet;

SELECT 'job_summary_parquet' AS table_name, COUNT(*) AS rows_count
FROM job_summary_parquet;

SELECT 'linkedin_jobs_enriched' AS table_name, COUNT(*) AS rows_count
FROM linkedin_jobs_enriched;

SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN job_skills IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_skills,
    SUM(CASE WHEN job_summary IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_summary
FROM linkedin_jobs_enriched;

SELECT
    job_link,
    job_title,
    company,
    job_location,
    search_country,
    search_position,
    job_level,
    job_type,
    job_skills,
    job_summary
FROM linkedin_jobs_enriched
LIMIT 5;