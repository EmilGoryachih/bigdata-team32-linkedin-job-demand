CREATE DATABASE IF NOT EXISTS ${hiveconf:HIVE_DB}
LOCATION '${hiveconf:HDFS_BASE}/warehouse';

USE ${hiveconf:HIVE_DB};

SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

DROP TABLE IF EXISTS linkedin_jobs_enriched;

CREATE TABLE linkedin_jobs_enriched
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/enriched/linkedin_jobs_enriched'
AS
SELECT
    p.job_link,
    p.last_processed_time,
    p.got_summary,
    p.got_ner,
    p.is_being_worked,
    p.job_title,
    p.company,
    p.job_location,
    p.first_seen,
    p.search_city,
    p.search_country,
    p.search_position,
    p.job_level,
    p.job_type,
    s.job_skills,
    sm.job_summary
FROM linkedin_job_postings_parquet p
LEFT JOIN job_skills_parquet s
    ON p.job_link = s.job_link
LEFT JOIN job_summary_parquet sm
    ON p.job_link = sm.job_link;