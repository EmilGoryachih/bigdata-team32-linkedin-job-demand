CREATE DATABASE IF NOT EXISTS ${hiveconf:HIVE_DB}
LOCATION '${hiveconf:HDFS_BASE}/warehouse';

USE ${hiveconf:HIVE_DB};

SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

DROP TABLE IF EXISTS linkedin_job_postings_parquet;
DROP TABLE IF EXISTS job_skills_parquet;
DROP TABLE IF EXISTS job_summary_parquet;

CREATE TABLE linkedin_job_postings_parquet
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/parquet/linkedin_job_postings_parquet'
AS
SELECT
    job_link,
    CAST(regexp_replace(last_processed_time, '\\+00$', '') AS TIMESTAMP) AS last_processed_time,

    CASE
        WHEN lower(got_summary) IN ('true', 't', '1') THEN 1
        ELSE 0
    END AS got_summary,

    CASE
        WHEN lower(got_ner) IN ('true', 't', '1') THEN 1
        ELSE 0
    END AS got_ner,

    CASE
        WHEN lower(is_being_worked) IN ('true', 't', '1') THEN 1
        ELSE 0
    END AS is_being_worked,

    job_title,
    company,
    job_location,
    CAST(first_seen AS DATE) AS first_seen,
    search_city,
    search_country,
    search_position,
    job_level,
    job_type
FROM linkedin_job_postings_raw;

CREATE TABLE job_skills_parquet
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/parquet/job_skills_parquet'
AS
SELECT
    job_link,
    job_skills
FROM job_skills_raw;

CREATE TABLE job_summary_parquet
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/parquet/job_summary_parquet'
AS
SELECT
    job_link,
    job_summary
FROM job_summary_raw;