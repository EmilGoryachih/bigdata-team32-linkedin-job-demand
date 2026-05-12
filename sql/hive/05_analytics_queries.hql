CREATE DATABASE IF NOT EXISTS ${hiveconf:HIVE_DB}
LOCATION '${hiveconf:HDFS_BASE}/warehouse';

USE ${hiveconf:HIVE_DB};

SET hive.execution.engine=tez;
SET hive.exec.compress.output=true;
SET hive.exec.parallel=false;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.stats.autogather=false;
SET tez.task.generate.counters=false;
SET parquet.compression=SNAPPY;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;

DROP TABLE IF EXISTS analytics_data_characteristics;
DROP TABLE IF EXISTS analytics_null_coverage;
DROP TABLE IF EXISTS analytics_clean_jobs;
DROP TABLE IF EXISTS analytics_job_skills_exploded;
DROP TABLE IF EXISTS analytics_top_positions_global;
DROP TABLE IF EXISTS analytics_top_positions_by_country;
DROP TABLE IF EXISTS analytics_top_skills_global;
DROP TABLE IF EXISTS analytics_top_skills_by_country;
DROP TABLE IF EXISTS analytics_skills_by_job_level;
DROP TABLE IF EXISTS analytics_job_type_distribution;
DROP TABLE IF EXISTS analytics_job_level_distribution;
DROP TABLE IF EXISTS analytics_top_companies;
DROP TABLE IF EXISTS analytics_demand_by_date;
DROP TABLE IF EXISTS analytics_country_position_matrix;

CREATE TABLE analytics_clean_jobs
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_clean_jobs'
AS
SELECT
    job_link,
    last_processed_time,
    got_summary,
    got_ner,
    is_being_worked,
    CASE
        WHEN job_title IS NULL OR trim(job_title) = '' THEN 'unknown'
        ELSE lower(trim(regexp_replace(job_title, '\\s+', ' ')))
    END AS job_title_clean,
    CASE
        WHEN company IS NULL OR trim(company) = '' THEN 'unknown'
        ELSE lower(trim(regexp_replace(company, '\\s+', ' ')))
    END AS company_clean,
    CASE
        WHEN job_location IS NULL OR trim(job_location) = '' THEN 'unknown'
        ELSE lower(trim(regexp_replace(job_location, '\\s+', ' ')))
    END AS job_location_clean,
    first_seen,
    CASE
        WHEN search_city IS NULL OR trim(search_city) = '' THEN 'unknown'
        ELSE lower(trim(regexp_replace(search_city, '\\s+', ' ')))
    END AS search_city_clean,
    CASE
        WHEN search_country IS NULL OR trim(search_country) = '' THEN 'unknown'
        ELSE lower(trim(regexp_replace(search_country, '\\s+', ' ')))
    END AS search_country_clean,
    CASE
        WHEN search_position IS NULL OR trim(search_position) = '' THEN 'unknown'
        ELSE lower(trim(regexp_replace(search_position, '\\s+', ' ')))
    END AS search_position_clean,
    CASE
        WHEN job_level IS NULL OR trim(job_level) = '' THEN 'unknown'
        ELSE lower(trim(regexp_replace(job_level, '\\s+', ' ')))
    END AS job_level_clean,
    CASE
        WHEN job_type IS NULL OR trim(job_type) = '' THEN 'unknown'
        ELSE lower(trim(regexp_replace(job_type, '\\s+', ' ')))
    END AS job_type_clean,
    CASE
        WHEN job_skills IS NULL OR trim(job_skills) = '' THEN NULL
        ELSE lower(trim(regexp_replace(job_skills, '\\s+', ' ')))
    END AS job_skills_clean,
    CASE
        WHEN job_summary IS NULL OR trim(job_summary) = '' THEN NULL
        ELSE trim(regexp_replace(job_summary, '\\s+', ' '))
    END AS job_summary_clean
FROM linkedin_jobs_enriched;

CREATE TABLE analytics_job_skills_exploded
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_job_skills_exploded'
AS
SELECT
    job_link,
    search_country_clean,
    search_position_clean,
    job_level_clean,
    job_type_clean,
    company_clean,
    first_seen,
    trim(raw_skill) AS skill
FROM analytics_clean_jobs
LATERAL VIEW explode(split(coalesce(job_skills_clean, ''), ',')) skill_table AS raw_skill
WHERE trim(raw_skill) <> '';

CREATE TABLE analytics_data_characteristics
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_data_characteristics'
AS
SELECT 'total_rows' AS metric_name, CAST(COUNT(*) AS STRING) AS metric_value
FROM analytics_clean_jobs
UNION ALL
SELECT 'feature_count', '16'
UNION ALL
SELECT 'feature_names',
       'job_link,last_processed_time,got_summary,got_ner,is_being_worked,job_title,company,job_location,first_seen,search_city,search_country,search_position,job_level,job_type,job_skills,job_summary'
UNION ALL
SELECT 'distinct_countries', CAST(COUNT(DISTINCT search_country_clean) AS STRING)
FROM analytics_clean_jobs
WHERE search_country_clean <> 'unknown'
UNION ALL
SELECT 'distinct_positions', CAST(COUNT(DISTINCT search_position_clean) AS STRING)
FROM analytics_clean_jobs
WHERE search_position_clean <> 'unknown'
UNION ALL
SELECT 'distinct_companies', CAST(COUNT(DISTINCT company_clean) AS STRING)
FROM analytics_clean_jobs
WHERE company_clean <> 'unknown'
UNION ALL
SELECT 'rows_with_skills', CAST(COUNT(*) AS STRING)
FROM analytics_clean_jobs
WHERE job_skills_clean IS NOT NULL
UNION ALL
SELECT 'rows_with_summary', CAST(COUNT(*) AS STRING)
FROM analytics_clean_jobs
WHERE job_summary_clean IS NOT NULL
UNION ALL
SELECT 'min_first_seen', CAST(MIN(first_seen) AS STRING)
FROM analytics_clean_jobs
UNION ALL
SELECT 'max_first_seen', CAST(MAX(first_seen) AS STRING)
FROM analytics_clean_jobs
;

CREATE TABLE analytics_null_coverage
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_null_coverage'
AS
SELECT 'job_title' AS column_name, COUNT(*) AS total_rows,
       SUM(CASE WHEN job_title_clean = 'unknown' THEN 1 ELSE 0 END) AS missing_rows,
       ROUND(100.0 * SUM(CASE WHEN job_title_clean = 'unknown' THEN 1 ELSE 0 END) / COUNT(*), 2) AS missing_pct
FROM analytics_clean_jobs
UNION ALL
SELECT 'company', COUNT(*), SUM(CASE WHEN company_clean = 'unknown' THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN company_clean = 'unknown' THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM analytics_clean_jobs
UNION ALL
SELECT 'search_country', COUNT(*), SUM(CASE WHEN search_country_clean = 'unknown' THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN search_country_clean = 'unknown' THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM analytics_clean_jobs
UNION ALL
SELECT 'search_position', COUNT(*), SUM(CASE WHEN search_position_clean = 'unknown' THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN search_position_clean = 'unknown' THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM analytics_clean_jobs
UNION ALL
SELECT 'job_level', COUNT(*), SUM(CASE WHEN job_level_clean = 'unknown' THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN job_level_clean = 'unknown' THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM analytics_clean_jobs
UNION ALL
SELECT 'job_type', COUNT(*), SUM(CASE WHEN job_type_clean = 'unknown' THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN job_type_clean = 'unknown' THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM analytics_clean_jobs
UNION ALL
SELECT 'job_skills', COUNT(*), SUM(CASE WHEN job_skills_clean IS NULL THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN job_skills_clean IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM analytics_clean_jobs
UNION ALL
SELECT 'job_summary', COUNT(*), SUM(CASE WHEN job_summary_clean IS NULL THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN job_summary_clean IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM analytics_clean_jobs
;

CREATE TABLE analytics_top_positions_global
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_top_positions_global'
AS
SELECT
    search_position_clean AS search_position,
    COUNT(*) AS posting_count,
    COUNT(DISTINCT company_clean) AS company_count,
    COUNT(DISTINCT search_country_clean) AS country_count
FROM analytics_clean_jobs
WHERE search_position_clean <> 'unknown'
GROUP BY search_position_clean;

CREATE TABLE analytics_top_positions_by_country
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_top_positions_by_country'
AS
SELECT
    search_country_clean AS search_country,
    search_position_clean AS search_position,
    COUNT(*) AS posting_count,
    COUNT(DISTINCT company_clean) AS company_count
FROM analytics_clean_jobs
WHERE search_country_clean <> 'unknown'
  AND search_position_clean <> 'unknown'
GROUP BY search_country_clean, search_position_clean;

CREATE TABLE analytics_top_skills_global
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_top_skills_global'
AS
SELECT
    skill,
    COUNT(*) AS skill_mentions,
    COUNT(DISTINCT job_link) AS posting_count,
    COUNT(DISTINCT search_country_clean) AS country_count
FROM analytics_job_skills_exploded
GROUP BY skill;

CREATE TABLE analytics_top_skills_by_country
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_top_skills_by_country'
AS
SELECT
    search_country_clean AS search_country,
    skill,
    COUNT(*) AS skill_mentions,
    COUNT(DISTINCT job_link) AS posting_count
FROM analytics_job_skills_exploded
WHERE search_country_clean <> 'unknown'
GROUP BY search_country_clean, skill;

CREATE TABLE analytics_skills_by_job_level
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_skills_by_job_level'
AS
SELECT
    job_level_clean AS job_level,
    skill,
    COUNT(*) AS skill_mentions,
    COUNT(DISTINCT job_link) AS posting_count
FROM analytics_job_skills_exploded
WHERE job_level_clean <> 'unknown'
GROUP BY job_level_clean, skill;

CREATE TABLE analytics_job_type_distribution
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_job_type_distribution'
AS
SELECT
    job_type_clean AS job_type,
    COUNT(*) AS posting_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS posting_pct
FROM analytics_clean_jobs
GROUP BY job_type_clean;

CREATE TABLE analytics_job_level_distribution
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_job_level_distribution'
AS
SELECT
    search_country_clean AS search_country,
    job_level_clean AS job_level,
    COUNT(*) AS posting_count
FROM analytics_clean_jobs
WHERE search_country_clean <> 'unknown'
GROUP BY search_country_clean, job_level_clean;

CREATE TABLE analytics_top_companies
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_top_companies'
AS
SELECT
    company_clean AS company,
    COUNT(*) AS posting_count,
    COUNT(DISTINCT search_country_clean) AS country_count,
    COUNT(DISTINCT search_position_clean) AS position_count
FROM analytics_clean_jobs
WHERE company_clean <> 'unknown'
GROUP BY company_clean;

CREATE TABLE analytics_demand_by_date
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_demand_by_date'
AS
SELECT
    to_date(first_seen) AS first_seen_date,
    search_country_clean AS search_country,
    search_position_clean AS search_position,
    COUNT(*) AS posting_count
FROM analytics_clean_jobs
WHERE first_seen IS NOT NULL
  AND search_country_clean <> 'unknown'
  AND search_position_clean <> 'unknown'
GROUP BY to_date(first_seen), search_country_clean, search_position_clean;

CREATE TABLE analytics_country_position_matrix
STORED AS PARQUET
LOCATION '${hiveconf:HDFS_BASE}/analytics/analytics_country_position_matrix'
AS
SELECT
    search_country_clean AS search_country,
    search_position_clean AS search_position,
    COUNT(*) AS posting_count,
    COUNT(DISTINCT company_clean) AS company_count
FROM analytics_clean_jobs
WHERE search_country_clean <> 'unknown'
  AND search_position_clean <> 'unknown'
GROUP BY search_country_clean, search_position_clean;
