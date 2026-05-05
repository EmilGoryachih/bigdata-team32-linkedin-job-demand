CREATE DATABASE IF NOT EXISTS ${hiveconf:HIVE_DB};
USE ${hiveconf:HIVE_DB};

DROP TABLE IF EXISTS linkedin_job_postings_raw;
DROP TABLE IF EXISTS job_skills_raw;
DROP TABLE IF EXISTS job_summary_raw;

CREATE EXTERNAL TABLE linkedin_job_postings_raw (
    job_link STRING,
    last_processed_time STRING,
    got_summary STRING,
    got_ner STRING,
    is_being_worked STRING,
    job_title STRING,
    company STRING,
    job_location STRING,
    first_seen STRING,
    search_city STRING,
    search_country STRING,
    search_position STRING,
    job_level STRING,
    job_type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:HDFS_BASE}/raw/linkedin_job_postings';

CREATE EXTERNAL TABLE job_skills_raw (
    job_link STRING,
    job_skills STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:HDFS_BASE}/raw/job_skills';

CREATE EXTERNAL TABLE job_summary_raw (
    job_link STRING,
    job_summary STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:HDFS_BASE}/raw/job_summary';