\echo '=== Row counts ==='

SELECT 'linkedin_job_postings' AS table_name, COUNT(*) AS row_count
FROM linkedin_job_postings
UNION ALL
SELECT 'job_skills', COUNT(*)
FROM job_skills
UNION ALL
SELECT 'job_summary', COUNT(*)
FROM job_summary;

\echo '=== Distinct job links ==='

SELECT 'linkedin_job_postings' AS table_name, COUNT(DISTINCT job_link) AS distinct_job_links
FROM linkedin_job_postings
UNION ALL
SELECT 'job_skills', COUNT(DISTINCT job_link)
FROM job_skills
UNION ALL
SELECT 'job_summary', COUNT(DISTINCT job_link)
FROM job_summary;

\echo '=== Time range ==='

SELECT
    MIN(first_seen) AS min_first_seen,
    MAX(first_seen) AS max_first_seen,
    MIN(last_processed_time) AS min_last_processed_time,
    MAX(last_processed_time) AS max_last_processed_time
FROM linkedin_job_postings;

\echo '=== Join coverage ==='

SELECT
    COUNT(*) AS postings_total,
    COUNT(s.job_link) AS postings_with_skills,
    COUNT(sm.job_link) AS postings_with_summary
FROM linkedin_job_postings p
LEFT JOIN job_skills s ON p.job_link = s.job_link
LEFT JOIN job_summary sm ON p.job_link = sm.job_link;

\echo '=== Job level distribution ==='

SELECT job_level, COUNT(*) AS cnt
FROM linkedin_job_postings
GROUP BY job_level
ORDER BY cnt DESC;

\echo '=== Job type distribution ==='

SELECT job_type, COUNT(*) AS cnt
FROM linkedin_job_postings
GROUP BY job_type
ORDER BY cnt DESC;