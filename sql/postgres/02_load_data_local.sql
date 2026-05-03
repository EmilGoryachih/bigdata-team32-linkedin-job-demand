COPY linkedin_job_postings (
    job_link,
    last_processed_time,
    got_summary,
    got_ner,
    is_being_worked,
    job_title,
    company,
    job_location,
    first_seen,
    search_city,
    search_country,
    search_position,
    job_level,
    job_type
)
FROM '/workspace/data/raw/linkedin_job_postings.csv'
WITH (
    FORMAT csv,
    HEADER true,
    QUOTE '"',
    ESCAPE '"'
);

COPY job_skills (
    job_link,
    job_skills
)
FROM '/workspace/data/raw/job_skills.csv'
WITH (
    FORMAT csv,
    HEADER true,
    QUOTE '"',
    ESCAPE '"'
);

COPY job_summary (
    job_link,
    job_summary
)
FROM '/workspace/data/raw/job_summary.csv'
WITH (
    FORMAT csv,
    HEADER true,
    QUOTE '"',
    ESCAPE '"'
);