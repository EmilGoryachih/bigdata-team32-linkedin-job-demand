DROP TABLE IF EXISTS job_summary;
DROP TABLE IF EXISTS job_skills;
DROP TABLE IF EXISTS linkedin_job_postings;

CREATE TABLE linkedin_job_postings (
    job_link TEXT PRIMARY KEY,
    last_processed_time TIMESTAMPTZ,
    got_summary BOOLEAN,
    got_ner BOOLEAN,
    is_being_worked BOOLEAN,
    job_title TEXT,
    company TEXT,
    job_location TEXT,
    first_seen DATE,
    search_city TEXT,
    search_country TEXT,
    search_position TEXT,
    job_level TEXT,
    job_type TEXT
);

CREATE TABLE job_skills (
    job_link TEXT,
    job_skills TEXT
);

CREATE TABLE job_summary (
    job_link TEXT,
    job_summary TEXT
);

CREATE INDEX idx_job_skills_job_link ON job_skills(job_link);
CREATE INDEX idx_job_summary_job_link ON job_summary(job_link);

CREATE INDEX idx_postings_first_seen ON linkedin_job_postings(first_seen);
CREATE INDEX idx_postings_last_processed_time ON linkedin_job_postings(last_processed_time);
CREATE INDEX idx_postings_search_city ON linkedin_job_postings(search_city);
CREATE INDEX idx_postings_search_country ON linkedin_job_postings(search_country);
CREATE INDEX idx_postings_job_level ON linkedin_job_postings(job_level);
CREATE INDEX idx_postings_job_type ON linkedin_job_postings(job_type);