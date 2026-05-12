# EDA And Dashboard Analytics

This project analyzes LinkedIn job demand with Hive, Spark, and Apache Superset.
The EDA and dashboard layer uses the enriched dataset as the source of truth:

```text
team32_linkedin.linkedin_jobs_enriched
```

HDFS source path:

```text
/user/team32/linkedin/enriched/linkedin_jobs_enriched
```

## Cluster Execution

Run these steps on the Hadoop/YARN cluster from the repository root. Do not run
Spark jobs on a local machine for final results.

```bash
export HDFS_BASE=/user/team32/linkedin
export HIVE_DB=team32_linkedin
export BEELINE_URL="jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default"
export HIVE_USER=team32
export HIVE_PASSWORD='PUT_PASSWORD_HERE'
export SPARK_MASTER=yarn
export SPARK_DEPLOY_MODE=client

bash scripts/stage2.sh
bash scripts/stage4.sh
```

`scripts/stage2.sh` first creates the raw, Parquet, and enriched Hive tables,
then creates Hive dashboard analytics tables, then runs Spark SQL EDA on YARN.

## Hive Analytics Tables

The HiveQL analytics layer is implemented in:

```text
sql/hive/05_analytics_queries.hql
```

It creates dashboard-ready Parquet tables under:

```text
/user/team32/linkedin/analytics/<table_name>
```

Tables:

| Table | Purpose |
|---|---|
| analytics_data_characteristics | Dataset size, feature count, feature names, coverage, and date range. |
| analytics_null_coverage | Missing-value coverage for key fields. |
| analytics_clean_jobs | Cleaned base table with normalized text fields. |
| analytics_job_skills_exploded | One row per job-skill pair. |
| analytics_top_positions_global | Most demanded job positions globally. |
| analytics_top_positions_by_country | Role demand by country. |
| analytics_top_skills_global | Most demanded skills globally. |
| analytics_top_skills_by_country | Skill demand by country. |
| analytics_skills_by_job_level | Skill demand by seniority level. |
| analytics_job_type_distribution | Distribution of job types/work arrangements. |
| analytics_job_level_distribution | Seniority distribution by country. |
| analytics_top_companies | Companies with the highest number of postings. |
| analytics_demand_by_date | Posting demand over time. |
| analytics_country_position_matrix | Country-role demand matrix for heatmaps. |

## Spark SQL Analytics

The Spark EDA script is:

```text
scripts/stage2_spark_eda.py
```

It reads the enriched Parquet dataset, cleans the key categorical columns,
explodes comma-separated skills, creates Spark SQL temp views, and writes
Spark-generated analytics to:

```text
/user/team32/linkedin/analytics_spark/
```

Spark outputs include:

| Output | Purpose |
|---|---|
| spark_top_positions_global | Spark SQL version of global role demand. |
| spark_top_skills_global | Spark SQL version of global skill demand. |
| spark_demand_by_country | Posting volume by country. |
| spark_job_type_distribution | Spark SQL job type distribution. |
| spark_skill_country_matrix | Skill demand by country. |
| spark_data_characteristics | Spark DataFrame data characteristics. |
| spark_skills_by_job_level | Spark DataFrame skills by seniority. |

Small CSV previews are written to `output/` for quick checking.

## Business Insights

### 1. Global Role Demand

Dataset: `analytics_top_positions_global`

Chart: horizontal bar chart with `search_position` on the Y axis and
`posting_count` on the X axis.

Story: the chart identifies which job roles dominate global LinkedIn demand.
This helps business stakeholders prioritize hiring plans, curriculum design,
and talent acquisition budgets around the roles with the largest market signal.

### 2. Country-Specific Role Demand

Dataset: `analytics_top_positions_by_country` or
`analytics_country_position_matrix`

Chart: heatmap with `search_country` and `search_position`, colored by
`posting_count`.

Story: job demand is not evenly distributed across countries. The same role can
be highly demanded in one market and much less visible in another, which helps
companies make region-specific recruiting and expansion decisions.

### 3. Global Skill Demand

Dataset: `analytics_top_skills_global`

Chart: bar chart with `skill` and `skill_mentions`.

Story: the most frequently requested skills show what employers expect from
candidates across the market. Candidates can use this to prioritize learning,
and companies can compare their job descriptions against market norms.

### 4. Skill Demand By Country

Dataset: `analytics_top_skills_by_country`

Chart: filterable bar chart by `search_country`.

Story: skill demand varies by geography because local industries and technical
stacks differ. This chart supports localized hiring and training strategies
instead of assuming one global skill profile.

### 5. Skills By Job Level

Dataset: `analytics_skills_by_job_level`

Chart: stacked bar chart or pivot table with `job_level`, `skill`, and
`skill_mentions`.

Story: junior, mid-level, and senior roles have different skill expectations.
This helps explain career progression and shows which capabilities become more
important at higher seniority.

### 6. Job Type Distribution

Dataset: `analytics_job_type_distribution`

Chart: bar chart or pie chart with `job_type` and `posting_pct`.

Story: this chart explains how the market is split by job type or work
arrangement. Hiring teams can use it to benchmark whether their offers match
market expectations.

### 7. Top Hiring Companies

Dataset: `analytics_top_companies`

Chart: bar chart with `company` and `posting_count`.

Story: companies with the highest posting volume are the strongest demand
drivers in the dataset. They can be interpreted as competitors for talent or
as market leaders in active hiring.

### 8. Demand Over Time

Dataset: `analytics_demand_by_date`

Chart: time-series line chart with `first_seen` and `posting_count`.

Story: the time-series view shows how posting activity changes during the
available collection window. It helps identify spikes, drops, and possible
collection-period effects.

## Superset Setup

1. Connect Superset to HiveServer2 using the cluster Hive connection.
2. Select the `team32_linkedin` database.
3. Add each `analytics_*` table as a Superset dataset.
4. Build the charts listed above.
5. Add filters for `search_country`, `search_position`, `job_level`,
   `job_type`, and `first_seen`.
6. Put the data characteristics and null coverage charts at the top of the
   dashboard, followed by role demand, skill demand, company demand, and time
   trend sections.

Stage 4 generates dashboard support files in `output/`:

```text
output/dashboard_tables.txt
output/dashboard_chart_plan.md
output/dashboard_storytelling.md
```
