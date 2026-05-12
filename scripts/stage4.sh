#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
fi

: "${HIVE_DB:=team32_linkedin}"
: "${HDFS_BASE:=/user/team32/linkedin}"
: "${BEELINE_URL:=jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default}"

mkdir -p "$PROJECT_ROOT/output"

cat > "$PROJECT_ROOT/output/dashboard_tables.txt" <<EOF
Superset Hive database:
${HIVE_DB}

Primary source table:
${HIVE_DB}.linkedin_jobs_enriched

Hive analytics tables:
${HIVE_DB}.analytics_data_characteristics
${HIVE_DB}.analytics_null_coverage
${HIVE_DB}.analytics_clean_jobs
${HIVE_DB}.analytics_job_skills_exploded
${HIVE_DB}.analytics_top_positions_global
${HIVE_DB}.analytics_top_positions_by_country
${HIVE_DB}.analytics_top_skills_global
${HIVE_DB}.analytics_top_skills_by_country
${HIVE_DB}.analytics_skills_by_job_level
${HIVE_DB}.analytics_job_type_distribution
${HIVE_DB}.analytics_job_level_distribution
${HIVE_DB}.analytics_top_companies
${HIVE_DB}.analytics_demand_by_date
${HIVE_DB}.analytics_country_position_matrix

Hive analytics HDFS base:
${HDFS_BASE}/analytics

Spark analytics HDFS base:
${HDFS_BASE}/analytics_spark
EOF

cat > "$PROJECT_ROOT/output/dashboard_chart_plan.md" <<'EOF'
# Superset Dashboard Chart Plan

## Connection

Connect Apache Superset to HiveServer2 / Beeline-compatible Hive using the
cluster connection details. Use the Hive database `team32_linkedin`, then add
the analytics tables as Superset datasets.

## Charts

| # | Superset dataset | Chart type | Main columns | Purpose |
|---|---|---|---|---|
| 1 | analytics_data_characteristics | Table / Big Number | metric_name, metric_value | Show dataset size, feature count, coverage, and date range. |
| 2 | analytics_null_coverage | Bar chart | column_name, missing_pct | Show data quality and missing-value risk before analysis. |
| 3 | analytics_top_positions_global | Bar chart | search_position, posting_count | Show the most demanded job positions globally. |
| 4 | analytics_top_positions_by_country | Heatmap / grouped bar | search_country, search_position, posting_count | Compare job role demand across countries. |
| 5 | analytics_top_skills_global | Bar chart | skill, skill_mentions | Show the most requested skills across the full market. |
| 6 | analytics_top_skills_by_country | Filterable bar chart | search_country, skill, skill_mentions | Compare skill demand by country. |
| 7 | analytics_skills_by_job_level | Stacked bar / table | job_level, skill, skill_mentions | Explain how skill expectations change by seniority. |
| 8 | analytics_job_type_distribution | Pie / bar chart | job_type, posting_count, posting_pct | Show distribution of work arrangements or employment types. |
| 9 | analytics_job_level_distribution | Stacked bar | search_country, job_level, posting_count | Compare seniority mix across countries. |
| 10 | analytics_top_companies | Bar chart | company, posting_count | Identify the companies driving the largest hiring volume. |
| 11 | analytics_demand_by_date | Time-series line chart | first_seen_date, posting_count | Show posting demand dynamics over the available date range. |
| 12 | analytics_country_position_matrix | Heatmap | search_country, search_position, posting_count | Show market concentration by country and role. |

## Recommended Filters

- search_country
- search_position
- job_level
- job_type
- first_seen_date
EOF

cat > "$PROJECT_ROOT/output/dashboard_storytelling.md" <<'EOF'
# Dashboard Storytelling

## 1. Global role demand

The top positions chart identifies which roles dominate LinkedIn job demand.
This helps workforce planners and education providers focus on the roles with
the largest hiring signal.

## 2. Country-specific role demand

The country-position view shows that demand is not uniform across markets.
Business stakeholders can use this to decide where to recruit, where to open
regional teams, and which roles are concentrated in specific countries.

## 3. Global skill demand

The top skills chart highlights the most frequently requested capabilities.
This is useful for candidates, training providers, and hiring teams that need
to align job descriptions with market expectations.

## 4. Skill demand by country

Skill demand differs by geography because local industries and hiring markets
have different needs. The country skill chart supports region-specific talent
strategy instead of one global hiring assumption.

## 5. Skills by job level

Skill requirements change with seniority. This chart helps distinguish skills
expected from junior roles from skills associated with senior or leadership
positions.

## 6. Job type distribution

The job type distribution explains how the market is split by work arrangement
or employment type. This helps companies benchmark whether their job offers are
aligned with market norms.

## 7. Top hiring companies

The top companies chart identifies organizations with the highest posting
volume. These companies can be treated as market leaders or competitors for
talent.

## 8. Demand over time

The date trend shows how posting activity changes over the available collection
period. Spikes or drops can be used to discuss seasonality, scraping coverage,
or market demand shifts.
EOF

echo "Dashboard artifacts written to output/."
