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
