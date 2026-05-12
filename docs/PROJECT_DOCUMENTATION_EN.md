# LinkedIn Job Demand — Big Data Final Project (Team 32)

This document is a **detailed, project-wide overview in English**: repository layout, pipeline stages, data locations, analytics and ML layers, known operational issues, and how the work maps to the official course requirements.

**Official course specification:** [BS — Final Project (Big Data, IU)](https://firas-jolha.github.io/bigdata/html/bs/BS%20-%20Final%20Project.html)

**Related documentation in this repository:**

| Document | Focus |
|----------|--------|
| [`README.md`](../README.md) | Template rules, cluster entry points, main tables and paths |
| [`docs/eda_dashboard.md`](eda_dashboard.md) | Hive analytics tables, Spark EDA outputs, business insights, Superset setup |
| [`docs/stage3_ml_report.md`](stage3_ml_report.md) | Full ML methodology, metrics, hyperparameter grids, per-model analysis |

---

## 1. Executive summary

The project implements an **end-to-end big data pipeline** for **LinkedIn job postings**: relational staging in **PostgreSQL**, bulk transfer to **HDFS** with **Sqoop**, **Hive** layers (raw, **Parquet** with compression, **enriched** join), **exploratory data analysis (EDA)** using **HiveQL** and **Spark SQL on YARN**, **machine learning** with **Spark MLlib** (classification, cross-validation, hyperparameter search), and **dashboard planning** for **Apache Superset** (plus CSV fallbacks).

**Canonical analytics source table (Hive):**

```text
team32_linkedin.linkedin_jobs_enriched
```

**Canonical HDFS path (enriched Parquet):**

```text
/user/team32/linkedin/enriched/linkedin_jobs_enriched
```

The enriched table is the **main hand-off** from data engineering to EDA, dashboard analytics, and ML.

---

## 2. Course requirements (mapping)

The course mandates (high level):

1. **Ingestion:** relational model in PostgreSQL (distributed/Citus as applicable), load data, **Sqoop** to HDFS.
2. **Storage / preparation:** **Hive** tables using efficient, compressed big-data formats (**Parquet**, **Avro**, etc.).
3. **Analysis:** **EDA with HiveQL** (Tez engine in the course description), **Spark DataFrame/SQL**, **predictive analysis** with **Spark MLlib** (feature work, preprocessing, models, tuning, cross-validation, metrics).
4. **Presentation:** **Apache Superset** dashboard with data characteristics, **at least six** valuable business insights (query + chart + **data storytelling**), model performance, predictions, visual quality.
5. **Deliverables:** **public or private Git repository** (following the provided template), **written report**, **defence presentation**. Dataset size and feature rules (e.g. ≥500k rows, ≥500 MB, ≥6 features, datetime/geospatial, non-lab dataset) apply as stated on the course page.

**This repository** follows the course template (`main.sh` is fixed for grading), keeps executable logic under `scripts/`, Hive scripts under `sql/hive/`, and writes reproducible artefacts under `output/`.

---

## 3. Repository layout

| Path | Role |
|------|------|
| `main.sh` | **Do not modify.** Orchestrates preprocess → stage1 → stage2 → stage3 → stage4 → postprocess → `pylint scripts`. |
| `scripts/` | All pipeline `.sh` and `.py` scripts (assessment expects no dependency on `notebooks/`). |
| `sql/hive/` | HiveQL scripts for raw, Parquet, enriched, validation, and analytics layers. |
| `data/` | Dataset files (typically gitignored; may live on cluster). |
| `output/` | CSV summaries, logs, ML metrics, dashboard planning files, etc. |
| `models/` | Saved Spark ML model metadata / pointers as produced by the project. |
| `notebooks/` | Learning only; pipeline must run without it. |
| `docs/` | Human-readable documentation (EDA/dashboard, ML report, this file). |

---

## 4. Environment and cluster identifiers

Typical **environment variables** (see `docs/eda_dashboard.md` and scripts) for running on the **university Hadoop/YARN** cluster:

| Variable | Example / purpose |
|----------|-------------------|
| `HDFS_BASE` | `/user/team32/linkedin` — root for LinkedIn artefacts |
| `HIVE_DB` | `team32_linkedin` — Hive database name |
| `BEELINE_URL` | JDBC URL to HiveServer2 (e.g. `jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default`) |
| `HIVE_USER` / `HIVE_PASSWORD` | Credentials for Beeline |
| `SPARK_MASTER` | `yarn` |
| `SPARK_DEPLOY_MODE` | `client` (as used in `scripts/stage2.sh`) |

Place secrets in a **`.env`** file at the repository root (sourced by several scripts); do not commit credentials.

**Important:** Final Spark jobs for course credit should run on **YARN**, not on a local-only Spark session.

---

## 5. Pipeline stages (detailed)

### 5.1 Stage 1 — PostgreSQL and Sqoop

**Purpose:** Load source data into a **relational staging** layer, validate structure and volume, then **import into HDFS** for the Hadoop ecosystem.

**Orchestration:** `scripts/stage1.sh`

- Runs `scripts/stage1_postgres_load.sh` (and related loaders such as `load_postgres.py` / `local_postgres_load.sh` as applicable to your environment).
- Runs `scripts/stage1_sqoop_import.sh` to copy PostgreSQL tables into **HDFS raw** locations consumed by Hive.

**Outcome:** Raw data available in HDFS for Hive external/managed tables defined in Stage 2.

---

### 5.2 Stage 2 — Hive, Parquet, enriched dataset, EDA

**Orchestration:** `scripts/stage2.sh`

Execution order inside Stage 2:

1. **`scripts/stage2_hive_tables.sh`** — creates Hive raw, Parquet, and enriched objects using the numbered HiveQL files.
2. **`scripts/stage2_analytics.sh`** — runs Beeline against **`sql/hive/05_analytics_queries.hql`** to build **dashboard analytics** tables.
3. **`spark-submit`** — runs **`scripts/stage2_spark_eda.py`** on **YARN** (`SPARK_MASTER`, `SPARK_DEPLOY_MODE` from environment), reading enriched Parquet and writing Spark analytics to HDFS plus small CSV previews to `output/`.

#### 5.2.1 HiveQL scripts (`sql/hive/`)

| File | Responsibility |
|------|----------------|
| `01_create_raw_tables.hql` | Hive tables over **raw** Sqoop/HDFS data. |
| `02_create_parquet_tables.hql` | **Parquet** (e.g. **Snappy** compression) tables for efficient analytics and Spark reads. |
| `03_create_enriched_table.hql` | **Enriched** table: joins core entities on **`job_link`** into **`linkedin_jobs_enriched`**. |
| `04_validation_queries.hql` | Row counts, coverage checks, sample queries. |
| `05_analytics_queries.hql` | **Analytics layer:** cleaned views, aggregates, and **`analytics_*`** Parquet tables for dashboards. |

**Core entity tables** (logical names; exact Hive names follow your DDL) include postings, skills, and summaries — e.g. `linkedin_job_postings`, `job_skills`, `job_summary` — materialised as Parquet tables and then joined.

**Enriched table** fields (conceptually): job title, company, location, country, position, level, type, skills, summary — unified for downstream SQL, Spark, and ML.

#### 5.2.2 Validated scale (example numbers from project runs)

These figures are useful for reports and defences; re-verify on the cluster if needed.

| Object | Rows |
|--------|------:|
| `linkedin_job_postings_parquet` | 1,348,454 |
| `job_skills_parquet` | 1,296,381 |
| `job_summary_parquet` | 1,297,332 |
| `linkedin_jobs_enriched` | 1,348,454 |

**Coverage:**

- Rows with skills: **1,294,374**
- Rows with summary: **1,297,332**

**Approximate storage:**

- Logical size ~**1.9 GB**
- Replicated HDFS footprint ~**5.8 GB** (depends on replication factor)

#### 5.2.3 HDFS quota incident

During heavy joins/materialisations, the cluster raised **`DSQuotaExceededException`** (quota on the order of **32 GB** including **replication**). Causes included accumulation of **raw + Parquet + enriched + Trash**. **Mitigation:** remove redundant raw copies, old enriched outputs, and empty **Trash** so the pipeline could complete.

#### 5.2.4 Hive execution engine (Tez vs MapReduce)

On the shared cluster, **Tez** sometimes failed with:

```text
Too many counters: 121 max=120
```

This is a **cluster-level counter limit**, not a logic error in HiveQL. **Workaround:** run the analytics DDL with **MapReduce** as the Hive execution engine so jobs remain **reproducible**. Validation `SELECT` queries may still appear as Tez DAGs in Beeline logs depending on session defaults.

#### 5.2.5 Hive analytics tables (`05_analytics_queries.hql`)

Physical location pattern:

```text
/user/team32/linkedin/analytics/<table_name>
```

**Tables / purposes** (from `docs/eda_dashboard.md`):

| Table | Purpose |
|-------|---------|
| `analytics_data_characteristics` | Row counts, feature counts/names, coverage, date range. |
| `analytics_null_coverage` | Missing-value rates for key columns. |
| `analytics_clean_jobs` | Cleaned/normalised text fields (if materialised in your version). |
| `analytics_job_skills_exploded` | One row per job–skill pair (if materialised; the project may use **views** to avoid huge intermediates). |
| `analytics_top_positions_global` | Global demand for job titles/positions. |
| `analytics_top_positions_by_country` | Position demand by country. |
| `analytics_top_skills_global` | Global skill frequency. |
| `analytics_top_skills_by_country` | Skills by country. |
| `analytics_skills_by_job_level` | Skills vs seniority. |
| `analytics_job_type_distribution` | Job type / arrangement mix. |
| `analytics_job_level_distribution` | Seniority mix (e.g. by country). |
| `analytics_top_companies` | Employers with highest posting volume. |
| `analytics_demand_by_date` | Posting counts over time (within available date range). |
| `analytics_country_position_matrix` | Heatmap-friendly country × position counts. |

**Runner:** `bash scripts/stage2_analytics.sh` — executes the HQL via **Beeline** and exports small **CSV** summaries to **`output/`** for quick inspection and fallback charting.

#### 5.2.6 Spark SQL EDA (`scripts/stage2_spark_eda.py`)

- **Input:** enriched Parquet at `HDFS_BASE/enriched/linkedin_jobs_enriched`.
- **Output directory (HDFS):** `HDFS_BASE/analytics_spark/` (e.g. `/user/team32/linkedin/analytics_spark/`).
- **Local CSV previews:** `PROJECT_ROOT/output/`.

**Typical Spark outputs** (Parquet datasets on HDFS):

| Output | Role |
|--------|------|
| `spark_top_positions_global` | Spark-side global role demand. |
| `spark_top_skills_global` | Spark-side global skill demand. |
| `spark_demand_by_country` | Volume by country. |
| `spark_job_type_distribution` | Job type distribution. |
| `spark_skill_country_matrix` | Skill × country matrix. |
| `spark_data_characteristics` | Dataset characteristics from Spark. |
| `spark_skills_by_job_level` | Skills by seniority from Spark. |

**Scale (reported run):** ~**1,348,454** cleaned rows; after **exploding** comma-separated skills, **~26,925,318** skill-level rows.

**Example submit** (as wired in `scripts/stage2.sh`):

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.sql.session.timeZone=UTC \
  scripts/stage2_spark_eda.py \
  --enriched-path "$HDFS_BASE/enriched/linkedin_jobs_enriched" \
  --spark-analytics-path "$HDFS_BASE/analytics_spark" \
  --output-dir "$PROJECT_ROOT/output"
```

---

### 5.3 Stage 3 — Machine learning (Spark MLlib)

**Orchestration:** `scripts/stage3.sh`

**Key scripts:**

| Script | Role |
|--------|------|
| `scripts/stage3_prepare_dataset.py` | Build ML-ready dataset from Hive/HDFS enriched data; labels and features. |
| `scripts/stage3_train_models.py` | Train/tune models, cross-validation, persist to HDFS, write metrics to `output/`. |
| `scripts/stage3_predict_samples.py` | Score example rows for dashboard/report demos. |

**HDFS ML area:** `/user/team32/linkedin/ml/` (models and related artefacts).

#### 5.3.1 Problem formulation

- **Source:** `team32_linkedin.linkedin_jobs_enriched` (**1,348,454** rows).
- **Aggregation unit:** one row per **`(search_country, search_position)`** after grouping postings.
- **Label `high_demand`:** binary indicator — group is “high demand” if its posting count exceeds the **approximate 75th percentile** of counts **within the same `search_position`** (see `docs/stage3_ml_report.md` for the exact definition).

**Important data limitation (documented honestly in the ML report):** timestamp fields such as `first_seen` only span about **six days** of scraper collection and behave like **collection artefacts**, not long-horizon posting dates. A planned time-series aggregation was **simplified** to static aggregation.

#### 5.3.2 Dataset sizes (after filters)

| Metric | Value |
|--------|------:|
| Groups before sparse filter | 6,177 |
| Groups after `group_count >= 5` | 4,426 |
| Train rows | 3,542 |
| Test rows | 884 |
| Train split | ~80% |
| Test split | ~20% |
| Random seed | 42 |
| Positive rate (train / test) | ~13.5% / ~13.2% |

Class **imbalance** strongly affects models that default toward the majority class.

#### 5.3.3 Features and leakage avoidance

`group_count` is **not** used as a feature (it would **leak** the label). Raw `distinct_companies` / `distinct_skills` with trivial upper bounds tied to `group_count` are also excluded; the pipeline uses **ratio-style** features (e.g. skills per posting, summary coverage, companies per posting) plus high-cardinality **one-hot** encodings for `search_country` and `search_position` (via `StringIndexer` + `OneHotEncoder`). See **`docs/stage3_ml_report.md`** for the full feature table and dimensionality (~**1,662** dimensions after assembly).

#### 5.3.4 Models, tuning, and validation

**Models:** Random Forest, **Gradient-Boosted Trees (GBT)**, **LinearSVC (SVM)**, **Gaussian Naive Bayes**, plus a **soft-voting ensemble** (RF + GBT + SVM; NB excluded from averaging due to poor calibration).

**Tuning:** `CrossValidator`, **3-fold** CV on the **training** set only, F1 as the selection metric, parallelism and grids documented in **`docs/stage3_ml_report.md`**.

**Held-out test metrics** (summary — see CSVs in `output/` for exact runs):

| Model | Accuracy | F1 (weighted) | AUC |
|-------|----------|---------------|-----|
| **GBT** | **~0.898** | **~0.896** | **~0.936** |
| Ensemble | ~0.891 | ~0.875 | ~0.931 |
| RF | ~0.868 | ~0.806 | ~0.908 |
| SVM | ~0.856 | ~0.868 | ~0.898 |
| NB | ~0.610 | ~0.657 | ~0.807 |

**Recommendation for dashboards:** **GBT** — best overall on reported metrics and more balanced on the minority positive class than RF/NB in this setup.

Additional artefacts: confusion matrices, CV grid CSVs (e.g. `output/stage3_rf_cv_grid.csv`), best-parameter text files under `output/`.

---

### 5.4 Stage 4 — Dashboard planning and Superset

**Script:** `scripts/stage4.sh`

This stage **does not** programmatically create a Superset dashboard. It **generates planning artefacts** under `output/`:

| File | Contents |
|------|----------|
| `output/dashboard_tables.txt` | Hive DB name, `linkedin_jobs_enriched`, full list of **`analytics_*`** tables, HDFS bases for Hive vs Spark analytics. |
| `output/dashboard_chart_plan.md` | Chart types, datasets, main columns (extends the course “query + chart” requirement into an actionable checklist). |
| `output/dashboard_storytelling.md` | Short narrative per insight for **data storytelling**. |

#### 5.4.1 Superset connectivity issue

The team attempted to connect **Apache Superset** to **HiveServer2**. Observed failures included **“Apache Hive Error: Could not connect”** and unstable schema/table discovery for **`team32_linkedin`**. This points to **infrastructure / connector** problems between Superset and the cluster Hive service, **not** missing Hive tables.

**Fallback strategy:**

1. Use **CSV exports** in `output/` (and any archived **`evidence/`** bundles) to build charts in any BI tool or slides.
2. When Hive connectivity is restored, register **`team32_linkedin.analytics_*`** as Superset datasets and implement the chart plan.

---

## 6. Business insights (course-aligned)

The course requires **at least six** insights with charts and narrative. **`docs/eda_dashboard.md`** maps **eight** insight themes to concrete **`analytics_*`** tables, chart types, and stakeholder stories:

1. **Global role demand** — `analytics_top_positions_global`
2. **Country-specific role demand** — `analytics_top_positions_by_country` / `analytics_country_position_matrix`
3. **Global skill demand** — `analytics_top_skills_global`
4. **Skill demand by country** — `analytics_top_skills_by_country`
5. **Skills by job level** — `analytics_skills_by_job_level`
6. **Job type distribution** — `analytics_job_type_distribution`
7. **Top hiring companies** — `analytics_top_companies`
8. **Demand over time** — `analytics_demand_by_date`

Recommended **Superset filters:** `search_country`, `search_position`, `job_level`, `job_type`, date/`first_seen` as available.

---

## 7. Evidence and reproducibility

For grading and team hand-off, the project maintains **logs and exports**, for example:

- Beeline / Hive analytics logs (e.g. `evidence/stage2_analytics.log` when archived)
- Spark driver logs (e.g. `evidence/stage2_spark_eda.log`)
- CSV summaries: Hive analytics and Spark (`output/analytics_*.csv`, `output/spark_*.csv`, etc.)
- HDFS directory listings
- Dashboard markdown plans

**Archives** (names may vary by packaging run): e.g. `evidence_bigdata_team32_eda.tar.gz`, `final_bigdata_team32_eda_dashboard_artifacts.tar.gz`.

**Reproducibility principle:** scripts should tolerate **re-runs** (drop/recreate or idempotent patterns) so a second execution of a stage does not fail on “object already exists” — align with course template expectations.

---

## 8. Code quality

`main.sh` ends with **`pylint scripts`** for static analysis of Python under `scripts/`, per template and course rubric items on code quality.

---

## 9. Quick command reference (cluster)

From repository root, after configuring `.env` / exports:

```bash
bash scripts/stage1.sh    # PostgreSQL + Sqoop
bash scripts/stage2.sh    # Hive tables + Hive analytics + Spark EDA on YARN
bash scripts/stage3.sh    # ML prepare + train + predict artefacts
bash scripts/stage4.sh    # Dashboard planning files → output/
```

Full pipeline (as graders may run):

```bash
bash main.sh
```

---

## 10. Reading Spark / Hive from applications

**Hive table API (Spark):**

```python
df = spark.table("team32_linkedin.linkedin_jobs_enriched")
```

**Direct Parquet path:**

```python
df = spark.read.parquet("/user/team32/linkedin/enriched/linkedin_jobs_enriched")
```

Use the same pattern for `analytics_*` paths under `/user/team32/linkedin/analytics/` and Spark outputs under `/user/team32/linkedin/analytics_spark/`.

---

## 11. Glossary

| Term | Meaning |
|------|---------|
| **EDA** | Exploratory Data Analysis |
| **HQL / HiveQL** | SQL dialect for Hive |
| **Parquet** | Columnar binary format, efficient for analytics |
| **Sqoop** | Bulk transfer between RDBMS and HDFS |
| **Tez / MR** | Hive execution engines (DAG vs MapReduce) |
| **YARN** | Hadoop resource manager; Spark **cluster** mode for the course |

---

*End of document. For slide-style English Markdown (Marp), see `presentation_team32_eda_pipeline.md` in the repository root.*
