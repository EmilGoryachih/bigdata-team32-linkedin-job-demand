# LinkedIn Job Demand Analytics — Team 32

End-to-end Big Data pipeline that ingests 1.35 M LinkedIn job postings,
stores them in a Hive warehouse, runs distributed ML to classify
country/position combinations as high- or low-demand, and presents the
results in an Apache Superset dashboard.

Course: **Big Data**, Innopolis University — final project.

## Team

Four-person team (Team 32). Per-task contribution percentages are in the
project report's "Reflections" section.

## Dataset

* Source: [1.3 M LinkedIn Jobs and Skills (2024)](https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024/)
* Records: **1 348 454** job postings
* Size: **6.19 GB** raw CSV (compressed to ~5 GB Parquet on HDFS)
* Three source CSVs joined on `job_link`: `linkedin_job_postings.csv`,
  `job_skills.csv`, `job_summary.csv`
* Includes both **datetime** (`last_processed_time` — TIMESTAMPTZ) and
  **geospatial** features (`search_country`, `search_city`,
  `job_location`), satisfying the dataset criterion.

The dataset is not committed to the repo (it lives on the cluster and in
HDFS under `/user/team32/linkedin/`). See `.gitignore`.

## Predictive task

Binary classification: predict whether a `(search_country,
search_position)` group has **high demand** (top quartile of postings
count for that position) or not.

The label `high_demand` is engineered — see Stage 3 docs for the exact
formula.

## Architecture

```
                CSV (Kaggle)
                     │
   ┌─────────────────▼─────────────────┐
   │  STAGE 1                          │
   │  PostgreSQL load + Sqoop import   │
   └─────────────────┬─────────────────┘
                     │  raw text files
                     ▼
                   HDFS  (/user/team32/linkedin/raw/)
                     │
   ┌─────────────────▼─────────────────┐
   │  STAGE 2                          │
   │  Hive raw → Parquet → enriched    │
   │  + EDA / analytics queries (Tez)  │
   └─────────────────┬─────────────────┘
                     │  team32_linkedin.linkedin_jobs_enriched
                     ▼
   ┌─────────────────────────────────────┐
   │  STAGE 3                            │
   │  Spark MLlib                        │
   │  prepare → train (RF/GBT/SVM/NB +   │
   │  ensemble) → sample predictions     │
   └─────────────────┬───────────────────┘
                     │  saved models on HDFS, metrics in output/
                     ▼
   ┌─────────────────────────────────────┐
   │  STAGE 4                            │
   │  Apache Superset dashboard          │
   │  + visualisations of EDA and ML     │
   └─────────────────────────────────────┘
```

## Tech stack

| Layer | Tool |
|---|---|
| Relational storage | PostgreSQL |
| Bulk transfer to HDFS | Apache Sqoop (Hadoop MapReduce engine) |
| Distributed file system | HDFS |
| Data warehouse | Apache Hive (Parquet + Snappy compression, Tez engine) |
| Distributed compute | Apache Spark (DataFrame, Spark SQL, Spark MLlib) |
| Cluster resource manager | Hadoop YARN |
| Dashboard | Apache Superset |
| Code quality | pylint |

All Python application code is **PySpark only** (Python 3.6 on the
cluster). All training runs on **Hadoop YARN**, not local machines.

## Repository structure

```
.
├── main.sh                      # full pipeline (do NOT modify)
├── README.md                    # this file
├── requirements.txt             # Python deps
├── docker-compose.yml           # local PostgreSQL for stage 1
├── .env.example                 # template for cluster credentials
├── data/                        # raw CSVs (gitignored)
├── models/                      # local model placeholder (see models/README.MD)
├── notebooks/                   # learning notebooks (not part of pipeline)
├── output/                      # pipeline artefacts (CSV/TXT/PNG)
├── scripts/
│   ├── stage1.sh                # PostgreSQL + Sqoop
│   ├── stage1_postgres_load.sh
│   ├── stage1_sqoop_import.sh
│   ├── load_postgres.py
│   ├── stage2.sh                # Hive warehouse + EDA
│   ├── stage2_hive_tables.sh
│   ├── stage3.sh                # Spark MLlib
│   ├── stage3_prepare_dataset.py
│   ├── stage3_train_models.py
│   ├── stage3_predict_samples.py
│   ├── stage4.sh                # Superset artefacts
│   ├── preprocess.sh
│   └── postprocess.sh
├── sql/
│   ├── postgres/                # 3 .sql files (schema, load, QA)
│   └── hive/                    # 5 .hql files (raw, parquet, enriched, validation, analytics)
└── docs/
    ├── stage3_ml_report.md      # full ML report
    └── eda_dashboard.md         # EDA & Superset planning
```

## How to run

The whole pipeline is launched by a single entry point:

```bash
bash main.sh
```

`main.sh` executes preprocess → stage 1 → stage 2 → stage 3 → stage 4
→ postprocess, then runs `pylint scripts` at the end. Per project
rules **this file may not be modified**.

Per-stage execution (run independently if needed):

```bash
bash scripts/stage1.sh    # PostgreSQL load + Sqoop into HDFS
bash scripts/stage2.sh    # Hive raw → Parquet → enriched + analytics
bash scripts/stage3.sh    # Spark MLlib: prepare → train → predict
bash scripts/stage4.sh    # Superset dashboard artefacts
```

### Configuration

Copy `.env.example` to `.env` and fill in cluster credentials before
running anything that touches PostgreSQL or Hive:

```bash
cp .env.example .env
# edit .env: HIVE_PASSWORD, DB_PASSWORD, etc.
```

Required variables: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`,
`DB_PASSWORD`, `HDFS_BASE`, `HIVE_DB`, `BEELINE_URL`, `HIVE_USER`,
`HIVE_PASSWORD`. Spark configuration (`SPARK_MASTER`,
`SPARK_DEPLOY_MODE`) is optional and defaults to YARN client mode.

### Reproducibility / idempotency

All scripts can be safely re-run: HDFS paths are written with
`mode("overwrite")` and HQL tables are `DROP TABLE IF EXISTS` first.
Hardcoded random seeds (`seed=42`) make CV folds and tree-ensemble
results reproducible up to Spark's distributed sum non-associativity.

## Stage outputs

| Stage | HDFS | Local `output/` |
|---|---|---|
| 1 | `/user/team32/linkedin/raw/{linkedin_job_postings,job_skills,job_summary}/` | `stage1_postgres_quality_checks.txt` |
| 2 | `/user/team32/linkedin/parquet/*`, `/enriched/linkedin_jobs_enriched/`, `/analytics/*` | `stage2_hive_counts.tsv` |
| 3 | `/user/team32/linkedin/ml/dataset_{train,test}/`, `/models/{rf,gbt,svm,nb}/` | `stage3_*` (metrics, CV grids, confusion, feature importance, sample predictions) |
| 4 | — | Superset dashboard export + screenshots |

`output/README.MD` contains the per-file index. `models/README.MD`
explains where the trained ML models actually live.

## Headline results

Stage 3 — binary classification on the held-out test split. The
ensemble is a soft vote over RF + GBT + SVM (NB excluded).

| Model | Accuracy | F1 | AUC ROC | AUC PR |
|---|---|---|---|---|
| **Gradient Boosted Trees** ⭐ | **0.906** | **0.902** | **0.941** | **0.703** |
| Soft-voting ensemble | 0.891 | 0.875 | 0.931 | — |
| Random Forest | 0.868 | 0.806 | 0.908 | — |
| Linear SVM | 0.856 | 0.868 | 0.898 | — |
| Gaussian Naive Bayes | 0.610 | 0.657 | 0.807 | — |

GBT is the recommended production model and is what the Superset
dashboard surfaces for interactive prediction. See
[`docs/stage3_ml_report.md`](docs/stage3_ml_report.md) for the
full analysis (feature engineering, hyperparameter tuning, confusion
matrices, ensemble construction, model collapse analysis for RF/NB).

## Documentation

* [`docs/stage3_ml_report.md`](docs/stage3_ml_report.md) — Stage 3 ML
  report (dataset construction, feature engineering, models, tuning,
  evaluation, model-by-model analysis, recommendations).
* [`docs/eda_dashboard.md`](docs/eda_dashboard.md) — Stage 2 EDA
  insights + Stage 4 Superset dashboard design.
* [`output/README.MD`](output/README.MD) — index of every artefact in
  `output/`.
* [`models/README.MD`](models/README.MD) — note on where Spark ML
  PipelineModels actually live (HDFS).

## Important notes

* `main.sh` is the grading entry point and **must not be modified**.
* `notebooks/` is for exploratory work only — the grader may delete it,
  so nothing in the pipeline imports from it.
* The dataset itself is not in this repository — see `.gitignore`. It
  lives on the cluster at `/user/team32/linkedin/raw/`.
