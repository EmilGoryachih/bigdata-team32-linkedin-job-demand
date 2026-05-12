This repository is a template for the final project of big data course. It contains the following directories:

- `data/` contains the dataset files.
- `models/` contains the Spark ML models.
- `notebooks/` has the Jupyter or Zeppelin notebooks of your project and used just for learning purposes.
- `output/` represents the output directory for storing the results of the project. It can contain `csv` files, text files. images and any other materials you returned as an ouput of the pipeline.
- `scripts/` is a place for storing `.sh` scripts and `.py` scripts of the pipeline.
- `sql/` is a folder for keeping all `.sql` and `.hql` files.

`requirements.txt` lists the Python packages needed for running your Python scripts. Feel free to add more packages when necessary.

`main.sh` is the main script that will run all scripts of the pipeline stages which will execute the full pipeline and store the results in `output/` folder. During checking your project repo, the grader will run only the main script and check the results in `output/` folder.

**Important Note:** You cannot change the content of the script `main.sh` since it will be used for assessment purposes.

**Another Note:** The notebooks in `notebooks/` folder are used only for learning purposes since you need to put all Python scripts of the pipeline in `scripts/` folder. During the assessment, the grader can delete the folder `notebooks/` to check that your pipeline does not depend on its content.

## LinkedIn Job Demand Analytics

This project analyzes LinkedIn job postings with PostgreSQL, Sqoop, HDFS, Hive,
Spark, and Apache Superset. The enriched analytics source table is:

```text
team32_linkedin.linkedin_jobs_enriched
```

HDFS source path:

```text
/user/team32/linkedin/enriched/linkedin_jobs_enriched
```

The EDA and dashboard layer is documented in:

```text
docs/eda_dashboard.md
```

Stage 2 creates Hive dashboard tables under `/user/team32/linkedin/analytics`
and Spark SQL analytics under `/user/team32/linkedin/analytics_spark`.
Stage 4 generates Superset dashboard planning artifacts in `output/`.

Run on the Hadoop/YARN cluster:

```bash
bash scripts/stage2.sh
bash scripts/stage4.sh
```
