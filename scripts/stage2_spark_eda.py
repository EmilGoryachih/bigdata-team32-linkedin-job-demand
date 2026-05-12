"""Spark SQL EDA for the LinkedIn job demand project.

The script reads the enriched Parquet dataset, normalizes key categorical
fields, explodes comma-separated skills, writes dashboard-ready Spark analytics
tables to HDFS, and exports small CSV previews to the local output directory.
It is intended to run on the Hadoop cluster with spark-submit on YARN.
"""

import argparse
import csv
from pathlib import Path
from typing import Iterable, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql import functions as F


DEFAULT_ENRICHED_PATH = "/user/team32/linkedin/enriched/linkedin_jobs_enriched"
DEFAULT_SPARK_ANALYTICS_PATH = "/user/team32/linkedin/analytics_spark"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enriched-path", default=DEFAULT_ENRICHED_PATH)
    parser.add_argument("--spark-analytics-path", default=DEFAULT_SPARK_ANALYTICS_PATH)
    parser.add_argument("--output-dir", default="output")
    return parser.parse_args()


def build_spark() -> SparkSession:
    """Create the Spark session."""
    return (
        SparkSession.builder
        .appName("team32-stage2-spark-eda")
        .enableHiveSupport()
        .getOrCreate()
    )


def normalized_text(col_name: str) -> Column:
    """Normalize a string column and replace missing values with unknown."""
    cleaned = F.lower(F.trim(F.regexp_replace(F.col(col_name), r"\s+", " ")))
    return F.when(
        F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
        "unknown",
    ).otherwise(cleaned)


def clean_jobs(df_raw: DataFrame) -> DataFrame:
    """Return the cleaned analytics base frame."""
    return df_raw.select(
        "job_link",
        "first_seen",
        "last_processed_time",
        normalized_text("job_title").alias("job_title"),
        normalized_text("company").alias("company"),
        normalized_text("job_location").alias("job_location"),
        normalized_text("search_city").alias("search_city"),
        normalized_text("search_country").alias("search_country"),
        normalized_text("search_position").alias("search_position"),
        normalized_text("job_level").alias("job_level"),
        normalized_text("job_type").alias("job_type"),
        F.when(
            F.col("job_skills").isNull() | (F.trim(F.col("job_skills")) == ""),
            None,
        ).otherwise(
            F.lower(F.trim(F.regexp_replace(F.col("job_skills"), r"\s+", " ")))
        ).alias("job_skills"),
        F.when(
            F.col("job_summary").isNull() | (F.trim(F.col("job_summary")) == ""),
            None,
        ).otherwise(F.trim(F.regexp_replace(F.col("job_summary"), r"\s+", " "))).alias(
            "job_summary"
        ),
    )


def explode_skills(clean_df: DataFrame) -> DataFrame:
    """Explode comma-separated skills into one row per skill mention."""
    return (
        clean_df
        .withColumn(
            "skill_raw",
            F.explode(F.split(F.coalesce(F.col("job_skills"), F.lit("")), ",")),
        )
        .withColumn("skill", F.trim(F.col("skill_raw")))
        .filter(F.col("skill") != "")
        .select(
            "job_link",
            "first_seen",
            "search_country",
            "search_position",
            "job_level",
            "job_type",
            "company",
            "skill",
        )
    )


def write_table(df_out: DataFrame, base_path: str, table_name: str) -> None:
    """Write one analytics table to HDFS as Parquet."""
    output_path = f"{base_path.rstrip('/')}/{table_name}"
    print(f"[stage2.spark_eda] Writing {table_name} -> {output_path}", flush=True)
    df_out.write.mode("overwrite").parquet(output_path)


def write_preview_csv(
    df_out: DataFrame,
    output_dir: Path,
    file_name: str,
    limit: int = 50,
) -> None:
    """Write a small local CSV preview for quick checking."""
    rows = df_out.limit(limit).collect()
    output_path = output_dir / file_name
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(df_out.columns)
        for row in rows:
            writer.writerow([row[col] for col in df_out.columns])


def build_sql_outputs(spark: SparkSession) -> List[Tuple[str, DataFrame]]:
    """Run Spark SQL aggregations over the cleaned temp views."""
    queries = [
        (
            "spark_top_positions_global",
            """
            SELECT
                search_position,
                COUNT(*) AS posting_count,
                COUNT(DISTINCT company) AS company_count,
                COUNT(DISTINCT search_country) AS country_count
            FROM spark_clean_jobs
            WHERE search_position <> 'unknown'
            GROUP BY search_position
            ORDER BY posting_count DESC
            """,
        ),
        (
            "spark_top_skills_global",
            """
            SELECT
                skill,
                COUNT(*) AS skill_mentions,
                COUNT(DISTINCT job_link) AS posting_count,
                COUNT(DISTINCT search_country) AS country_count
            FROM spark_job_skills_exploded
            GROUP BY skill
            ORDER BY skill_mentions DESC
            """,
        ),
        (
            "spark_demand_by_country",
            """
            SELECT
                search_country,
                COUNT(*) AS posting_count,
                COUNT(DISTINCT search_position) AS position_count,
                COUNT(DISTINCT company) AS company_count
            FROM spark_clean_jobs
            WHERE search_country <> 'unknown'
            GROUP BY search_country
            ORDER BY posting_count DESC
            """,
        ),
        (
            "spark_job_type_distribution",
            """
            SELECT
                job_type,
                COUNT(*) AS posting_count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS posting_pct
            FROM spark_clean_jobs
            GROUP BY job_type
            ORDER BY posting_count DESC
            """,
        ),
        (
            "spark_skill_country_matrix",
            """
            SELECT
                search_country,
                skill,
                COUNT(*) AS skill_mentions,
                COUNT(DISTINCT job_link) AS posting_count
            FROM spark_job_skills_exploded
            WHERE search_country <> 'unknown'
            GROUP BY search_country, skill
            """,
        ),
    ]
    return [(name, spark.sql(query)) for name, query in queries]


def build_dataframe_outputs(
    clean_df: DataFrame,
    skills_df: DataFrame,
) -> List[Tuple[str, DataFrame]]:
    """Build supplemental Spark DataFrame aggregations."""
    characteristics = clean_df.agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("search_country").alias("distinct_countries"),
        F.countDistinct("search_position").alias("distinct_positions"),
        F.countDistinct("company").alias("distinct_companies"),
        F.sum(F.when(F.col("job_skills").isNotNull(), 1).otherwise(0)).alias(
            "rows_with_skills"
        ),
        F.sum(F.when(F.col("job_summary").isNotNull(), 1).otherwise(0)).alias(
            "rows_with_summary"
        ),
        F.min("first_seen").alias("min_first_seen"),
        F.max("first_seen").alias("max_first_seen"),
    )

    top_skills_by_level = (
        skills_df
        .filter(F.col("job_level") != "unknown")
        .groupBy("job_level", "skill")
        .agg(
            F.count("*").alias("skill_mentions"),
            F.countDistinct("job_link").alias("posting_count"),
        )
    )

    return [
        ("spark_data_characteristics", characteristics),
        ("spark_skills_by_job_level", top_skills_by_level),
    ]


def write_outputs(
    outputs: Iterable[Tuple[str, DataFrame]],
    analytics_path: str,
    output_dir: Path,
) -> None:
    """Persist all Spark analytics outputs and local previews."""
    for table_name, df_out in outputs:
        write_table(df_out, analytics_path, table_name)
        write_preview_csv(df_out, output_dir, f"{table_name}.csv")


def main() -> None:
    """Entry point for spark-submit."""
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(
        f"[stage2.spark_eda] Reading enriched parquet: {args.enriched_path}",
        flush=True,
    )
    raw_df = spark.read.parquet(args.enriched_path)
    clean_df = clean_jobs(raw_df).cache()
    skills_df = explode_skills(clean_df).cache()

    clean_df.createOrReplaceTempView("spark_clean_jobs")
    skills_df.createOrReplaceTempView("spark_job_skills_exploded")

    clean_count = clean_df.count()
    skills_count = skills_df.count()
    print(
        f"[stage2.spark_eda] Clean rows: {clean_count}; exploded skill rows: {skills_count}",
        flush=True,
    )

    outputs = build_sql_outputs(spark) + build_dataframe_outputs(clean_df, skills_df)
    write_outputs(outputs, args.spark_analytics_path, output_dir)

    spark.stop()


if __name__ == "__main__":
    main()
