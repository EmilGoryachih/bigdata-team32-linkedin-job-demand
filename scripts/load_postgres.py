import os
import csv
import psycopg2
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DATA_DIR = Path(os.environ.get("DATA_DIR", PROJECT_ROOT / "data" / "raw"))

OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def run_sql_file(conn, path: Path):
    with path.open("r", encoding="utf-8") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()


def copy_csv(conn, table_name: str, columns: list[str], file_path: Path):
    columns_sql = ", ".join(columns)

    with file_path.open("r", encoding="utf-8", newline="") as f:
        with conn.cursor() as cur:
            cur.copy_expert(
                f"""
                COPY {table_name} ({columns_sql})
                FROM STDIN
                WITH (
                    FORMAT csv,
                    HEADER true,
                    QUOTE '"',
                    ESCAPE '"'
                )
                """,
                f,
            )

    conn.commit()


def run_quality_checks(conn):
    checks_path = PROJECT_ROOT / "sql" / "postgres" / "03_quality_checks.sql"
    output_path = OUTPUT_DIR / "stage1_postgres_quality_checks.txt"

    with checks_path.open("r", encoding="utf-8") as f:
        sql = f.read()

    # remove psql-only \echo commands, because psycopg2 does not understand them
    lines = []
    for line in sql.splitlines():
        if not line.strip().startswith("\\echo"):
            lines.append(line)
    sql = "\n".join(lines)

    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with output_path.open("w", encoding="utf-8") as out:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)

                if cur.description is None:
                    continue

                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                out.write("\t".join(columns) + "\n")
                for row in rows:
                    out.write("\t".join("" if value is None else str(value) for value in row) + "\n")
                out.write("\n")

    print(f"Quality checks written to {output_path}")


def main():
    required_files = [
        DATA_DIR / "linkedin_job_postings.csv",
        DATA_DIR / "job_skills.csv",
        DATA_DIR / "job_summary.csv",
    ]

    for file_path in required_files:
        if not file_path.exists():
            raise FileNotFoundError(f"Missing dataset file: {file_path}")

    conn = connect()

    try:
        print("Creating PostgreSQL tables...")
        run_sql_file(conn, PROJECT_ROOT / "sql" / "postgres" / "01_create_tables.sql")

        print("Loading linkedin_job_postings.csv...")
        copy_csv(
            conn,
            "linkedin_job_postings",
            [
                "job_link",
                "last_processed_time",
                "got_summary",
                "got_ner",
                "is_being_worked",
                "job_title",
                "company",
                "job_location",
                "first_seen",
                "search_city",
                "search_country",
                "search_position",
                "job_level",
                "job_type",
            ],
            DATA_DIR / "linkedin_job_postings.csv",
        )

        print("Loading job_skills.csv...")
        copy_csv(
            conn,
            "job_skills",
            ["job_link", "job_skills"],
            DATA_DIR / "job_skills.csv",
        )

        print("Loading job_summary.csv...")
        copy_csv(
            conn,
            "job_summary",
            ["job_link", "job_summary"],
            DATA_DIR / "job_summary.csv",
        )

        print("Running quality checks...")
        run_quality_checks(conn)

        print("PostgreSQL load completed.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()