"""
ingest_postgres.py

Sprint 2 – T5 : Dual DB Ingestion
Topic M6 : Synthetic Thermal Time-Series
Team SG03

Purpose
-------
Load all files produced by T4 (etl_pipeline.py) into a PostgreSQL database.

Tables filled
    subjects     ← etl_summary.json  (norm params, per-patient stats)
    recordings   ← derived from cleaned_csv per-segment grouping
    signals      ← cleaned_csv/patient_XX_cleaned.csv  (5 184 000 rows)
    windows      ← all_windows_meta.csv  (172 780 rows)

Strategy
--------
  • Uses psycopg2 COPY FROM STDIN for maximum throughput (~200 k rows/s).
  • Creates schema if it does not exist (runs schema_postgres.sql).
  • Indexes are created AFTER the bulk load to avoid B-tree rebuild overhead.
  • Measures and reports ingestion rate (rows/s) and disk size before/after indexing.

Usage
-----
  python ingest_postgres.py [--etl-dir PATH] [--dsn DSN] [--patients N]

  Credentials are loaded from .env automatically (no need to pass --dsn).

Requirements
------------
  pip install psycopg2-binary pandas tqdm python-dotenv
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
load_dotenv()

def _build_pg_dsn() -> str:
    host     = os.getenv("PG_HOST",     "localhost")
    port     = os.getenv("PG_PORT",     "5432")
    user     = os.getenv("PG_USER",     "postgres")
    password = os.getenv("PG_PASSWORD", "postgres")
    db       = os.getenv("PG_DB",       "m6_thermal")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

def _default_etl_dir() -> str:
    return os.getenv("ETL_DIR", "etl_output")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingest_pg")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_conn(dsn: str):
    """Return a psycopg2 connection with autocommit OFF."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def run_sql_file(conn, path: Path):
    """Execute a .sql file against conn (used to apply schema)."""
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info(f"Schema applied from {path.name}")


def table_row_count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        return cur.fetchone()[0]


def table_size_pretty(conn, table: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_size_pretty(pg_total_relation_size(%s));", (table,)
        )
        return cur.fetchone()[0]


def copy_df_to_table(conn, df: pd.DataFrame, table: str, columns: list[str]) -> tuple[int, float]:
    """
    Use COPY FROM STDIN (CSV) for maximum throughput.
    Returns (rows_copied, elapsed_seconds).
    """
    buf = io.StringIO()
    df[columns].to_csv(buf, index=False, header=False)
    buf.seek(0)

    col_str = ", ".join(columns)
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} ({col_str}) FROM STDIN WITH (FORMAT CSV, NULL '')",
            buf,
        )
    conn.commit()
    elapsed = time.perf_counter() - t0
    rows = len(df)
    return rows, elapsed


# ---------------------------------------------------------------------------
# Stage A : subjects (from etl_summary.json)
# ---------------------------------------------------------------------------

def ingest_subjects(conn, etl_dir: Path, max_patients: int):
    summary_path = etl_dir / "logs" / "etl_summary.json"
    if not summary_path.exists():
        log.error(f"etl_summary.json not found at {summary_path}")
        sys.exit(1)

    with open(summary_path) as f:
        summary = json.load(f)

    patients = summary["patients"][:max_patients]

    rows = []
    for p in patients:
        norm = p["norm_params"]
        rows.append((
            p["patient_id"],
            norm["norm_mean_left"],
            norm["norm_std_left"],
            norm["norm_mean_right"],
            norm["norm_std_right"],
            p["rows_input"],
            p["rows_output"],
            p["windows"],
            p["anomaly_windows"],
            p["segments"],
            p["interpolated_rows"],
        ))

    sql = """
        INSERT INTO subjects (
            patient_id,
            norm_mean_left, norm_std_left,
            norm_mean_right, norm_std_right,
            rows_input, rows_output,
            n_windows, n_anomaly_windows,
            n_segments, n_interpolated_rows
        ) VALUES %s
        ON CONFLICT (patient_id) DO NOTHING;
    """
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    elapsed = time.perf_counter() - t0

    log.info(f"subjects: {len(rows)} rows in {elapsed:.2f}s")
    return len(rows)


# ---------------------------------------------------------------------------
# Stage B : signals (from cleaned_csv/patient_XX_cleaned.csv)
# ---------------------------------------------------------------------------

SIGNALS_COLUMNS = [
    "timestamp", "patient_id",
    "left_temperature", "right_temperature",
    "left_temperature_norm", "right_temperature_norm",
    "temp_asymmetry",
    "anomaly_label", "anomaly_type",
    "is_interpolated", "segment_id", "session_second",
]


def ingest_signals(conn, etl_dir: Path, max_patients: int) -> dict:
    cleaned_dir = etl_dir / "cleaned_csv"
    csv_files = sorted(cleaned_dir.glob("patient_*_cleaned.csv"))[:max_patients]

    if not csv_files:
        log.error(f"No cleaned CSV files found in {cleaned_dir}")
        sys.exit(1)

    total_rows = 0
    total_time = 0.0
    results = []

    for csv_path in tqdm(csv_files, desc="signals", unit="patient"):
        pid = int(csv_path.stem.split("_")[1])
        df = pd.read_csv(csv_path, parse_dates=["timestamp"])
        df["is_interpolated"] = df["is_interpolated"].astype(bool)

        rows, elapsed = copy_df_to_table(conn, df, "signals", SIGNALS_COLUMNS)
        rate = rows / elapsed if elapsed > 0 else float("inf")
        log.info(f"  patient_{pid:02d}: {rows:,} rows in {elapsed:.2f}s → {rate:,.0f} rows/s")

        total_rows += rows
        total_time += elapsed
        results.append({"patient_id": pid, "rows": rows, "elapsed_s": round(elapsed, 3),
                         "rows_per_s": round(rate, 0)})

    overall_rate = total_rows / total_time if total_time > 0 else 0
    log.info(f"signals TOTAL: {total_rows:,} rows in {total_time:.1f}s → "
             f"{overall_rate:,.0f} rows/s")
    return {
        "table": "signals",
        "total_rows": total_rows,
        "total_elapsed_s": round(total_time, 2),
        "overall_rows_per_s": round(overall_rate, 0),
        "per_patient": results,
    }


# ---------------------------------------------------------------------------
# Stage C : recordings (derived from signals groupby segment)
# ---------------------------------------------------------------------------

def ingest_recordings(conn):
    sql = """
        INSERT INTO recordings (patient_id, segment_id, segment_start, segment_end, n_rows)
        SELECT
            patient_id,
            segment_id,
            MIN(timestamp) AS segment_start,
            MAX(timestamp) AS segment_end,
            COUNT(*)       AS n_rows
        FROM signals
        GROUP BY patient_id, segment_id
        ON CONFLICT (patient_id, segment_id) DO NOTHING;
    """
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    elapsed = time.perf_counter() - t0

    count = table_row_count(conn, "recordings")
    log.info(f"recordings: {count} rows derived from signals in {elapsed:.2f}s")
    return count


# ---------------------------------------------------------------------------
# Stage D : windows (from all_windows_meta.csv)
# ---------------------------------------------------------------------------

WINDOWS_COLUMNS = [
    "window_id", "patient_id", "segment_id", "window_index",
    "window_start", "window_end",
    "label", "anomaly_ratio", "is_interpolated",
]


def ingest_windows(conn, etl_dir: Path) -> dict:
    meta_path = etl_dir / "all_windows_meta.csv"
    if not meta_path.exists():
        log.error(f"all_windows_meta.csv not found at {meta_path}")
        sys.exit(1)

    df = pd.read_csv(meta_path, parse_dates=["window_start", "window_end"])
    df["is_interpolated"] = df["is_interpolated"].astype(bool)

    rows, elapsed = copy_df_to_table(conn, df, "windows", WINDOWS_COLUMNS)
    rate = rows / elapsed if elapsed > 0 else float("inf")
    log.info(f"windows: {rows:,} rows in {elapsed:.2f}s → {rate:,.0f} rows/s")

    return {
        "table": "windows",
        "total_rows": rows,
        "elapsed_s": round(elapsed, 2),
        "rows_per_s": round(rate, 0),
    }


# ---------------------------------------------------------------------------
# Stage E : Post-load indexes + size measurement
# ---------------------------------------------------------------------------

def create_indexes_and_measure(conn) -> dict:
    size_before = {}
    for t in ("signals", "windows", "subjects", "recordings"):
        size_before[t] = table_size_pretty(conn, t)

    log.info("Creating indexes on signals...")
    index_sql = [
        "CREATE INDEX IF NOT EXISTS idx_signals_patient_ts ON signals (patient_id, timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals (timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_signals_anomaly ON signals (anomaly_label) WHERE anomaly_label = 1;",
        "CREATE INDEX IF NOT EXISTS idx_windows_patient ON windows (patient_id);",
        "CREATE INDEX IF NOT EXISTS idx_windows_label ON windows (label) WHERE label = 1;",
    ]

    t0 = time.perf_counter()
    with conn.cursor() as cur:
        for stmt in index_sql:
            cur.execute(stmt)
    conn.commit()
    idx_time = time.perf_counter() - t0
    log.info(f"Indexes created in {idx_time:.1f}s")

    size_after = {}
    for t in ("signals", "windows", "subjects", "recordings"):
        size_after[t] = table_size_pretty(conn, t)

    return {
        "index_build_s": round(idx_time, 2),
        "size_before_index": size_before,
        "size_after_index": size_after,
    }


# ---------------------------------------------------------------------------
# Stage F : Spot-check row counts
# ---------------------------------------------------------------------------

def spot_check(conn) -> dict:
    result = {}
    for table in ("subjects", "recordings", "signals", "windows"):
        n = table_row_count(conn, table)
        result[table] = n
        log.info(f"  {table:12s} : {n:,} rows")

    assert result["signals"]  >= 4_000_000, f"signals too low: {result['signals']:,}"
    assert result["windows"]  == 172_780,   f"windows expected 172 780, got {result['windows']:,}"
    log.info("Spot-check PASSED")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="T5 – PostgreSQL ingestion")
    parser.add_argument(
        "--etl-dir", default=_default_etl_dir(),
        help="Root output directory of etl_pipeline.py"
    )
    parser.add_argument(
        "--dsn", default=_build_pg_dsn(),
        help="PostgreSQL DSN (default: built from .env)"
    )
    parser.add_argument(
        "--patients", type=int, default=20,
        help="Max number of patients to ingest (default: 20)"
    )
    parser.add_argument(
        "--schema-file",
        default=str(Path(__file__).parent / "sql" / "schema_postgres.sql"),
        help="Path to schema_postgres.sql"
    )
    parser.add_argument(
        "--skip-schema", action="store_true",
        help="Skip schema creation (tables already exist)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    etl_dir = Path(args.etl_dir)
    schema_file = Path(args.schema_file)

    log.info("=" * 60)
    log.info("T5 – PostgreSQL Ingestion")
    log.info(f"  ETL dir  : {etl_dir}")
    log.info(f"  DSN      : {args.dsn}")
    log.info(f"  Patients : {args.patients}")
    log.info("=" * 60)

    conn = get_conn(args.dsn)

    # 0. Schema
    if not args.skip_schema:
        if not schema_file.exists():
            log.error(f"Schema file not found: {schema_file}")
            sys.exit(1)
        run_sql_file(conn, schema_file)

    pipeline_start = time.perf_counter()

    # A. subjects
    log.info("── Stage A : subjects ──────────────────────────")
    n_subjects = ingest_subjects(conn, etl_dir, args.patients)

    # B. signals
    log.info("── Stage B : signals ───────────────────────────")
    signals_metrics = ingest_signals(conn, etl_dir, args.patients)

    # C. recordings
    log.info("── Stage C : recordings (derived) ──────────────")
    n_recordings = ingest_recordings(conn)

    # D. windows
    log.info("── Stage D : windows ───────────────────────────")
    windows_metrics = ingest_windows(conn, etl_dir)

    # E. indexes + size
    log.info("── Stage E : indexes & size measurement ─────────")
    size_metrics = create_indexes_and_measure(conn)

    # F. spot-check
    log.info("── Stage F : spot-check ────────────────────────")
    counts = spot_check(conn)

    total_elapsed = time.perf_counter() - pipeline_start
    report = {
        "database":        "postgresql",
        "dsn":             args.dsn,
        "total_elapsed_s": round(total_elapsed, 2),
        "n_subjects":      n_subjects,
        "n_recordings":    n_recordings,
        "signals":         signals_metrics,
        "windows":         windows_metrics,
        "size":            size_metrics,
        "row_counts":      counts,
    }

    report_path = etl_dir / "logs" / "ingest_postgres_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    log.info("=" * 60)
    log.info(f"PostgreSQL ingestion COMPLETE in {total_elapsed:.1f}s")
    log.info(f"Report saved → {report_path}")
    log.info("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()