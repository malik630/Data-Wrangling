# -*- coding: utf-8 -*-
"""
benchmark_runner.py

Sprint 2 - T6 : Benchmark Report
Topic M6 : Synthetic Thermal Time-Series
Team SG03

Runs 8 official benchmark queries x 5 times on both PostgreSQL and TimescaleDB.
Records median + p95 latency and exports results to CSV and JSON.

Usage
-----
    python benchmark_runner.py

Credentials are loaded from .env automatically.

Output
------
    data/processed/logs/benchmark_raw.csv       <- all individual run timings
    data/processed/logs/benchmark_summary.csv   <- median + p95 per query per DB
    data/processed/logs/benchmark_summary.json  <- same as summary CSV in JSON
"""

import os
import csv
import json
import time
import statistics
import logging
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
load_dotenv()

def _build_pg_dsn() -> str:
    return (
        f"postgresql://{os.getenv('PG_USER','postgres')}:"
        f"{os.getenv('PG_PASSWORD','postgres')}@"
        f"{os.getenv('PG_HOST','localhost')}:"
        f"{os.getenv('PG_PORT','5432')}/"
        f"{os.getenv('PG_DB','m6_thermal')}"
    )

def _build_tsdb_dsn() -> str:
    return (
        f"postgresql://{os.getenv('TSDB_USER','postgres')}:"
        f"{os.getenv('TSDB_PASSWORD','postgres')}@"
        f"{os.getenv('TSDB_HOST','localhost')}:"
        f"{os.getenv('TSDB_PORT','5433')}/"
        f"{os.getenv('TSDB_DB','m6_thermal_tsdb')}"
    )

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("benchmark")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
N_RUNS   = 5
OUT_DIR  = Path(os.getenv("ETL_DIR", "data/processed")) / "logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
PATIENT_ID = 3

# ---------------------------------------------------------------------------
# Query strings
# ---------------------------------------------------------------------------

BM_R1_PG = """
SELECT timestamp, left_temperature, right_temperature,
       temp_asymmetry, anomaly_label
FROM signals
WHERE patient_id = {patient_id}
  AND timestamp >= '{min_ts}'
  AND timestamp <  '{min_ts}'::timestamptz + INTERVAL '1 hour'
ORDER BY timestamp;
"""

BM_R1_TSDB = """
SELECT timestamp, left_temperature, right_temperature,
       temp_asymmetry, anomaly_label
FROM thermal_readings
WHERE patient_id = {patient_id}
  AND timestamp >= '{min_ts}'
  AND timestamp <  '{min_ts}'::timestamptz + INTERVAL '1 hour'
ORDER BY timestamp;
"""

BM_R2_PG = """
SELECT timestamp, left_temperature, right_temperature,
       temp_asymmetry, anomaly_label, anomaly_type
FROM signals
WHERE patient_id = {patient_id}
ORDER BY timestamp;
"""

BM_R2_TSDB = """
SELECT timestamp, left_temperature, right_temperature,
       temp_asymmetry, anomaly_label, anomaly_type
FROM thermal_readings
WHERE patient_id = {patient_id}
ORDER BY timestamp;
"""

BM_A1_PG = """
SELECT DATE_TRUNC('minute', timestamp) AS bucket,
       AVG(left_temperature)  AS avg_left,
       AVG(right_temperature) AS avg_right,
       AVG(temp_asymmetry)    AS avg_asymmetry,
       COUNT(*)               AS n_samples
FROM signals
WHERE patient_id = {patient_id}
GROUP BY bucket ORDER BY bucket;
"""

BM_A1_TSDB = """
SELECT time_bucket('1 minute', timestamp) AS bucket,
       AVG(left_temperature)  AS avg_left,
       AVG(right_temperature) AS avg_right,
       AVG(temp_asymmetry)    AS avg_asymmetry,
       COUNT(*)               AS n_samples
FROM thermal_readings
WHERE patient_id = {patient_id}
GROUP BY bucket ORDER BY bucket;
"""

BM_A2_PG = """
SELECT DATE_TRUNC('hour', timestamp) AS hour_bucket, patient_id,
       COUNT(*) AS total_samples, SUM(anomaly_label) AS anomaly_count,
       ROUND(100.0 * SUM(anomaly_label) / COUNT(*), 4) AS anomaly_rate_pct
FROM signals
GROUP BY hour_bucket, patient_id
ORDER BY hour_bucket, patient_id;
"""

BM_A2_TSDB = """
SELECT time_bucket('1 hour', timestamp) AS hour_bucket, patient_id,
       COUNT(*) AS total_samples, SUM(anomaly_label) AS anomaly_count,
       ROUND(100.0 * SUM(anomaly_label) / COUNT(*), 4) AS anomaly_rate_pct
FROM thermal_readings
GROUP BY hour_bucket, patient_id
ORDER BY hour_bucket, patient_id;
"""

BM_S1_PG = """
SELECT pg_size_pretty(pg_total_relation_size('signals')) AS total_size,
       pg_size_pretty(pg_relation_size('signals'))        AS table_size,
       pg_size_pretty(pg_total_relation_size('signals')
           - pg_relation_size('signals'))                 AS index_size,
       pg_total_relation_size('signals')                  AS total_bytes;
"""

BM_S1_TSDB = """
SELECT pg_size_pretty(hypertable_size('thermal_readings')) AS total_size,
       hypertable_size('thermal_readings')                  AS total_bytes;
"""

BM_S2_PG = """
SELECT pg_size_pretty(pg_total_relation_size('signals')) AS total_size,
       1.0 AS compression_ratio, 0.0 AS pct_saved;
"""

BM_S2_TSDB = """
SELECT pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes)  AS after,
       ROUND(before_compression_total_bytes::numeric
           / NULLIF(after_compression_total_bytes, 0), 2) AS compression_ratio,
       ROUND(100.0 * (1 - after_compression_total_bytes::numeric
           / NULLIF(before_compression_total_bytes, 0)), 1) AS pct_saved
FROM hypertable_compression_stats('thermal_readings');
"""

# Write query templates
SETUP_W1_PG = """
CREATE TEMP TABLE IF NOT EXISTS _bm_w1_pg AS
SELECT timestamp + INTERVAL '72 hours' AS timestamp,
       patient_id, left_temperature, right_temperature,
       left_temperature_norm, right_temperature_norm,
       temp_asymmetry, anomaly_label, anomaly_type,
       is_interpolated, segment_id, session_second
FROM signals ORDER BY patient_id, timestamp LIMIT 10000;
"""
INSERT_W1_PG = """
INSERT INTO signals (timestamp, patient_id, left_temperature, right_temperature,
    left_temperature_norm, right_temperature_norm, temp_asymmetry,
    anomaly_label, anomaly_type, is_interpolated, segment_id, session_second)
SELECT * FROM _bm_w1_pg ON CONFLICT DO NOTHING;
"""
CLEANUP_W1_PG = """
DELETE FROM signals
WHERE timestamp >= (SELECT MIN(timestamp) FROM _bm_w1_pg)
  AND timestamp <= (SELECT MAX(timestamp) FROM _bm_w1_pg)
  AND patient_id IN (SELECT DISTINCT patient_id FROM _bm_w1_pg);
"""

SETUP_W1_TSDB = """
CREATE TEMP TABLE IF NOT EXISTS _bm_w1_tsdb AS
SELECT timestamp + INTERVAL '72 hours' AS timestamp,
       patient_id, left_temperature, right_temperature,
       left_temperature_norm, right_temperature_norm,
       temp_asymmetry, anomaly_label, anomaly_type,
       is_interpolated, segment_id, session_second
FROM thermal_readings ORDER BY patient_id, timestamp LIMIT 10000;
"""
INSERT_W1_TSDB = """
INSERT INTO thermal_readings (timestamp, patient_id, left_temperature, right_temperature,
    left_temperature_norm, right_temperature_norm, temp_asymmetry,
    anomaly_label, anomaly_type, is_interpolated, segment_id, session_second)
SELECT * FROM _bm_w1_tsdb;
"""
CLEANUP_W1_TSDB = """
DELETE FROM thermal_readings
WHERE timestamp >= (SELECT MIN(timestamp) FROM _bm_w1_tsdb)
  AND timestamp <= (SELECT MAX(timestamp) FROM _bm_w1_tsdb);
"""

SETUP_W2_PG = """
CREATE TEMP TABLE IF NOT EXISTS _bm_w2_pg AS
SELECT timestamp + INTERVAL '144 hours' AS timestamp,
       patient_id, left_temperature, right_temperature,
       left_temperature_norm, right_temperature_norm,
       temp_asymmetry, anomaly_label, anomaly_type,
       is_interpolated, segment_id, session_second
FROM signals ORDER BY patient_id, timestamp LIMIT 50000;
"""
INSERT_W2_PG = """
INSERT INTO signals (timestamp, patient_id, left_temperature, right_temperature,
    left_temperature_norm, right_temperature_norm, temp_asymmetry,
    anomaly_label, anomaly_type, is_interpolated, segment_id, session_second)
SELECT * FROM _bm_w2_pg ON CONFLICT DO NOTHING;
"""
CLEANUP_W2_PG = """
DELETE FROM signals
WHERE timestamp >= (SELECT MIN(timestamp) FROM _bm_w2_pg)
  AND timestamp <= (SELECT MAX(timestamp) FROM _bm_w2_pg)
  AND patient_id IN (SELECT DISTINCT patient_id FROM _bm_w2_pg);
"""

SETUP_W2_TSDB = """
CREATE TEMP TABLE IF NOT EXISTS _bm_w2_tsdb AS
SELECT timestamp + INTERVAL '144 hours' AS timestamp,
       patient_id, left_temperature, right_temperature,
       left_temperature_norm, right_temperature_norm,
       temp_asymmetry, anomaly_label, anomaly_type,
       is_interpolated, segment_id, session_second
FROM thermal_readings ORDER BY patient_id, timestamp LIMIT 50000;
"""
INSERT_W2_TSDB = """
INSERT INTO thermal_readings (timestamp, patient_id, left_temperature, right_temperature,
    left_temperature_norm, right_temperature_norm, temp_asymmetry,
    anomaly_label, anomaly_type, is_interpolated, segment_id, session_second)
SELECT * FROM _bm_w2_tsdb;
"""
CLEANUP_W2_TSDB = """
DELETE FROM thermal_readings
WHERE timestamp >= (SELECT MIN(timestamp) FROM _bm_w2_tsdb)
  AND timestamp <= (SELECT MAX(timestamp) FROM _bm_w2_tsdb);
"""

# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def time_query(conn, sql: str) -> float:
    """Run a read query and return elapsed time in ms."""
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(sql)
        cur.fetchall()
        elapsed_ms = (time.perf_counter() - t0) * 1000
    conn.rollback()
    return elapsed_ms


def time_write_query(conn, setup_sql, insert_sql, cleanup_sql) -> float:
    """Setup once, time the INSERT only, then cleanup. Returns ms."""
    with conn.cursor() as cur:
        cur.execute(setup_sql)
    conn.commit()

    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(insert_sql)
        conn.commit()
        elapsed_ms = (time.perf_counter() - t0) * 1000

    with conn.cursor() as cur:
        cur.execute(cleanup_sql)
    conn.commit()

    return elapsed_ms


def percentile(data: list, p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def summarize(query_id, category, db, timings):
    return {
        "query_id":  query_id,
        "category":  category,
        "db":        db,
        "median_ms": round(statistics.median(timings), 2),
        "p95_ms":    round(percentile(timings, 95), 2),
        "mean_ms":   round(statistics.mean(timings), 2),
        "min_ms":    round(min(timings), 2),
        "max_ms":    round(max(timings), 2),
    }


def get_min_ts(conn, table, patient_id):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN(timestamp) FROM {table} WHERE patient_id = %s",
                    (patient_id,))
        return str(cur.fetchone()[0])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_benchmarks():
    pg_conn   = psycopg2.connect(_build_pg_dsn())
    pg_conn.autocommit = False
    tsdb_conn = psycopg2.connect(_build_tsdb_dsn())
    tsdb_conn.autocommit = False

    pg_min_ts   = get_min_ts(pg_conn,   "signals",          PATIENT_ID)
    tsdb_min_ts = get_min_ts(tsdb_conn, "thermal_readings", PATIENT_ID)
    log.info("Anchor timestamps — PG: %s | TSDB: %s", pg_min_ts, tsdb_min_ts)

    raw_rows     = []
    summary_rows = []

    # ── Read / Aggregation / Storage queries ────────────────────────────────
    read_tasks = [
        ("BM-R1", "Read",
         BM_R1_PG.format(patient_id=PATIENT_ID,   min_ts=pg_min_ts),
         BM_R1_TSDB.format(patient_id=PATIENT_ID, min_ts=tsdb_min_ts)),
        ("BM-R2", "Read",
         BM_R2_PG.format(patient_id=PATIENT_ID),
         BM_R2_TSDB.format(patient_id=PATIENT_ID)),
        ("BM-A1", "Aggregation",
         BM_A1_PG.format(patient_id=PATIENT_ID),
         BM_A1_TSDB.format(patient_id=PATIENT_ID)),
        ("BM-A2", "Aggregation", BM_A2_PG, BM_A2_TSDB),
        ("BM-S1", "Storage",     BM_S1_PG, BM_S1_TSDB),
        ("BM-S2", "Storage",     BM_S2_PG, BM_S2_TSDB),
    ]

    for query_id, category, pg_sql, tsdb_sql in read_tasks:
        log.info("── %s (%s) ──────────────────────────────────", query_id, category)
        for db_label, conn, sql in [
            ("PostgreSQL",  pg_conn,   pg_sql),
            ("TimescaleDB", tsdb_conn, tsdb_sql),
        ]:
            # warm-up (not recorded)
            try:
                time_query(conn, sql)
            except Exception as e:
                log.warning("Warm-up failed %s/%s: %s", query_id, db_label, e)
                conn.rollback()

            timings = []
            for run_i in range(1, N_RUNS + 1):
                try:
                    ms = time_query(conn, sql)
                    timings.append(ms)
                    log.info("  %s | %s | run %d : %.2f ms",
                             query_id, db_label, run_i, ms)
                    raw_rows.append({
                        "query_id": query_id, "category": category,
                        "db": db_label, "run": run_i, "elapsed_ms": round(ms, 2),
                    })
                except Exception as e:
                    log.error("ERROR %s/%s run %d: %s", query_id, db_label, run_i, e)
                    conn.rollback()

            if timings:
                s = summarize(query_id, category, db_label, timings)
                log.info("  %s | %s | median=%.2f ms | p95=%.2f ms",
                         query_id, db_label, s["median_ms"], s["p95_ms"])
                summary_rows.append(s)

    # ── Write queries ────────────────────────────────────────────────────────
    write_tasks = [
        ("BM-W1", "Write", 10_000,
         SETUP_W1_PG, INSERT_W1_PG, CLEANUP_W1_PG,
         SETUP_W1_TSDB, INSERT_W1_TSDB, CLEANUP_W1_TSDB),
        ("BM-W2", "Write", 50_000,
         SETUP_W2_PG, INSERT_W2_PG, CLEANUP_W2_PG,
         SETUP_W2_TSDB, INSERT_W2_TSDB, CLEANUP_W2_TSDB),
    ]

    for (query_id, category, n_rows,
         pg_setup, pg_insert, pg_cleanup,
         tsdb_setup, tsdb_insert, tsdb_cleanup) in write_tasks:
        log.info("── %s (%s, %d rows) ─────────────────────────", query_id, category, n_rows)
        for db_label, conn, setup, insert, cleanup in [
            ("PostgreSQL",  pg_conn,   pg_setup,   pg_insert,   pg_cleanup),
            ("TimescaleDB", tsdb_conn, tsdb_setup, tsdb_insert, tsdb_cleanup),
        ]:
            timings = []
            for run_i in range(1, N_RUNS + 1):
                try:
                    ms = time_write_query(conn, setup, insert, cleanup)
                    timings.append(ms)
                    log.info("  %s | %s | run %d : %.2f ms (%.0f rows/s)",
                             query_id, db_label, run_i, ms, n_rows / (ms / 1000))
                    raw_rows.append({
                        "query_id": query_id, "category": category,
                        "db": db_label, "run": run_i, "elapsed_ms": round(ms, 2),
                    })
                except Exception as e:
                    log.error("ERROR %s/%s run %d: %s", query_id, db_label, run_i, e)
                    conn.rollback()

            if timings:
                s = summarize(query_id, category, db_label, timings)
                log.info("  %s | %s | median=%.2f ms | p95=%.2f ms",
                         query_id, db_label, s["median_ms"], s["p95_ms"])
                summary_rows.append(s)

    # ── Export ───────────────────────────────────────────────────────────────
    raw_path = OUT_DIR / "benchmark_raw.csv"
    with open(raw_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "query_id", "category", "db", "run", "elapsed_ms"])
        writer.writeheader()
        writer.writerows(raw_rows)
    log.info("Raw timings  -> %s", raw_path)

    summary_path = OUT_DIR / "benchmark_summary.csv"
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "query_id", "category", "db",
            "median_ms", "p95_ms", "mean_ms", "min_ms", "max_ms"])
        writer.writeheader()
        writer.writerows(summary_rows)
    log.info("Summary CSV  -> %s", summary_path)

    json_path = OUT_DIR / "benchmark_summary.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary_rows, f, indent=2)
    log.info("Summary JSON -> %s", json_path)

    pg_conn.close()
    tsdb_conn.close()
    log.info("Benchmark complete.")


if __name__ == "__main__":
    run_benchmarks()