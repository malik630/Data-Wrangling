-- =============================================================================
-- benchmark_queries.sql
-- Sprint 2 – T6 : Benchmark Report
-- Topic M6 : Synthetic Thermal Time-Series
-- Team SG03
--
-- 8 official benchmark queries × 2 databases (PostgreSQL + TimescaleDB)
-- Each query must be run 5 times; record median + p95 latency.
--
-- Query categories:
--   BM-W1 / BM-W2  → Write speed
--   BM-R1 / BM-R2  → Range scans (Read)
--   BM-A1 / BM-A2  → Aggregations
--   BM-S1 / BM-S2  → Storage metrics
-- =============================================================================


-- =============================================================================
-- BM-W1 : Write speed — bulk insert 10 000 rows into signals / thermal_readings
-- Metric : rows/s (10 000 / elapsed_s)
-- =============================================================================

-- ── PostgreSQL ────────────────────────────────────────────────────────────────
-- Step 1: create a staging table with 10 000 rows sampled from signals
CREATE TEMP TABLE IF NOT EXISTS bm_write_batch AS
SELECT
    timestamp + INTERVAL '72 hours' AS timestamp,   -- shift to avoid PK conflict
    patient_id,
    left_temperature,
    right_temperature,
    left_temperature_norm,
    right_temperature_norm,
    temp_asymmetry,
    anomaly_label,
    anomaly_type,
    is_interpolated,
    segment_id,
    session_second
FROM signals
ORDER BY patient_id, timestamp
LIMIT 10000;

-- Step 2: run this block 5 times and measure wall-clock time externally
INSERT INTO signals (
    timestamp, patient_id,
    left_temperature, right_temperature,
    left_temperature_norm, right_temperature_norm,
    temp_asymmetry, anomaly_label, anomaly_type,
    is_interpolated, segment_id, session_second
)
SELECT * FROM bm_write_batch
ON CONFLICT DO NOTHING;

-- Step 3: clean up after each run to keep row count stable
DELETE FROM signals
WHERE timestamp > NOW() - INTERVAL '1 second'
  AND session_second = (SELECT session_second FROM bm_write_batch LIMIT 1);


-- ── TimescaleDB ───────────────────────────────────────────────────────────────
CREATE TEMP TABLE IF NOT EXISTS bm_write_batch_tsdb AS
SELECT
    timestamp + INTERVAL '72 hours' AS timestamp,
    patient_id,
    left_temperature,
    right_temperature,
    left_temperature_norm,
    right_temperature_norm,
    temp_asymmetry,
    anomaly_label,
    anomaly_type,
    is_interpolated,
    segment_id,
    session_second
FROM thermal_readings
ORDER BY patient_id, timestamp
LIMIT 10000;

INSERT INTO thermal_readings (
    timestamp, patient_id,
    left_temperature, right_temperature,
    left_temperature_norm, right_temperature_norm,
    temp_asymmetry, anomaly_label, anomaly_type,
    is_interpolated, segment_id, session_second
)
SELECT * FROM bm_write_batch_tsdb;

DELETE FROM thermal_readings
WHERE timestamp >= (SELECT MIN(timestamp) FROM bm_write_batch_tsdb)
  AND timestamp <= (SELECT MAX(timestamp) FROM bm_write_batch_tsdb)
  AND patient_id = (SELECT patient_id FROM bm_write_batch_tsdb LIMIT 1);


-- =============================================================================
-- BM-W2 : Write speed — bulk insert 50 000 rows
-- Same pattern as BM-W1 but with LIMIT 50000
-- Metric : rows/s (50 000 / elapsed_s)
-- =============================================================================

-- ── PostgreSQL ────────────────────────────────────────────────────────────────
CREATE TEMP TABLE IF NOT EXISTS bm_write_batch_50k AS
SELECT
    timestamp + INTERVAL '144 hours' AS timestamp,
    patient_id,
    left_temperature,
    right_temperature,
    left_temperature_norm,
    right_temperature_norm,
    temp_asymmetry,
    anomaly_label,
    anomaly_type,
    is_interpolated,
    segment_id,
    session_second
FROM signals
ORDER BY patient_id, timestamp
LIMIT 50000;

INSERT INTO signals (
    timestamp, patient_id,
    left_temperature, right_temperature,
    left_temperature_norm, right_temperature_norm,
    temp_asymmetry, anomaly_label, anomaly_type,
    is_interpolated, segment_id, session_second
)
SELECT * FROM bm_write_batch_50k
ON CONFLICT DO NOTHING;

DELETE FROM signals
WHERE timestamp >= (SELECT MIN(timestamp) FROM bm_write_batch_50k)
  AND timestamp <= (SELECT MAX(timestamp) FROM bm_write_batch_50k);


-- ── TimescaleDB ───────────────────────────────────────────────────────────────
CREATE TEMP TABLE IF NOT EXISTS bm_write_batch_50k_tsdb AS
SELECT
    timestamp + INTERVAL '144 hours' AS timestamp,
    patient_id,
    left_temperature,
    right_temperature,
    left_temperature_norm,
    right_temperature_norm,
    temp_asymmetry,
    anomaly_label,
    anomaly_type,
    is_interpolated,
    segment_id,
    session_second
FROM thermal_readings
ORDER BY patient_id, timestamp
LIMIT 50000;

INSERT INTO thermal_readings (
    timestamp, patient_id,
    left_temperature, right_temperature,
    left_temperature_norm, right_temperature_norm,
    temp_asymmetry, anomaly_label, anomaly_type,
    is_interpolated, segment_id, session_second
)
SELECT * FROM bm_write_batch_50k_tsdb;

DELETE FROM thermal_readings
WHERE timestamp >= (SELECT MIN(timestamp) FROM bm_write_batch_50k_tsdb)
  AND timestamp <= (SELECT MAX(timestamp) FROM bm_write_batch_50k_tsdb);


-- =============================================================================
-- BM-R1 : Read — 1-hour range scan for patient 03
-- Retrieves all signal rows for patient_03 over a 1-hour window.
-- Metric : query latency (ms)
-- =============================================================================

-- ── PostgreSQL ────────────────────────────────────────────────────────────────
SELECT
    timestamp,
    left_temperature,
    right_temperature,
    temp_asymmetry,
    anomaly_label
FROM signals
WHERE patient_id = 3
  AND timestamp >= (
      SELECT MIN(timestamp) FROM signals WHERE patient_id = 3
  )
  AND timestamp < (
      SELECT MIN(timestamp) FROM signals WHERE patient_id = 3
  ) + INTERVAL '1 hour'
ORDER BY timestamp;


-- ── TimescaleDB ───────────────────────────────────────────────────────────────
SELECT
    timestamp,
    left_temperature,
    right_temperature,
    temp_asymmetry,
    anomaly_label
FROM thermal_readings
WHERE patient_id = 3
  AND timestamp >= (
      SELECT MIN(timestamp) FROM thermal_readings WHERE patient_id = 3
  )
  AND timestamp < (
      SELECT MIN(timestamp) FROM thermal_readings WHERE patient_id = 3
  ) + INTERVAL '1 hour'
ORDER BY timestamp;


-- =============================================================================
-- BM-R2 : Read — full 72-hour scan for patient 03
-- Retrieves all 259 200 rows for one patient.
-- Metric : query latency (ms)
-- =============================================================================

-- ── PostgreSQL ────────────────────────────────────────────────────────────────
SELECT
    timestamp,
    left_temperature,
    right_temperature,
    temp_asymmetry,
    anomaly_label,
    anomaly_type
FROM signals
WHERE patient_id = 3
ORDER BY timestamp;


-- ── TimescaleDB ───────────────────────────────────────────────────────────────
SELECT
    timestamp,
    left_temperature,
    right_temperature,
    temp_asymmetry,
    anomaly_label,
    anomaly_type
FROM thermal_readings
WHERE patient_id = 3
ORDER BY timestamp;


-- =============================================================================
-- BM-A1 : Aggregation — average temperature per minute for patient 03
-- Computes mean left/right temp grouped into 1-minute buckets.
-- Metric : query latency (ms)
-- =============================================================================

-- ── PostgreSQL ────────────────────────────────────────────────────────────────
SELECT
    DATE_TRUNC('minute', timestamp)     AS bucket,
    AVG(left_temperature)               AS avg_left,
    AVG(right_temperature)              AS avg_right,
    AVG(temp_asymmetry)                 AS avg_asymmetry,
    COUNT(*)                            AS n_samples
FROM signals
WHERE patient_id = 3
GROUP BY bucket
ORDER BY bucket;


-- ── TimescaleDB (uses time_bucket for chunk-aligned aggregation) ──────────────
SELECT
    time_bucket('1 minute', timestamp)  AS bucket,
    AVG(left_temperature)               AS avg_left,
    AVG(right_temperature)              AS avg_right,
    AVG(temp_asymmetry)                 AS avg_asymmetry,
    COUNT(*)                            AS n_samples
FROM thermal_readings
WHERE patient_id = 3
GROUP BY bucket
ORDER BY bucket;

-- ── TimescaleDB alternative — query the continuous aggregate (pre-materialized)
-- Run CALL refresh_continuous_aggregate('avg_temp_per_minute', NULL, NULL)
-- once before timing this variant.
SELECT
    bucket,
    avg_left,
    avg_right,
    avg_asymmetry,
    n_samples
FROM avg_temp_per_minute
WHERE patient_id = 3
ORDER BY bucket;


-- =============================================================================
-- BM-A2 : Aggregation — anomaly rate per hour across all patients
-- For each hour bucket, compute fraction of rows with anomaly_label = 1.
-- Metric : query latency (ms)
-- =============================================================================

-- ── PostgreSQL ────────────────────────────────────────────────────────────────
SELECT
    DATE_TRUNC('hour', timestamp)               AS hour_bucket,
    patient_id,
    COUNT(*)                                    AS total_samples,
    SUM(anomaly_label)                          AS anomaly_count,
    ROUND(
        100.0 * SUM(anomaly_label) / COUNT(*),
        4
    )                                           AS anomaly_rate_pct
FROM signals
GROUP BY hour_bucket, patient_id
ORDER BY hour_bucket, patient_id;


-- ── TimescaleDB ───────────────────────────────────────────────────────────────
SELECT
    time_bucket('1 hour', timestamp)            AS hour_bucket,
    patient_id,
    COUNT(*)                                    AS total_samples,
    SUM(anomaly_label)                          AS anomaly_count,
    ROUND(
        100.0 * SUM(anomaly_label) / COUNT(*),
        4
    )                                           AS anomaly_rate_pct
FROM thermal_readings
GROUP BY hour_bucket, patient_id
ORDER BY hour_bucket, patient_id;


-- =============================================================================
-- BM-S1 : Storage — raw disk size of the main signal table
-- Metric : MB (no query timer needed; read from system catalog)
-- =============================================================================

-- ── PostgreSQL ────────────────────────────────────────────────────────────────
SELECT
    pg_size_pretty(pg_total_relation_size('signals'))           AS total_size,
    pg_size_pretty(pg_relation_size('signals'))                 AS table_size,
    pg_size_pretty(
        pg_total_relation_size('signals')
        - pg_relation_size('signals')
    )                                                           AS index_size;


-- ── TimescaleDB ───────────────────────────────────────────────────────────────
SELECT
    pg_size_pretty(hypertable_size('thermal_readings'))         AS total_size,
    pg_size_pretty(
        (SELECT before_compression_total_bytes
         FROM hypertable_compression_stats('thermal_readings'))
    )                                                           AS uncompressed_size,
    pg_size_pretty(
        (SELECT after_compression_total_bytes
         FROM hypertable_compression_stats('thermal_readings'))
    )                                                           AS compressed_size;


-- =============================================================================
-- BM-S2 : Storage — compression ratio (TimescaleDB only)
-- Metric : ratio and % saved (no query timer needed)
-- =============================================================================

-- ── TimescaleDB ───────────────────────────────────────────────────────────────
SELECT
    pg_size_pretty(before_compression_total_bytes)              AS before,
    pg_size_pretty(after_compression_total_bytes)               AS after,
    ROUND(
        before_compression_total_bytes::numeric
        / NULLIF(after_compression_total_bytes, 0),
        2
    )                                                           AS compression_ratio,
    ROUND(
        100.0 * (1 - after_compression_total_bytes::numeric
                   / NULLIF(before_compression_total_bytes, 0)),
        1
    )                                                           AS pct_saved
FROM hypertable_compression_stats('thermal_readings');

-- ── PostgreSQL (baseline — no compression, ratio = 1.0) ──────────────────────
SELECT
    pg_size_pretty(pg_total_relation_size('signals'))           AS total_size,
    1.0                                                         AS compression_ratio,
    0.0                                                         AS pct_saved;
