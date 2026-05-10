-- =============================================================================
-- schema_postgres.sql
-- Sprint 2 – T5 : Dual DB Ingestion
-- Topic M6 : Synthetic Thermal Time-Series
-- Team: SG03
--
-- PostgreSQL schema (4 tables)
-- Run once before ingest_postgres.py
-- =============================================================================

-- --------------------------------------------------------------------
-- 0. Extension & search path
-- --------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- for gen_random_uuid() if needed

-- --------------------------------------------------------------------
-- 1. subjects
--    One row per patient. Normalization params from etl_summary.json.
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS subjects (
    patient_id          SMALLINT        PRIMARY KEY,          -- 0-19
    norm_mean_left      DOUBLE PRECISION NOT NULL,            -- μ left channel
    norm_std_left       DOUBLE PRECISION NOT NULL,            -- σ left channel
    norm_mean_right     DOUBLE PRECISION NOT NULL,            -- μ right channel
    norm_std_right      DOUBLE PRECISION NOT NULL,            -- σ right channel
    rows_input          INTEGER,
    rows_output         INTEGER,
    n_windows           INTEGER,
    n_anomaly_windows   INTEGER,
    n_segments          INTEGER,
    n_interpolated_rows INTEGER,
    created_at          TIMESTAMPTZ     DEFAULT NOW()
);

COMMENT ON TABLE subjects IS
    'One row per patient. Normalization params are required by Sprint 3 '
    'to inverse-transform z-scores back to °C.';

-- --------------------------------------------------------------------
-- 2. recordings
--    Segment-level metadata (segment_id is local to each patient).
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recordings (
    id              SERIAL          PRIMARY KEY,
    patient_id      SMALLINT        NOT NULL REFERENCES subjects(patient_id),
    segment_id      INTEGER         NOT NULL,                 -- 0-based within patient
    segment_start   TIMESTAMPTZ,
    segment_end     TIMESTAMPTZ,
    n_rows          INTEGER,
    UNIQUE (patient_id, segment_id)
);

COMMENT ON TABLE recordings IS
    'One row per (patient, segment). A new segment is opened when a gap > 60 s '
    'was detected by the ETL pipeline (Stage 2).';

CREATE INDEX IF NOT EXISTS idx_recordings_patient
    ON recordings (patient_id);

-- --------------------------------------------------------------------
-- 3. signals
--    The main time-series table. 20 patients × 259 200 rows = 5 184 000 rows.
--    Mirrors all columns from patient_XX_cleaned.csv.
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signals (
    id                      BIGSERIAL       PRIMARY KEY,
    timestamp               TIMESTAMPTZ     NOT NULL,
    patient_id              SMALLINT        NOT NULL REFERENCES subjects(patient_id),
    left_temperature        REAL            NOT NULL,         -- raw °C, 4 dp
    right_temperature       REAL            NOT NULL,         -- raw °C, 4 dp
    left_temperature_norm   REAL            NOT NULL,         -- z-score, 6 dp
    right_temperature_norm  REAL            NOT NULL,         -- z-score, 6 dp
    temp_asymmetry          REAL            NOT NULL,         -- right − left, 4 dp
    anomaly_label           SMALLINT        NOT NULL CHECK (anomaly_label IN (0, 1)),
    anomaly_type            VARCHAR(8)      NOT NULL CHECK (anomaly_type IN ('none','spike','drift')),
    is_interpolated         BOOLEAN         NOT NULL DEFAULT FALSE,
    segment_id              INTEGER         NOT NULL DEFAULT 0,
    session_second          INTEGER         NOT NULL
);

COMMENT ON TABLE signals IS
    '5 184 000 rows of 1 Hz thermal readings. '
    'Load with COPY for ~200 k rows/s throughput. '
    'is_interpolated / temp_asymmetry / segment_id are required by Sprint 3.';

-- Indexes for benchmark queries (created AFTER bulk load to avoid overhead)
-- BM-R1 / BM-R2 : range scans by patient + time
CREATE INDEX IF NOT EXISTS idx_signals_patient_ts
    ON signals (patient_id, timestamp);

-- BM-A1 : avg temp per minute → time bucketing
CREATE INDEX IF NOT EXISTS idx_signals_ts
    ON signals (timestamp);

-- BM-A2 : anomaly rate per hour → label filter
CREATE INDEX IF NOT EXISTS idx_signals_anomaly
    ON signals (anomaly_label)
    WHERE anomaly_label = 1;

-- --------------------------------------------------------------------
-- 4. windows
--    Window-level metadata from patient_XX_windows_meta.csv
--    + all_windows_meta.csv (172 780 rows total).
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS windows (
    window_id           INTEGER         NOT NULL,
    patient_id          SMALLINT        NOT NULL REFERENCES subjects(patient_id),
    segment_id          INTEGER         NOT NULL,
    window_index        INTEGER         NOT NULL,             -- local index within patient
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    label               SMALLINT        NOT NULL CHECK (label IN (0, 1)),
    anomaly_ratio       REAL            NOT NULL,             -- fraction of anomalous samples
    is_interpolated     BOOLEAN         NOT NULL DEFAULT FALSE,
    PRIMARY KEY (patient_id, window_id)
);

COMMENT ON TABLE windows IS
    '172 780 rows of 60 s window metadata (50 % overlap). '
    'label=1 if ≥1 anomalous sample. anomaly_ratio enables soft thresholding in Sprint 3.';

CREATE INDEX IF NOT EXISTS idx_windows_patient
    ON windows (patient_id);

CREATE INDEX IF NOT EXISTS idx_windows_label
    ON windows (label)
    WHERE label = 1;

-- --------------------------------------------------------------------
-- 5. Verify
-- --------------------------------------------------------------------
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;