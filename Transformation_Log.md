# Cleaning and Transformation Log

## Topic M6 : Synthetic Thermal Time-Series | Sprint 2

**Team:** SG03  
**Pipeline:** `etl_pipeline.py`  
**Input:** 20 × `patient_XX.csv` (259,200 rows each, 1 Hz, 72 hours)  
**Output:** 20 × cleaned CSV + NPY + combined window metadata

---

## 1. Input Dataset Summary

| Parameter            | Value                               |
| -------------------- | ----------------------------------- |
| Patients             | 20 (patient_00 to patient_19)       |
| Sampling rate        | 1 Hz                                |
| Duration per patient | 72 hours (259,200 samples)          |
| Channels             | left_temperature, right_temperature |
| Label classes        | 0 = normal, 1 = spike/drift anomaly |
| Total input rows     | 5,184,000                           |
| Raw CSV size         | ~240 MB (20 × ~12 MB)               |

---

## 2. Stage 1 : CSV Parsing & Schema Validation

**Decision:** Full file rejection on any schema or label coherence violation, consistent with the Annotation Conventions specification.

### Header Validation

- All 6 expected columns must be present: `timestamp`, `patient_id`, `left_temperature`, `right_temperature`, `anomaly_label`, `anomaly_type`
- Delimiter must be comma; encoding must be UTF-8
- Column order mismatch triggers a warning and auto-mapping (not rejection)

### Row-Level Validation

| Rule                   | Condition                                 | Action                 |
| ---------------------- | ----------------------------------------- | ---------------------- |
| Null guard             | No NULL in any column                     | Quarantine row         |
| Temperature range      | 30°C ≤ temp ≤ 37.5°C                      | Quarantine + flag      |
| Label coherence        | (0,"none"), (1,"spike"), (1,"drift") only | **Reject entire file** |
| Timestamp monotonicity | Each ts > previous ts                     | Quarantine row         |
| Sampling regularity    | Δt = 1.0 s ± 0.01 s                       | Flag gap, continue     |

### Results

- Files processed: 20
- Files accepted: **20**
- Files rejected: **0**
- Rows quarantined: **0**

All 20 patient files passed validation with no anomalous label combinations, no null values, and no temperature range violations.

---

## 3. Stage 2 : Gap Detection & Imputation

**Decision:** Tiered gap policy applied per the ETL Logic Documentation.

### Gap Policy

| Gap Duration    | Action                                                | Flag                     |
| --------------- | ----------------------------------------------------- | ------------------------ |
| Δt ≤ 5 seconds  | Linear interpolation between adjacent samples         | `is_interpolated = True` |
| 6 s ≤ Δt ≤ 60 s | Forward-fill (copy last known value) + warning logged | `is_interpolated = True` |
| Δt > 60 seconds | New segment opened; `segment_id` incremented          | new `segment_id`         |

**Rationale for linear interpolation (≤5 s):** Short gaps in 1 Hz thermal signals are well-approximated by linear interpolation because skin temperature changes are physiologically slow (<<1°C/s in normal baseline). A 5-second linear fill introduces negligible distortion.

**Rationale for forward-fill (6-60 s):** Medium gaps are filled with the last known valid sample to preserve continuity for the windowing stage. This is less accurate than interpolation but avoids introducing spurious trends. Affected rows are flagged so Sprint 3 can down-weight them.

**Rationale for segment splitting (>60 s):** Gaps exceeding one minute represent true signal discontinuities (sensor dropout, session boundary). Treating them as continuous would corrupt the time-series statistics. Each segment is treated as an independent recording for normalization and windowing purposes.

### Results

| Metric               | Value                           |
| -------------------- | ------------------------------- |
| Total gaps detected  | **0**                           |
| Interpolated rows    | **0**                           |
| Forward-filled rows  | **0**                           |
| Segment splits       | **0**                           |
| Segments per patient | **1** (all patients continuous) |

No timestamp gaps were found in any of the 20 patient files. The synthetic generator produced perfectly regular 1 Hz recordings. The `is_interpolated` column is present in all outputs (set to `False`) for schema consistency and forward-compatibility with real-world acquisitions.

---

## 4. Stage 3 : Derived Field Computation

Two fields are computed during transformation before insertion, per the ETL Documentation specification:

### `temp_asymmetry`

```
temp_asymmetry = right_temperature - left_temperature
```

Quantifies the inter-breast thermal differential. A persistent positive asymmetry is a known clinical indicator studied in breast thermography literature. Rounded to 4 decimal places.

### `session_second`

```
session_second = row index within segment (0-indexed)
```

Provides a relative timestamp within each segment, enabling efficient TS-DB partitioning without relying on absolute timestamps.

---

## 5. Stage 4 : Per-Patient Z-Score Normalization

**Decision:** Z-score standardization applied per patient, computed over the full 72-hour recording **before** windowing.

### Formula

```
z = (value − μ) / σ
```

Where μ and σ are computed from all 259,200 samples for each channel independently.

**Rationale for Z-score over Min-Max:** The anomaly injection engine introduces localized spikes (+2°C over 5-30 seconds). Min-Max normalization would compress the normal signal range around these outlier peaks, causing the model to learn a distorted baseline. Z-score is robust to these localized extremes because the spike occupies at most 0.01% of the total samples.

**Rationale for per-patient normalization:** Each subject has a distinct thermal baseline (MESOR). Global normalization would conflate inter-subject variability with anomaly signal. Per-patient normalization ensures the model learns anomaly patterns relative to each subject's individual physiology.

**Storage of normalization parameters:** `norm_mean_left`, `norm_std_left`, `norm_mean_right`, `norm_std_right` are stored in the ETL summary JSON. Without these, model predictions cannot be inverse-transformed back to interpretable °C values.

### Per-Patient Normalization Parameters

| Patient | μ_left (°C) | σ_left   | μ_right (°C) | σ_right  |
| ------- | ----------- | -------- | ------------ | -------- |
| 00      | 33.563273   | 0.582701 | 33.663318    | 0.582203 |
| 01      | 33.672544   | 0.636325 | 33.772901    | 0.636477 |
| 02      | 33.595156   | 0.536489 | 33.695180    | 0.536445 |
| 03      | 34.520721   | 0.413283 | 34.620877    | 0.413262 |
| 04      | 33.174790   | 0.585416 | 33.274588    | 0.584945 |
| 05      | 33.100009   | 0.507008 | 33.199808    | 0.506697 |
| 06      | 34.027265   | 0.710451 | 34.126889    | 0.710031 |
| 07      | 33.500974   | 0.603697 | 33.600911    | 0.603812 |
| 08      | 32.631582   | 0.496563 | 32.731847    | 0.496983 |
| 09      | 33.098907   | 0.612392 | 33.198926    | 0.612685 |
| 10      | 32.948645   | 0.544682 | 33.049763    | 0.545065 |
| 11      | 33.517651   | 0.679020 | 33.617715    | 0.679275 |
| 12      | 33.497819   | 0.668908 | 33.597267    | 0.668363 |
| 13      | 34.413773   | 0.388289 | 34.513423    | 0.387611 |
| 14      | 33.848317   | 0.501919 | 33.948274    | 0.501919 |
| 15      | 32.784971   | 0.521961 | 32.885317    | 0.521700 |
| 16      | 33.203477   | 0.642149 | 33.303549    | 0.642496 |
| 17      | 34.051013   | 0.603289 | 34.150934    | 0.603023 |
| 18      | 33.284169   | 0.503383 | 33.384222    | 0.503533 |
| 19      | 33.315396   | 0.653417 | 33.415798    | 0.653745 |

---

## 6. Stage 5 : Windowing & Segmentation

**Decision:** Fixed-size overlapping windows with conservative majority-vote labeling.

### Parameters

| Parameter   | Value                           | Rationale                                             |
| ----------- | ------------------------------- | ----------------------------------------------------- |
| Window size | 60 seconds (60 samples at 1 Hz) | Covers one full thermal fluctuation cycle             |
| Overlap     | 30 seconds (50%)                | Preserves anomaly continuity across window boundaries |
| Step        | 30 seconds                      | Derived: window_size − overlap                        |

### Window Labeling Rule

A window receives `label = 1` if **at least 1 sample** within the window carries `anomaly_label = 1`.

**Rationale:** This conservative rule minimizes false negatives. Because spike anomalies are short (5-30 s) relative to the window (60 s), a stricter majority rule would cause many true anomaly windows to be mislabeled as normal. The `anomaly_ratio` field (fraction of anomalous samples in the window) is stored alongside the binary label to allow threshold tuning during Sprint 3 model evaluation.

### Window Results

| Metric                | Value                |
| --------------------- | -------------------- |
| Windows per patient   | 8,639                |
| Total windows         | **172,780**          |
| Total anomaly windows | **564** (0.33%)      |
| Total normal windows  | **172,216** (99.67%) |
| Class imbalance ratio | 306:1                |

**Note on class imbalance:** The 0.33% anomaly rate reflects the spike-injection density of the synthetic generator. Sprint 3 should apply weighted loss functions or oversampling (e.g., SMOTE on the window feature space) to prevent the model from collapsing to the majority class. The `anomaly_ratio` field enables soft thresholding as an alternative.

---

## 7. Output Files

### Cleaned CSV (`cleaned_csv/patient_XX_cleaned.csv`)

One file per patient. 259,200 rows. Columns:

| Column                 | Type     | Description                            |
| ---------------------- | -------- | -------------------------------------- |
| timestamp              | datetime | ISO 8601, 1 Hz index                   |
| patient_id             | int      | 0-19                                   |
| left_temperature       | float64  | Raw left breast temp (°C), 4 dp        |
| right_temperature      | float64  | Raw right breast temp (°C), 4 dp       |
| left_temperature_norm  | float64  | Z-score normalized, 6 dp               |
| right_temperature_norm | float64  | Z-score normalized, 6 dp               |
| temp_asymmetry         | float64  | right - left, 4 dp                     |
| anomaly_label          | int8     | 0 = normal, 1 = anomaly                |
| anomaly_type           | str      | "none", "spike", or "drift"            |
| is_interpolated        | bool     | True if row was gap-filled             |
| segment_id             | int      | 0 if no splits; increments on >60s gap |
| session_second         | int      | Row index within segment               |

### NumPy Windows (`npy/patient_XX_windows.npy`)

Shape: `(8639, 5, 60)`

| Channel index | Content                          |
| ------------- | -------------------------------- |
| 0             | left_temperature (raw)           |
| 1             | right_temperature (raw)          |
| 2             | left_temperature_norm (z-score)  |
| 3             | right_temperature_norm (z-score) |
| 4             | temp_asymmetry                   |

Companion metadata: `npy/patient_XX_windows_meta.csv` (window_id, segment_id, label, anomaly_ratio, is_interpolated, timestamps).

### Combined Metadata (`all_windows_meta.csv`)

172,780 rows. All patients combined. Required for DB insertion.

---

## 8. Known Limitations

- **No gap imputation exercised:** The synthetic data has perfectly regular 1 Hz timestamps. The gap-handling code is implemented and tested in structure but not exercised on real data. Real-world acquisitions should re-validate gap thresholds.
- **Spike-only anomalies:** All 20 patients contain only spike anomalies. Drift anomalies (≥600 s duration) are not represented in the current dataset, leading to a gap in anomaly type coverage. Sprint 3 models may not generalize to drift patterns without augmentation.
- **Severe class imbalance (0.33%):** Downstream models must account for this. Raw accuracy is a misleading metric; F1, AUC-ROC, and Precision-Recall curves are required.
- **Normalization parameters are patient-specific:** Cross-patient inference requires re-normalization or a domain-adaptation step.

---

## 9. Reproducibility

The pipeline is fully reproducible. Running:

```bash
python etl_pipeline.py \
  --data-dir <path_to_raw_csvs> \
  --out-dir <output_dir> \
  --patients 20
```

produces identical outputs given the same input files. No random seeds are used in the ETL stage (normalization, windowing, and gap handling are all deterministic). The ETL summary JSON (`etl_summary.json`) records all normalization parameters and run metadata for auditability.
