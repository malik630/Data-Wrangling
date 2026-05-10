# Sprint 2 : ETL Pipeline & Dual DB Ingestion

**Topic M6 - Synthetic Thermal Time-Series - Team SG03**

---

## Structure du repo

```
Data-Wrangling/
├── etl_pipeline.py                   # Pipeline ETL complet (6 stages)
├── Transformation_Log.md             # Documentation de toutes les décisions
├── requirements.txt                  # Dépendances Python
├── .env.example                      # Template des variables d'environnement
├── .env                              # (à créer localement, non commité)
├── README.md
├── dual_db_ingestion/
│   ├── scripts/
│   │   ├── ingest_postgres.py        # Ingestion PostgreSQL (T5)
│   │   └── ingest_tsdb.py            # Ingestion TimescaleDB (T5)
│   └── sql/
│       ├── schema_postgres.sql       # DDL PostgreSQL
│       └── schema_timescaledb.sql    # DDL TimescaleDB + hypertable
└── data/
    ├── raw/                          # Fichiers patient_XX.csv bruts
    └── processed/                    # Sortie ETL (généré automatiquement)
```

---

## Prérequis

- Python 3.10 ou supérieur
- PostgreSQL installé avec l'extension TimescaleDB activée
- Les 20 fichiers `patient_00.csv` … `patient_19.csv` placés dans `data/raw/`

---
## Environnement virtuel

```bash
python -m venv venv
venv\Scripts\activate
```

---
## Installation

```bash
pip install -r requirements.txt
```

---

## Étape 1 — Lancer le pipeline ETL (T4)

```bash
python etl_pipeline.py --data-dir data/raw --out-dir data/processed
```

### Options disponibles

| Argument     | Défaut           | Description                                      |
| ------------ | ---------------- | ------------------------------------------------ |
| `--data-dir` | `data/raw`       | Dossier contenant les CSV bruts                  |
| `--out-dir`  | `data/processed` | Dossier de sortie                                |
| `--patients` | `20`             | Nombre de patients à traiter (utile pour tester) |

**Exemple test rapide sur 2 patients :**

```bash
python etl_pipeline.py --data-dir data/raw --out-dir data/processed --patients 2
```

---

## Fichiers générés (dans `data/processed/`)

```
data/processed/
├── cleaned_csv/
│   ├── patient_00_cleaned.csv         # Signal nettoyé + colonnes enrichies
│   └── ...                            # × 20 patients
├── npy/
│   ├── patient_00_windows.npy         # Fenêtres (8639, 5, 60) float32
│   ├── patient_00_windows_meta.csv    # Métadonnées des fenêtres
│   └── ...                            # × 20 patients
├── all_windows_meta.csv               # Toutes les fenêtres combinées (172 780 lignes)
└── logs/
    ├── etl_run_<timestamp>.log        # Log détaillé de l'exécution
    └── etl_summary.json               # Rapport complet + paramètres de normalisation
```

### `cleaned_csv/patient_XX_cleaned.csv`

259 200 lignes (72h à 1 Hz). Colonnes :

| Colonne                  | Description                               |
| ------------------------ | ----------------------------------------- |
| `timestamp`              | Horodatage ISO 8601                       |
| `left_temperature`       | Température brute gauche (°C)             |
| `right_temperature`      | Température brute droite (°C)             |
| `left_temperature_norm`  | Z-score normalisé gauche                  |
| `right_temperature_norm` | Z-score normalisé droite                  |
| `temp_asymmetry`         | right − left (°C)                         |
| `anomaly_label`          | 0 = normal, 1 = anomalie                  |
| `anomaly_type`           | none / spike / drift                      |
| `is_interpolated`        | True si la ligne a été reconstruite (gap) |
| `segment_id`             | 0 si continu ; monte si gap > 60 s        |
| `session_second`         | Position relative dans le segment         |

### `npy/patient_XX_windows.npy`

Tableau NumPy de shape `(8639, 5, 60)` en float32.

| Index canal | Contenu                            |
| ----------- | ---------------------------------- |
| 0           | `left_temperature` (brut)          |
| 1           | `right_temperature` (brut)         |
| 2           | `left_temperature_norm` (z-score)  |
| 3           | `right_temperature_norm` (z-score) |
| 4           | `temp_asymmetry`                   |

```python
import numpy as np
arr = np.load("data/processed/npy/patient_00_windows.npy")
# arr.shape == (8639, 5, 60)
```

### `npy/patient_XX_windows_meta.csv`

Métadonnées de chaque fenêtre :

| Colonne                       | Description                                  |
| ----------------------------- | -------------------------------------------- |
| `window_id`                   | Identifiant unique                           |
| `patient_id`                  | ID patient                                   |
| `segment_id`                  | Segment d'origine                            |
| `window_start` / `window_end` | Timestamps de la fenêtre                     |
| `label`                       | 0 = normale, 1 = anomalie                    |
| `anomaly_ratio`               | Fraction de samples anormaux dans la fenêtre |
| `is_interpolated`             | Contient des points reconstruits             |

**À insérer dans la table `windows` (PostgreSQL / TimescaleDB)**

### `logs/etl_summary.json`

Contient les statistiques par patient et les **paramètres de normalisation**
(`norm_mean_left`, `norm_std_left`, `norm_mean_right`, `norm_std_right`)
indispensables en Sprint 3 pour inverser le z-score.

---

## Statistiques ETL

| Métrique             | Valeur             |
| -------------------- | ------------------ |
| Patients traités     | 20                 |
| Rows par patient     | 259 200            |
| Fenêtres par patient | 8 639              |
| Total fenêtres       | 172 780            |
| Fenêtres anormales   | 564 (0.33 %)       |
| Taille fenêtre       | 60 s               |
| Overlap              | 50 % (step = 30 s) |

---

## Étape 2 — Ingestion Dual DB (T5)

### Prérequis base de données

Créer les deux bases dans PostgreSQL :

```sql
CREATE DATABASE m6_thermal;
CREATE DATABASE m6_thermal_tsdb;
```

Activer TimescaleDB sur la deuxième :

```sql
\c m6_thermal_tsdb
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

### Configuration des credentials

Copier le fichier d'exemple et remplir ses valeurs :

```bash
cp .env.example .env
```

Ouvrir `.env` et renseigner le mot de passe :

```
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=ton_mot_de_passe       ← à modifier
PG_DB=m6_thermal

TSDB_HOST=localhost
TSDB_PORT=5432
TSDB_USER=postgres
TSDB_PASSWORD=ton_mot_de_passe     ← même mot de passe
TSDB_DB=m6_thermal_tsdb

ETL_DIR=data/processed
```

> Le `.env` n'est jamais commité dans git. Chaque membre configure le sien localement.

### Lancer l'ingestion

Depuis la racine `Data-Wrangling/` :

```bash
# Ingestion PostgreSQL
python dual_db_ingestion/scripts/ingest_postgres.py

# Ingestion TimescaleDB
python dual_db_ingestion/scripts/ingest_tsdb.py
```

Les scripts créent automatiquement le schéma, insèrent les données, construisent les index et génèrent les rapports.

### Rapports générés automatiquement

```
data/processed/logs/
├── ingest_postgres_report.json    # Métriques PostgreSQL
└── ingest_tsdb_report.json        # Métriques TimescaleDB + compression
```

---

## Résultats T5 — Ingestion Rate

| Metric                        | PostgreSQL   | TimescaleDB  |
| ----------------------------- | ------------ | ------------ |
| Total rows insérés (signals)  | 5 184 000    | 5 184 000    |
| Temps total ingestion (s)     | 208.3 s      | 159.2 s      |
| Débit moyen (rows/s)          | 24 883       | 32 572       |
| Rows windows insérés          | 172 780      | 172 780      |
| Débit windows (rows/s)        | 39 003       | 32 155       |
| Temps total pipeline (s)      | 273.4 s      | 217.0 s      |

**TimescaleDB est 31 % plus rapide** sur l'ingestion des signaux (32 572 vs 24 883 rows/s).

---

## Résultats T5 — Storage Report

| Table                    | PostgreSQL  | TSDB (avant compression) | TSDB (après compression) | Gain     |
| ------------------------ | ----------- | ------------------------ | ------------------------ | -------- |
| signals / thermal_readings | 735 MB    | 749 MB                   | 119 MB                   | **84.2 %** |
| windows                  | 17 MB       | —                        | —                        | —        |
| subjects                 | 24 kB       | —                        | —                        | —        |
| recordings               | 56 kB       | —                        | —                        | —        |

- Temps de compression : 26.5 s
- Taille finale hypertable : ~120 MB vs 735 MB en PostgreSQL standard
- **Ratio de compression global : 6.3×**

---

## Pour T6 : Point de synchronisation

Les deux bases sont prêtes pour les requêtes de benchmark :

| Base de données | Host      | Port | DB name           |
| --------------- | --------- | ---- | ----------------- |
| PostgreSQL      | localhost | 5432 | `m6_thermal`      |
| TimescaleDB     | localhost | 5432 | `m6_thermal_tsdb` |

Tables disponibles :

| Table               | DB              | Rows      |
| ------------------- | --------------- | --------- |
| `signals`           | PostgreSQL      | 5 184 000 |
| `windows`           | PostgreSQL      | 172 780   |
| `subjects`          | PostgreSQL      | 20        |
| `recordings`        | PostgreSQL      | 20        |
| `thermal_readings`  | TimescaleDB     | 5 184 000 |
| `windows_tsdb`      | TimescaleDB     | 172 780   |
| `subjects`          | TimescaleDB     | 20        |