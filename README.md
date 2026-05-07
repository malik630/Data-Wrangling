# Sprint 2 : ETL Pipeline

**Topic M6 - Synthetic Thermal Time-Series - Team SG03**

---

## Structure du repo

```
Data-Wrangling/
├── etl_pipeline.py          # Pipeline ETL complet (6 stages)
├── Transformation_Log.md    # Documentation de toutes les décisions
├── requirements.txt         # Dépendances Python
├── README.md
└── data/
    └── raw/                 # placer les fichiers patient_XX.csv ici
```

---

## Prérequis

- Python 3.10 ou supérieur
- Les 20 fichiers `patient_00.csv` … `patient_19.csv` placés dans `data/raw/`

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Lancer le pipeline

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

Après exécution, le dossier de sortie contient :

```
data/processed/
├── cleaned_csv/
│   ├── patient_00_cleaned.csv    # Signal nettoyé + colonnes enrichies
│   ├── patient_01_cleaned.csv
│   └── ...                       # × 20 patients
│
├── npy/
│   ├── patient_00_windows.npy         # Fenêtres (8639, 5, 60) float32
│   ├── patient_00_windows_meta.csv    # Métadonnées des fenêtres
│   └── ...                           # × 20 patients
│
├── all_windows_meta.csv          # Toutes les fenêtres combinées (172 780 lignes)
│
└── logs/
    ├── etl_run_<timestamp>.log   # Log détaillé de l'exécution
    └── etl_summary.json          # Rapport complet + paramètres de normalisation
```

---

## Description des fichiers de sortie

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

**À insérer dans la table `signals` (PostgreSQL / TimescaleDB)**

---

### `npy/patient_XX_windows.npy`

Tableau NumPy de shape `(8639, 5, 60)` en float32.

| Index canal | Contenu                            |
| ----------- | ---------------------------------- |
| 0           | `left_temperature` (brut)          |
| 1           | `right_temperature` (brut)         |
| 2           | `left_temperature_norm` (z-score)  |
| 3           | `right_temperature_norm` (z-score) |
| 4           | `temp_asymmetry`                   |

**Chargé directement par le modèle en Sprint 3**

```python
import numpy as np
arr = np.load("data/processed/npy/patient_00_windows.npy")
# arr.shape == (8639, 5, 60)
```

---

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

---

### `all_windows_meta.csv`

Concaténation des 20 fichiers `windows_meta.csv` : 172 780 lignes. Pratique pour une insertion en masse côté T5.

---

### `logs/etl_summary.json`

Rapport machine-readable contenant :

- Statistiques par patient (rows, windows, gaps, segments)
- **Paramètres de normalisation** (`norm_mean_left`, `norm_std_left`, `norm_mean_right`, `norm_std_right`) qui sont **indispensables en Sprint 3** pour inverser le z-score et retrouver des valeurs en °C interprétables

---

## Statistiques de sortie

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

## Pour T5 : Point de synchronisation

Les fichiers à récupérer avant de commencer l'insertion en DB :

```
data/processed/cleaned_csv/patient_XX_cleaned.csv   (×20)  → table signals
data/processed/npy/patient_XX_windows_meta.csv      (×20)  → table windows
data/processed/all_windows_meta.csv                         → insertion combinée
data/processed/logs/etl_summary.json                        → paramètres de normalisation
```

Les colonnes `is_interpolated`, `temp_asymmetry` et `segment_id` doivent être **présentes dans les deux schémas DB** (PostgreSQL et TimescaleDB). Elles sont requises par le modèle Sprint 3.
