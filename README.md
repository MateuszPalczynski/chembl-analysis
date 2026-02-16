# ChEMBL Data Engineering & QSAR Pipeline

## Project Overview

This project implements an end-to-end **Data Engineering and Machine Learning pipeline** for Computational Drug Discovery (QSAR). It automates the extraction, transformation, and versioning of bioactivity data from the **ChEMBL database** to prepare high-quality datasets for predicting molecular activity (pIC50).

The architecture follows a **Hybrid Data Stack** approach:
* **Orchestration:** Airflow manages the workflow DAGs.
* **ETL & EDA:** **Polars** is used for high-performance, single-node processing of intermediate datasets.
* **Scalability:** **PySpark** is configured for distributed training and handling large-scale chemical spaces.
* **Reproducibility:** **MLflow** tracks dataset versions, schemas, and future model metrics.

## Architecture & Tech Stack

The system is fully containerized using Docker Compose.

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Orchestrator** | Apache Airflow | Schedules tasks and manages dependencies. |
| **Compute Engine** | PySpark (Master/Worker) | Distributed processing for heavy workloads. |
| **Fast ETL** | Polars | Blazing fast data manipulation and aggregations. |
| **Experiment Tracking** | MLflow | Versions datasets (Silver/Gold) and logs model artifacts. |
| **Cheminformatics** | RDKit | Chemical structure handling, fingerprint generation, and scaffold analysis. |

---

## Key Features

### 1. Domain-Specific Data Cleaning
* **Target Engineering:** Automatic conversion of raw IC50 ($nM$) to logarithmic **pIC50**.
* **Unit Standardization:** Strict filtering for standard units (`nM`) and relations (`=`).
* **Chemical Aggregation:** Handling experimental duplicates by aggregating results per unique molecule (Median aggregation to handle outliers).

### 2. Advanced Feature Engineering
* **Physicochemical Descriptors:** Calculation of **Lipinski's Rule of Five** parameters (MW, LogP, HBA, HBD) plus PSA and QED.
* **Structural Featurization:** Generation of **Morgan Fingerprints (ECFP4)** for ML models.
* **Scaffold Analysis:** Implementation of Scaffold Splits (Murcko Scaffolds) to prevent data leakage between train/test sets.

### 3. Robust EDA & Quality Control
* **Automated Inspections:** Statistical checks for missing values and constant columns.
* **Outlier Detection:** IQR-based detection for physical properties and "Hit" identification for high-potency compounds.
* **Chemical Space Visualization:** Dimensionality reduction (PCA) to visualize active vs. inactive clusters.

---

## Pipeline Stages (The "Medallion" Flow)

### Bronze Layer (Raw Ingestion)
* Fetches data from ChEMBL API based on Target ID.
* Stores raw Parquet with full metadata.

### Silver Layer (Cleaning & Filtering)
* **Polars ETL:**
    * Unpacks nested JSON structures.
    * Removes non-informative columns (high cardinality IDs, constant values).
    * Filters for specific assay conditions.
    * Calculates pIC50.

### Gold Layer (Aggregated & ML-Ready)
* **Aggregation:** Groups by `canonical_smiles` to ensure one row per molecule.
* **Imputation:** Strategies for missing physicochemical properties.
* **Versioning:** Final dataset is saved as `.parquet` and logged as an **MLflow Artifact**.


## Example EDA Results

The pipeline automatically generates insights into the chemical data:

* **Hit Discovery:** Automatic identification of high-potency compounds based on pIC50 distribution thresholds.
* **Correlation Heatmaps:** Analysis of relationships between molecular weight, lipophilicity, and activity.
* **Missingness Maps:** Visual auditing of data quality before training.

