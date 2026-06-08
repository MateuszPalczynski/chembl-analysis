## Project Overview

This project implements an end-to-end **Data Engineering and Machine Learning pipeline** for Computational Drug Discovery (QSAR). It automates the extraction, transformation, and versioning of bioactivity data from the **ChEMBL database**, trains a state-of-the-art **Graph Isomorphism Network (GIN)**, and deploys it behind an interactive **LLM-based Agent** for explainable molecular analysis.

The architecture follows a **Hybrid Data Stack** approach combined with **Modern MLOps**:
* **Orchestration:** Airflow manages the workflow DAGs.
* **ETL & EDA:** **Polars** is used for high-performance processing of intermediate datasets.
* **Scalability:** **PySpark** is configured for distributed training and handling large-scale chemical spaces.
* **Machine Learning:** **PyTorch Geometric** powers the Graph Neural Network (GIN) for predictive modeling.
* **Explainability (UI):** **Streamlit** combined with **LangGraph** (Gemini 2.5 Flash) orchestrates tools and explains predictions in plain language.
* **Reproducibility:** **MLflow** tracks dataset versions, schemas, hyperparameters, and model artifacts.

## Architecture & Tech Stack

The system is fully containerized using Docker Compose.

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Orchestrator** | Apache Airflow | Schedules tasks and manages dependencies. |
| **Compute Engine** | PySpark (Master/Worker) | Distributed processing for heavy workloads. |
| **Fast ETL** | Polars | Blazing fast data manipulation and aggregations. |
| **Experiment Tracking** | MLflow | Versions datasets (Silver/Gold) and logs model artifacts. |
| **Cheminformatics** | RDKit | Chemical structure handling, fingerprint generation, and scaffold analysis. |
| **Deep Learning** | PyTorch / PyG | Architecture and training of the Graph Isomorphism Network (GINAdvanced). |
| **Agentic Framework** | LangChain / LangGraph | Orchestrates LLM tool calling (GNN inference + RDKit physchem calculation). |
| **User Interface** | Streamlit + Plotly | Interactive dashboard for molecular drawing, agent trace inspection, and prediction visualization. |

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
* **4. Advanced Predictive Modeling (GNN)**
    * **Graph Architecture:** Custom **GINAdvanced** network with multiple `GINEConv` layers, `BatchNorm1d`, and Dropout.
    * **Global Features Injection:** Physicochemical properties (MW, LogP, PSA, HBA, HBD, QED) and ECFP fingerprints are seamlessly concatenated with graph embeddings (Mean, Max, Add pooling) to enhance model generalization.
    * **Scaffold Split Validation:** Rigorous evaluation on held-out Murcko scaffolds to ensure the model learns generalized chemical patterns, not just localized similarities.
* **5. Explainable AI & Tool Orchestration (LLM Agent)**
    * **Interactive Tool Calling:** An autonomous LLM agent dynamically invokes Python tools in the background to calculate molecular properties and run the GNN model inference.
    * **Chemical Interpretation:** The agent synthesizes raw numerical outputs (e.g., $pIC_{50}$ values, molecular weights) into a cohesive, professional chemical interpretation.
    * **Robust Error Handling:** The system elegantly handles invalid SMILES formats and tensor mismatch errors without breaking the application state.

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
* **Deployment Layer (Inference & UI)**
    * **Real-time Featurization:** Converts input SMILES strings into PyTorch `Data` objects on the fly.
    * **Agentic Trace:** UI allows users to peek under the hood and view the exact sequence of tools called by the LLM.
    * **Model Mismatch Analysis:** A dedicated dashboard visualizes the Parity Plot ($True$ vs $Predicted \ pIC_{50}$) and identifies problematic molecular scaffolds to ensure full transparency of the model's limitations.

---

## Model Performance & Evaluation

The final `GINAdvanced` model demonstrated strong predictive capabilities on the stringent **Scaffold Split** test set, proving its ability to generalize to novel chemical spaces.

* **Test $R^2$:** 0.528
* **Test MSE:** 0.520
* **Test MAE:** 0.534

### Mismatch Analysis (Error Localization)
A dedicated script evaluates the model's residuals (absolute error) grouped by molecular scaffold. This analysis helps identify which chemical cores the model struggles with, providing actionable insights for future data collection or feature engineering.

---

## Example EDA Results

The pipeline automatically generates insights into the chemical data:

* **Hit Discovery:** Automatic identification of high-potency compounds based on pIC50 distribution thresholds.
* **Correlation Heatmaps:** Analysis of relationships between molecular weight, lipophilicity, and activity.
* **Missingness Maps:** Visual auditing of data quality before training.

## Quick Start (UI Application)

1. Ensure the final model weights (`best_final_gin.pt`) are located in the `app` directory.
2. Provide your API key for the LLM in a `.env` file (`GOOGLE_API_KEY=your_key`).
3. Run the Streamlit application `streamlit run app/ui.py`
