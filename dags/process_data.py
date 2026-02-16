import logging
import polars as pl
from pathlib import Path

# Configuration
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

ACTIVITY_FILE = DATA_DIR / "chembl_203_ic50_activity.parquet"
MOLECULE_FILE = DATA_DIR / "molecules_metadata.parquet"
OUTPUT_FILE = DATA_DIR / "dataset_full_processed.parquet"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

def process_data():
    logger.info("Starting data processing pipeline...")
    
    # Load Raw Data
    if not ACTIVITY_FILE.exists() or not MOLECULE_FILE.exists():
        logger.error("Input files missing. Ensure previous steps completed successfully.")
        return

    df_act = pl.read_parquet(ACTIVITY_FILE)
    df_mol = pl.read_parquet(MOLECULE_FILE)

    logger.info(f"Loaded Activities: {len(df_act)}, Molecules: {len(df_mol)}")

    # Extract Canonical SMILES from Nested Structure
    logger.info("Extracting Canonical SMILES...")
    
    if "molecule_structures" in df_mol.columns:
        # Check if column is struct or already flattened (edge case handling)
        try:
            df_mol = df_mol.with_columns(
                pl.col("molecule_structures")
                .struct.field("canonical_smiles")
                .alias("canonical_smiles")
            )
        except Exception as e:
             logger.warning(f"Complex structure extraction failed, attempting direct access: {e}")

    if "canonical_smiles" not in df_mol.columns:
        logger.error("Canonical SMILES could not be extracted. Aborting.")
        return

    # Select relevant molecule features
    cols_to_keep = ["molecule_chembl_id", "canonical_smiles", "molecule_properties"]
    existing_cols = [c for c in cols_to_keep if c in df_mol.columns]
    df_mol_clean = df_mol.select(existing_cols)

    # Join Activity with Structure
    logger.info("Joining Activity and Structure data...")
    df_final = df_act.join(df_mol_clean, on="molecule_chembl_id", how="inner")
    
    # Filter for Regression (Must have SMILES and Standard Value)
    before_count = len(df_final)
    df_final = df_final.filter(
        pl.col("canonical_smiles").is_not_null() & 
        pl.col("standard_value").is_not_null()
    )
    after_count = len(df_final)
    
    logger.info(f"Dropped {before_count - after_count} records due to missing SMILES or Values.")

    # Save Final Dataset
    df_final.write_parquet(OUTPUT_FILE)
    logger.info(f"SUCCESS. Processed dataset saved to: {OUTPUT_FILE} ({len(df_final)} rows)")

if __name__ == "__main__":
    process_data()