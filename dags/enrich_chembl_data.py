import logging
import math
import os
from pathlib import Path
from typing import List

import polars as pl
from chembl_webresource_client.new_client import new_client
from tqdm import tqdm

# --- Configuration ---
BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"

INPUT_FILE = DATA_DIR / "chembl_203_ic50_activity.parquet"
OUTPUT_DIR = DATA_DIR
BATCH_SIZE = 50

# Resources to fetch
RESOURCES_TO_FETCH = {
    'molecule': 'molecules_metadata',       
    'mechanism': 'drug_mechanisms',        
    'compound_structural_alert': 'alerts'  
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_unique_molecule_ids(input_path: Path) -> List[str]:
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return []

    try:
        df = pl.read_parquet(input_path)
        if "molecule_chembl_id" not in df.columns:
            logger.error("Column 'molecule_chembl_id' is missing in the input file.")
            return []
        
        # Extract unique, non-null IDs
        unique_ids = df["molecule_chembl_id"].drop_nulls().unique().to_list()
        logger.info(f"Extracted {len(unique_ids)} unique molecule IDs.")
        return unique_ids
        
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        return []


def fetch_resource_batch(resource_name: str, molecule_ids: List[str]) -> pl.DataFrame:
    resource_client = getattr(new_client, resource_name)
    records = []
    
    total_batches = math.ceil(len(molecule_ids) / BATCH_SIZE)
    logger.info(f"Fetching '{resource_name}' data in {total_batches} batches...")

    for i in tqdm(range(0, len(molecule_ids), BATCH_SIZE), desc=f"Downloading {resource_name}", unit="batch"):
        batch_ids = molecule_ids[i : i + BATCH_SIZE]
        try:
            # Filter by molecule IDs
            query = resource_client.filter(molecule_chembl_id__in=batch_ids)
            # Fetch relevant columns only (optimization) serves as implicit projection
            records.extend(list(query))
        except Exception as e:
            logger.warning(f"Batch processing failed for {resource_name}: {e}")

    if not records:
        return pl.DataFrame()

    return pl.DataFrame(records, infer_schema_length=None)


def save_dataframe(df: pl.DataFrame, filename: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{filename}.parquet"
    
    try:
        df.write_parquet(output_path)
        logger.info(f"Successfully saved {len(df)} records to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")


if __name__ == "__main__":
    molecule_ids = get_unique_molecule_ids(INPUT_FILE)

    if molecule_ids:
        logger.info(f"Starting enrichment pipeline for {len(RESOURCES_TO_FETCH)} resources...")
        
        for resource_api_name, output_filename in RESOURCES_TO_FETCH.items():
            df_enriched = fetch_resource_batch(resource_api_name, molecule_ids)
            
            if not df_enriched.is_empty():
                save_dataframe(df_enriched, output_filename)
            else:
                logger.warning(f"No data returned for resource: {resource_api_name}")
                
        logger.info("Enrichment completed successfully.")
    else:
        logger.warning("No molecule IDs found to enrich. Exiting.")