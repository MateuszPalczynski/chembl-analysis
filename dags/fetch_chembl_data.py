import logging
import polars as pl
from chembl_webresource_client.new_client import new_client
from pathlib import Path

# Configuration
TARGET_CHEMBL_ID = "CHEMBL203"
ACTIVITY_TYPE = "IC50"
OUTPUT_FILE = "chembl_203_ic50_activity.parquet"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def fetch_activity_data(target_id: str, activity_type: str) -> pl.DataFrame:
    """
    Fetches activity data from ChEMBL API and returns a Polars DataFrame.
    """
    logger.info(f"Starting data fetch for Target: {target_id}, Type: {activity_type}")

    try:
        activity = new_client.activity
        query = activity.filter(target_chembl_id=target_id, standard_type=activity_type)
        
        # Execute query (lazy evaluation to list triggers the fetch)
        data = list(query)

        if not data:
            logger.warning("No data found for the given criteria.")
            return pl.DataFrame()

        df = pl.DataFrame(data, infer_schema_length=None)

        # Data type casting
        if "standard_value" in df.columns:
            df = df.with_columns(
                pl.col("standard_value").cast(pl.Float64, strict=False)
            )

        logger.info(f"Successfully fetched {len(df)} records.")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch data from ChEMBL API: {e}")
        return pl.DataFrame()

def save_to_parquet(df: pl.DataFrame, filepath: str) -> None:
    """
    Saves the DataFrame to a Parquet file if data exists.
    """
    if df.is_empty():
        logger.warning("DataFrame is empty. Skipping save.")
        return

    try:
        path = Path(filepath)
        df.write_parquet(path)
        logger.info(f"Data saved successfully to: {path.absolute()}")
    except Exception as e:
        logger.error(f"Failed to save Parquet file: {e}")

if __name__ == "__main__":
    df_activity = fetch_activity_data(TARGET_CHEMBL_ID, ACTIVITY_TYPE)
    save_to_parquet(df_activity, OUTPUT_FILE)