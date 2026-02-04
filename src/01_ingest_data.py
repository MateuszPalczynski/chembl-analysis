import os
import pandas as pd
from chembl_webresource_client.new_client import new_client
from datetime import datetime

# Configuration
OUTPUT_DIR = "/opt/spark/data"
RAW_FILE_NAME = "chembl_raw.parquet"
TARGET_PROTEIN = "CHEMBL203"  # Example: EGFR (Epidermal Growth Factor Receptor)
LIMIT = 2000  # Set to None for full download

def fetch_chembl_data(target_id: str, limit: int = 1000) -> pd.DataFrame:
    print(f"[{datetime.now()}] Fetching data for target: {target_id}...")
    
    activity = new_client.activity
    # Filter for valid IC50 measurements (standard for regression)
    query = activity.filter(target_chembl_id=target_id)\
                    .filter(standard_type="IC50")\
                    .filter(standard_value__isnull=False)\
                    .filter(standard_relation='=')\
                    .only('molecule_chembl_id', 'canonical_smiles', 'standard_value', 'standard_units')

    if limit:
        print(f"[{datetime.now()}] Limit applied: {limit} records.")
        results = query[:limit]
    else:
        results = query

    # Convert to list first (client returns lazy iterator)
    data_list = list(results)
    
    df = pd.DataFrame(data_list)
    print(f"[{datetime.now()}] Downloaded {len(df)} rows.")
    return df

def save_data(df: pd.DataFrame, output_path: str):
    print(f"[{datetime.now()}] Saving data to {output_path}...")
    df.to_parquet(output_path, index=False)
    print(f"[{datetime.now()}] Success. File saved.")

if __name__ == "__main__":
    try:
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # execution
        df = fetch_chembl_data(TARGET_PROTEIN, LIMIT)
        
        if not df.empty:
            full_path = os.path.join(OUTPUT_DIR, RAW_FILE_NAME)
            save_data(df, full_path)
        else:
            print("[WARNING] No data found for specified criteria.")
            
    except Exception as e:
        print(f"[ERROR] Pipeline failed: {e}")
        exit(1)