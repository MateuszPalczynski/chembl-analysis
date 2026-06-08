from pathlib import Path
import polars as pl
import sys
from tools import predict_gnn_activity

GOLD_DIR = Path(r"C:\Users\User\Desktop\spark_airflow\chembl\dags\data\gold")
DATA_PATH = GOLD_DIR / "version_v2.0_20260606_1226"

def generate_test_predictions(parquet_path: Path, output_csv: Path):
    df = pl.read_parquet(parquet_path)
    results = []

    print("Rozpoczynam inferencję na zbiorze testowym...")

    for i, row in enumerate(df.iter_rows(named=True)):
        smiles = row["canonical_smiles"]
        real_pic50 = row["pIC50"]
        scaffold = row.get("scaffold", "unknown")

        pred_result = predict_gnn_activity.invoke({"smiles": smiles})

        if "pIC50" in pred_result:
            results.append(
                {
                    "molecule_chembl_id": row.get("molecule_chembl_id", "unknown"),
                    "canonical_smiles": smiles,
                    "scaffold": scaffold,
                    "y_true": real_pic50,
                    "y_pred": pred_result["pIC50"],
                }
            )
        else:
            print(f"\n[BŁĄD KRYTYCZNY] Inferencja przerwana!")
            print(f"Cząsteczka: {smiles}")
            print(f"Odpowiedź narzędzia: {pred_result}")
            sys.exit(1)
            
        if i % 100 == 0 and i > 0:
            print(f"Przetworzono {i} cząsteczek...")

    out_df = pl.DataFrame(results)
    out_df.write_csv(output_csv)

if __name__ == "__main__":
    test_path = DATA_PATH / "scaffold_test.parquet"
    output_path = Path("final_test_predictions.csv")
    
    generate_test_predictions(test_path, output_path)
    print(f"\nZakończono sukcesem! Predictions saved to {output_path}")