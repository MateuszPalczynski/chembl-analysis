import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

def perform_mismatch_analysis(csv_path: str):
    # Wczytanie danych
    df = pd.read_csv(csv_path)
    
    # Mapowanie nazw kolumn z Twojego skryptu treningowego (y_true -> pIC50, y_pred -> pIC50_pred)
    if 'y_true' in df.columns and 'y_pred' in df.columns:
        df = df.rename(columns={'y_true': 'pIC50', 'y_pred': 'pIC50_pred'})
    elif 'pIC50_pred' not in df.columns:
        raise ValueError("Brak odpowiednich kolumn z predykcjami w pliku CSV.")
    
    # Obliczenie reszt
    df['residual'] = np.abs(df['pIC50'] - df['pIC50_pred'])
    
    # Metryki
    r2 = r2_score(df['pIC50'], df['pIC50_pred'])
    mae = mean_absolute_error(df['pIC50'], df['pIC50_pred'])
    rmse = np.sqrt(mean_squared_error(df['pIC50'], df['pIC50_pred']))
    
    print("=== RAPORT METRYK ===")
    print(f"R2: {r2:.3f} | MAE: {mae:.3f} | RMSE: {rmse:.3f}\n")
    
    # Najgorsze predykcje (zabezpieczenie na wypadek braku kolumn z ID/Scaffold)
    display_cols = ['pIC50', 'pIC50_pred', 'residual']
    if 'molecule_chembl_id' in df.columns: display_cols.insert(0, 'molecule_chembl_id')
    if 'scaffold' in df.columns: display_cols.append('scaffold')
        
    worst_predictions = df.nlargest(10, 'residual')
    print("=== TOP 10 NAJGORSZYCH PREDYKCJI ===")
    print(worst_predictions[display_cols])
    
    # Analiza rusztowań (jeśli dostępne)
    if 'scaffold' in df.columns:
        worst_scaffolds = df.groupby('scaffold')['residual'].mean().sort_values(ascending=False).head(5)
        print("\n=== TOP 5 NAJGORSZYCH RUSZTOWAŃ (ŚREDNI BŁĄD) ===")
        print(worst_scaffolds)
    
    # Generowanie wykresu
    plt.figure(figsize=(8, 6))
    plt.scatter(df['pIC50'], df['pIC50_pred'], alpha=0.5, edgecolor='k', c=df['residual'], cmap='coolwarm')
    plt.plot([df['pIC50'].min(), df['pIC50'].max()], [df['pIC50'].min(), df['pIC50'].max()], 'r--', lw=2)
    plt.colorbar(label='Błąd bezwzględny (Residual)')
    plt.xlabel('Rzeczywiste pIC50')
    plt.ylabel('Przewidywane pIC50')
    plt.title(f'Analiza błędów (Mismatch Analysis) - R2: {r2:.2f}')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('mismatch_analysis.png', dpi=300)
    print("\nZapisano wykres do pliku: mismatch_analysis.png")

if __name__ == "__main__":
    csv_file = "final_test_predictions.csv" 
    perform_mismatch_analysis(csv_file)