import random
from langchain_core.tools import tool
from rdkit import Chem
from rdkit.Chem import Descriptors

@tool
def calculate_physchem_properties(smiles: str) -> dict:
    """
    Calculates physicochemical properties (molecular weight and LogP) for a given SMILES string.
    Use this tool when you need to check the chemical parameters of a molecule.
    """
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return {"error": "Invalid SMILES format"}
    
    mw = Descriptors.MolWt(mol)
    logp = Descriptors.MolLogP(mol)
    
    return {
        "molecular_weight": round(mw, 2),
        "logp": round(logp, 2)
    }

@tool
def predict_gnn_activity(smiles: str) -> dict:
    """
    Simulates the GNN model prediction. Predicts whether a given molecule (SMILES) 
    is biologically active or inactive. Returns the class and probability.
    Always use this to determine the activity of the molecule.
    """
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return {"error": "Invalid SMILES format"}
    
    prob = random.uniform(0.1, 0.9)
    is_active = bool(prob > 0.5)
    
    return {
        "is_active": is_active,
        "probability": round(prob, 2),
        "model_version": "GIN_mock_v1"
    }