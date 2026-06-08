import os
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import GINEConv, global_mean_pool, global_max_pool, global_add_pool
from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors, QED, AllChem
from langchain_core.tools import tool

NODE_DIM = 38
EDGE_DIM = 5
GLOBAL_DIM = 72
HIDDEN_DIM = 512
NUM_LAYERS = 4
DROPOUT = 0.1

def one_hot_encoding(value, choices):
    enc = [0] * (len(choices) + 1)
    idx = choices.index(value) if value in choices else -1
    enc[idx] = 1
    return enc

class GINAdvanced(nn.Module):
    def __init__(self, node_dim, edge_dim, global_dim, hidden_dim=128, num_layers=3, dropout=0.2):
        super().__init__()
        self.node_emb = nn.Linear(node_dim, hidden_dim)
        self.edge_emb = nn.Linear(edge_dim, hidden_dim)
        self.convs = nn.ModuleList()
        self.bns = nn.ModuleList()
        for _ in range(num_layers):
            seq = nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim * 2),
                nn.BatchNorm1d(hidden_dim * 2),
                nn.ReLU(),
                nn.Linear(hidden_dim * 2, hidden_dim)
            )
            self.convs.append(GINEConv(seq))
            self.bns.append(nn.BatchNorm1d(hidden_dim))
        self.gbn = nn.BatchNorm1d(global_dim)
        self.mlp = nn.Sequential(
            nn.Linear(hidden_dim * 3 + global_dim, hidden_dim),
            nn.BatchNorm1d(hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.BatchNorm1d(hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, 1)
        )

    def forward(self, x, ei, ea, batch, gf):
        x = self.node_emb(x)
        ea = self.edge_emb(ea)
        for conv, bn in zip(self.convs, self.bns):
            x = F.relu(bn(conv(x, ei, ea) + x))
        xm = global_mean_pool(x, batch)
        xa = global_max_pool(x, batch)
        xs = global_add_pool(x, batch)
        return self.mlp(torch.cat([xm, xa, xs, self.gbn(gf)], 1))

model = None
try:
    weights_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "best_final_gin.pt"))
    
    _temp_model = GINAdvanced(
        node_dim=NODE_DIM,
        edge_dim=EDGE_DIM,
        global_dim=GLOBAL_DIM,
        hidden_dim=HIDDEN_DIM,
        num_layers=NUM_LAYERS,
        dropout=DROPOUT
    )
    
    _temp_model.load_state_dict(torch.load(weights_path, map_location=torch.device('cpu'), weights_only=True))
    _temp_model.eval()
    
    model = _temp_model

except Exception as e:
    print(f"Failed to load model: {e}")

def compute_g_feats(mol):
    aromatic_rings = rdMolDescriptors.CalcNumAromaticRings(mol)
    hbd = rdMolDescriptors.CalcNumHBD(mol)
    alogp = Descriptors.MolLogP(mol)
    mw = Descriptors.MolWt(mol)
    rtb = rdMolDescriptors.CalcNumRotatableBonds(mol)
    psa = rdMolDescriptors.CalcTPSA(mol)
    hba = rdMolDescriptors.CalcNumHBA(mol)
    try:
        qed_weighted = QED.qed(mol)
    except Exception:
        qed_weighted = 0.5
    
    g_base = [aromatic_rings, hbd, alogp, mw / 500.0, rtb, psa / 100.0, hba, qed_weighted]
    fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=2048)
    ecfp = list(fp)[:64]
    return g_base + ecfp

def get_graph_data(smiles):
    mol = Chem.MolFromSmiles(smiles)
    if not mol:
        return None
    
    x_list = []
    for a in mol.GetAtoms():
        feat = (
            one_hot_encoding(a.GetAtomicNum(), [1, 6, 7, 8, 9, 15, 16, 17, 35, 53]) +
            one_hot_encoding(a.GetDegree(), [0, 1, 2, 3, 4, 5]) +
            one_hot_encoding(a.GetTotalNumHs(), [0, 1, 2, 3, 4]) +
            one_hot_encoding(int(a.GetHybridization()), [2, 3, 4, 5]) +
            one_hot_encoding(int(a.GetChiralTag()), [0, 1, 2, 3]) +
            [a.GetIsAromatic(), a.IsInRing(), a.GetFormalCharge(), a.GetMass() / 100.0]
        )
        x_list.append(feat)
    x = torch.tensor(x_list, dtype=torch.float)
    
    edges, attrs = [], []
    for b in mol.GetBonds():
        i, j = b.GetBeginAtomIdx(), b.GetEndAtomIdx()
        f = one_hot_encoding(b.GetBondType(), [Chem.rdchem.BondType.SINGLE, Chem.rdchem.BondType.DOUBLE, Chem.rdchem.BondType.TRIPLE, Chem.rdchem.BondType.AROMATIC])
        edges += [(i, j), (j, i)]
        attrs += [f, f]
        
    edge_index = torch.tensor(edges).t().contiguous() if edges else torch.empty((2, 0), dtype=torch.long)
    edge_attr = torch.tensor(attrs, dtype=torch.float) if attrs else torch.empty((0, 5))
    
    g_feats = compute_g_feats(mol)
    global_feats = torch.tensor([g_feats], dtype=torch.float)
    batch = torch.zeros(x.size(0), dtype=torch.long)
    
    return x, edge_index, edge_attr, batch, global_feats

tool
def calculate_physchem_properties(smiles: str) -> dict:
    """
    Calculates physicochemical properties (molecular weight and LogP) for a given SMILES string.
    Use this tool to check the chemical parameters of a molecule.
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
    Predicts the pIC50 activity value for a given SMILES using the trained GIN model.
    Always use this tool to determine the predicted biological potency of the molecule.
    """
    if model is None:
        return {"error": "Model weights not found or failed to load. Check server logs."}

    data = get_graph_data(smiles)
    if data is None:
        return {"error": "Invalid SMILES format or featurization failed"}
    
    x, edge_index, edge_attr, batch, global_feats = data
    
    model.eval()
    
    try:
        with torch.no_grad():
            pred = model(x, edge_index, edge_attr, batch, global_feats).item()
    except ValueError as e:
        if "Expected more than 1 value" in str(e):
            x_2 = torch.cat([x, x], dim=0)
            edge_index_2 = torch.cat([edge_index, edge_index + x.size(0)], dim=1)
            edge_attr_2 = torch.cat([edge_attr, edge_attr], dim=0)
            batch_2 = torch.cat([batch, batch + 1], dim=0)
            global_feats_2 = torch.cat([global_feats, global_feats], dim=0)
            
            with torch.no_grad():
                pred_2 = model(x_2, edge_index_2, edge_attr_2, batch_2, global_feats_2)
                pred = pred_2[0].item()
        else:
            return {"error": f"ValueError: {e}"}
    except Exception as e:
        return {"error": f"Prediction error: {e}"}
    
    return {
        "pIC50": round(pred, 3),
        "model_version": "GINAdvanced_v2"
    }