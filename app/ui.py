import streamlit as st
from rdkit import Chem
from rdkit.Chem import Draw
from agent import run_agent

st.set_page_config(page_title="ChemAnalyzer MVP", layout="wide")
st.title("Molecular Activity Analyzer (GNN + LLM)")

smiles_input = st.text_input("SMILES:", "CC(=O)OC1=CC=CC=C1C(=O)O")

if st.button("Run Agent"):
    mol = Chem.MolFromSmiles(smiles_input)
    
    if mol is None:
        st.error("Invalid SMILES format.")
    else:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Structure 2D")
            img = Draw.MolToImage(mol)
            st.image(img)
            
        with col2:
            st.subheader("LLM Agent Analysis")
            with st.spinner("Orchestrating tools..."):
                try:
                    result = run_agent(smiles_input)
                    st.markdown(result)
                except Exception as e:
                    st.error(f"Agent execution failed: {e}")