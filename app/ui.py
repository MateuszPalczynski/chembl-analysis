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
        st.error("Nieprawidłowy format SMILES.")
    else:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Struktura 2D")
            img = Draw.MolToImage(mol)
            st.image(img)
            
        with col2:
            st.subheader("Analiza Agenta LLM")
            with st.spinner("Orkiestracja narzędzi..."):
                try:
                    result = run_agent(smiles_input)
                    
                    if isinstance(result, str):
                        st.markdown(result)
                    elif isinstance(result, list):
                        with st.expander("Ślad działania Agenta (Trace)", expanded=False):
                            for msg in result:
                                if hasattr(msg, "type") and msg.type == "ai" and hasattr(msg, "tool_calls") and msg.tool_calls:
                                    for tool_call in msg.tool_calls:
                                        st.markdown(f"🛠️ **Wywołanie narzędzia:** `{tool_call.get('name', 'unknown')}`")
                                        st.code(tool_call.get('args', ''))
                                elif hasattr(msg, "type") and msg.type == "tool":
                                    st.markdown("✅ **Wynik:**")
                                    st.code(msg.content)

                        final_msg = result[-1].content if result else ""
                        if isinstance(final_msg, list):
                            final_msg = final_msg[0].get("text", str(final_msg))
                        
                        st.markdown(final_msg)
                    else:
                        st.error("Unexpected return type from agent.")
                        
                except Exception as e:
                    st.error(f"Błąd wykonania Agenta: {e}")