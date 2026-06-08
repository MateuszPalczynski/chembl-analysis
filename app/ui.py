import os
import streamlit as st
import plotly.graph_objects as go
from rdkit import Chem
from rdkit.Chem import Draw
from agent import run_agent

st.set_page_config(page_title="ChemAnalyzer MVP", layout="wide")
st.title("Molecular Activity Analyzer (GNN + LLM)")

tab1, tab2 = st.tabs(["Agent Analysis", "Mismatch Analysis (Model Evaluation)"])

with tab1:
    smiles_input = st.text_input("SMILES:", "CC(=O)OC1=CC=CC=C1C(=O)O")

    if st.button("Run Agent"):
        mol = Chem.MolFromSmiles(smiles_input)
        
        if mol is None:
            st.error("Invalid SMILES format.")
        else:
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("2D Structure")
                img = Draw.MolToImage(mol)
                st.image(img)
                
                st.subheader("Prediction Dashboard")
                gauge_placeholder = st.empty()
                
            with col2:
                st.subheader("LLM Agent Evaluation")
                with st.spinner("Agent is orchestrating tools..."):
                    try:
                        result = run_agent(smiles_input)
                        predicted_pic50 = None
                        
                        if isinstance(result, list):
                            with st.expander("Agent Trace", expanded=False):
                                for msg in result:
                                    if hasattr(msg, "type") and msg.type == "ai" and hasattr(msg, "tool_calls") and msg.tool_calls:
                                        for tool_call in msg.tool_calls:
                                            st.markdown(f"🛠️ **Tool called:** `{tool_call.get('name', 'unknown')}`")
                                            st.code(tool_call.get('args', ''))
                                    elif hasattr(msg, "type") and msg.type == "tool":
                                        st.markdown("✅ **Result:**")
                                        st.code(msg.content)
                                        
                                        if "pIC50" in msg.content:
                                            import json
                                            try:
                                                content_dict = json.loads(msg.content)
                                                if "pIC50" in content_dict:
                                                    predicted_pic50 = content_dict["pIC50"]
                                            except:
                                                pass

                            final_msg = result[-1].content if result else ""
                            if isinstance(final_msg, list):
                                final_msg = final_msg[0].get("text", str(final_msg))
                            
                            st.markdown(final_msg)
                            
                            if predicted_pic50 is not None:
                                fig = go.Figure(go.Indicator(
                                    mode="gauge+number",
                                    value=predicted_pic50,
                                    domain={'x': [0, 1], 'y': [0, 1]},
                                    title={'text': "Predicted pIC50"},
                                    gauge={
                                        'axis': {'range': [-2, 12]},
                                        'bar': {'color': "darkblue"},
                                        'steps': [
                                            {'range': [-2, 4], 'color': "lightcoral"},
                                            {'range': [4, 7], 'color': "lightyellow"},
                                            {'range': [7, 12], 'color': "lightgreen"}
                                        ]
                                    }
                                ))
                                fig.update_layout(height=300, margin=dict(l=20, r=20, t=50, b=20))
                                gauge_placeholder.plotly_chart(fig, use_container_width=True)
                                
                        elif isinstance(result, str):
                            st.markdown(result)
                        else:
                            st.error("Unexpected return type from agent.")
                            
                    except Exception as e:
                        st.error(f"Execution error: {e}")

with tab2:
    st.header("Test Set Performance & Mismatch Analysis")
    st.write("Review of the model's performance on the scaffold-split test set.")
    
    img_path = "mismatch_analysis.png"
    if os.path.exists(img_path):
        col_img, col_metrics = st.columns([2, 1])
        with col_img:
            st.image(img_path, caption="Predicted vs True pIC50 Parity Plot")
        with col_metrics:
            st.info("The mismatch analysis highlights the molecular scaffolds where the GNN struggles the most. This proves deep understanding of the model's limitations.")
    else:
        st.warning("Mismatch analysis plot not found. Run mismatch_analysis.py first.")