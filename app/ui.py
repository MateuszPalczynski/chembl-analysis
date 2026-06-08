import shap
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from rdkit.Chem import rdMolDescriptors
import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from rdkit import Chem
from rdkit.Chem import Draw
from agent import run_agent
from sklearn.metrics import r2_score, mean_absolute_error
from rdkit.Chem import Descriptors

st.set_page_config(page_title="ChemAnalyzer MVP", layout="wide")
st.title("Molecular Activity Analyzer (GNN + LLM)")

tab1, tab2 = st.tabs(["Agent Analysis", "Mismatch Analysis"])

@st.cache_data
def load_enriched_data(csv_path):
    df = pd.read_csv(csv_path)
    df['residual'] = abs(df['y_true'] - df['y_pred'])
    
    def safe_mw(smi):
        m = Chem.MolFromSmiles(smi)
        return Descriptors.MolWt(m) if m else 0.0
        
    def safe_logp(smi):
        m = Chem.MolFromSmiles(smi)
        return Descriptors.MolLogP(m) if m else 0.0
        
    df['MW'] = df['canonical_smiles'].apply(safe_mw)
    df['LogP'] = df['canonical_smiles'].apply(safe_logp)
    return df

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
    st.write("Interactive error analysis focusing on chemical properties and activity cliffs.")
    
    csv_path = "final_test_predictions.csv"
    if os.path.exists(csv_path):
        df = load_enriched_data(csv_path)
        
        r2 = r2_score(df['y_true'], df['y_pred'])
        mae = mean_absolute_error(df['y_true'], df['y_pred'])
        
        col_m1, col_m2 = st.columns(2)
        col_m1.metric(label="Test R²", value=f"{r2:.3f}")
        col_m2.metric(label="Test MAE", value=f"{mae:.3f}")
        
        st.markdown("---")
        
        st.subheader("1. General Parity Plot")
        fig_parity = px.scatter(
            df, x="y_true", y="y_pred", color="residual",
            hover_data=["molecule_chembl_id", "canonical_smiles", "scaffold"],
            color_continuous_scale="RdBu_r",
            labels={"y_true": "True pIC50", "y_pred": "Predicted pIC50", "residual": "Absolute Error"}
        )
        fig_parity.add_shape(
            type="line", line=dict(dash='dash', color="red"),
            x0=df['y_true'].min(), y0=df['y_true'].min(),
            x1=df['y_true'].max(), y1=df['y_true'].max()
        )
        st.plotly_chart(fig_parity, use_container_width=True)

        st.markdown("---")
        st.subheader("2. Error Profiling (Physicochemical Properties)")
        
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            fig_mw = px.scatter(
                df, x="MW", y="residual", opacity=0.6,
                hover_data=["canonical_smiles"], trendline="ols",
                labels={"MW": "Molecular Weight", "residual": "Absolute Error"},
                title="Error vs Molecular Weight"
            )
            st.plotly_chart(fig_mw, use_container_width=True)
            
        with col_chart2:
            fig_logp = px.scatter(
                df, x="LogP", y="residual", opacity=0.6,
                hover_data=["canonical_smiles"], trendline="ols",
                labels={"LogP": "LogP (Lipophilicity)", "residual": "Absolute Error"},
                title="Error vs LogP"
            )
            st.plotly_chart(fig_logp, use_container_width=True)

        st.markdown("---")
        st.subheader("3. Activity Cliffs Analysis")
        st.write("Scaffolds where slight structural changes cause massive pIC50 differences (>1.5 log units), leading to high model error.")
        
        cliff_df = df.groupby('scaffold').filter(lambda x: len(x) >= 2)
        scaffold_cliffs = cliff_df.groupby('scaffold').agg(
            true_pIC50_range=('y_true', lambda x: x.max() - x.min()),
            mean_error=('residual', 'mean'),
            molecule_count=('molecule_chembl_id', 'count')
        ).reset_index()
        
        cliffs_filtered = scaffold_cliffs[scaffold_cliffs['true_pIC50_range'] > 1.5].sort_values('mean_error', ascending=False)
        
        if not cliffs_filtered.empty:
            st.dataframe(cliffs_filtered.head(10), use_container_width=True)
        else:
            st.info("No significant activity cliffs found in this test set.")
            
        st.markdown("---")
        st.subheader("4. Explainable AI (Surrogate SHAP Analysis)")
        st.write("Analysis of the impact of global physicochemical features on the GNN model's predictions using a Surrogate Model XAI.")
        
        @st.cache_data
        def compute_shap_features(df_input):
            features = []
            for smi in df_input['canonical_smiles']:
                mol = Chem.MolFromSmiles(smi)
                if mol:
                    features.append([
                        Descriptors.MolWt(mol),
                        Descriptors.MolLogP(mol),
                        rdMolDescriptors.CalcNumHBD(mol),
                        rdMolDescriptors.CalcNumHBA(mol),
                        rdMolDescriptors.CalcTPSA(mol),
                        rdMolDescriptors.CalcNumRotatableBonds(mol)
                    ])
                else:
                    features.append([0]*6)
            return pd.DataFrame(features, columns=['MW', 'LogP', 'HBD', 'HBA', 'PSA', 'RotBonds'])

        with st.spinner("Generating SHAP values..."):
            X_shap = compute_shap_features(df)
            y_shap = df['y_pred'] 
            
            rf_surrogate = RandomForestRegressor(n_estimators=50, random_state=42)
            rf_surrogate.fit(X_shap, y_shap)
            
            explainer = shap.TreeExplainer(rf_surrogate)
            shap_values = explainer.shap_values(X_shap)
            
            fig_shap, ax_shap = plt.subplots(figsize=(10, 6))
            shap.summary_plot(shap_values, X_shap, show=False, plot_size=(10, 6))
            st.pyplot(fig_shap)
            
            st.info("The SHAP summary plot (Beeswarm) shows which features most strongly drive the GNN model's predicted pIC50 activity higher (red dots on the right) or lower (dots on the left).")
            
    else:
        st.warning("Missing `final_test_predictions.csv` file.")


    