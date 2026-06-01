import os
from pathlib import Path
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import SystemMessage, HumanMessage
from tools import calculate_physchem_properties, predict_gnn_activity

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def run_agent(smiles: str) -> list:
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0.0,
        api_key=os.environ.get("GOOGLE_API_KEY")
    )

    tools = [calculate_physchem_properties, predict_gnn_activity]

    system_prompt = """Jesteś ekspertem ds. chemii informatycznej. Twoim zadaniem jest ocena potencjalnej aktywności biologicznej cząsteczek na podstawie ich struktury SMILES.
    
    Dla każdej podanej cząsteczki MUSISZ wykonać następujące kroki:
    1. Użyj narzędzia do obliczenia właściwości fizykochemicznych (masa, LogP).
    2. Użyj narzędzia modelu GNN (klasyfikatora).
    3. Połącz te informacje w krótką, profesjonalną interpretację chemiczną w języku polskim.
    
    Jeśli narzędzie zwróci błąd, poinformuj o tym użytkownika i przerwij analizę."""

    agent_executor = create_react_agent(llm, tools)

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"Przeanalizuj cząsteczkę o SMILES: {smiles}")
    ]
    
    response = agent_executor.invoke({"messages": messages})
    
    return response.get("messages", [])