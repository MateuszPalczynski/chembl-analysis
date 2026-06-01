import os
from pathlib import Path
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage
from tools import calculate_physchem_properties, predict_gnn_activity

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def run_agent(smiles: str):
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0.0,
        api_key=os.environ.get("GOOGLE_API_KEY")
    )

    tools = [calculate_physchem_properties, predict_gnn_activity]

    system_prompt = """Jesteś ekspertem ds. chemii informatycznej. Twoim zadaniem jest ocena potencjalnej aktywności biologicznej cząsteczek na podstawie ich struktury SMILES.
    
    Dla każdej podanej cząsteczki MUSISZ wykonać następujące kroki:
    1. Użyj narzędzia do obliczenia właściwości fizykochemicznych (masa, LogP), aby poznać podstawowe parametry.
    2. Użyj narzędzia modelu GNN (klasyfikatora), aby otrzymać przewidywaną aktywność i prawdopodobieństwo.
    3. Połącz te informacje w krótką, profesjonalną interpretację chemiczną w języku polskim. Zwróć uwagę na to, czy parametry (np. Reguła 5 Lipinskiego) mogą wpływać na aktywność.
    
    Jeśli narzędzie zwróci błąd, poinformuj o tym użytkownika i przerwij analizę."""

    agent_executor = create_agent(
        model=llm, 
        tools=tools, 
        system_prompt=system_prompt
    )

    user_message = f"Przeanalizuj cząsteczkę o SMILES: {smiles}"
    response = agent_executor.invoke({"messages": [HumanMessage(content=user_message)]})
    
    content = response["messages"][-1].content
    if isinstance(content, list):
        return content[0].get("text", str(content))
    return content

if __name__ == "__main__":
    test_smiles = "CC(=O)OC1=CC=CC=C1C(=O)O"
    print(f"Analyzing: {test_smiles}\n")
    
    result = run_agent(test_smiles)
    
    print("\n--- FINAL OUTPUT ---")
    print(result)