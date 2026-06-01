import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()

def test_connection():
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0.0,
        api_key=os.environ.get("GOOGLE_API_KEY")
    )
    
    response = llm.invoke("Acknowledge this connection test in one sentence.")
    print(response.content)

if __name__ == "__main__":
    test_connection()