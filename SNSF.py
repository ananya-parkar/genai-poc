import requests
import configparser
import re
import snowflake.connector
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
from datetime import datetime, timedelta
 
#Load configuration
config = configparser.ConfigParser()
config.read("config.ini")
SNOWFLAKE_CONFIG = {
    "user": config["snowflake"]["user"],
    "password": config["snowflake"]["password"],
    "account": config["snowflake"]["account"],
    "warehouse": config["snowflake"]["warehouse"],
    "database": config["snowflake"]["database"],
    "schema": config["snowflake"]["schema"],
    }
 
API_URL = "https://router.huggingface.co/together/v1/chat/completions"
API_KEY = config["huggingface"]["api_key"]
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
 
def clean_think_block(text):
    # Remove <think> tags
    text = re.sub(r'(?i)<\s*think\s*>.*?<\s*/\s*think\s*>', '', text, flags=re.DOTALL)
    text = re.sub(r'(?i)<\s*/?\s*think\s*>', '', text)
    # Remove markdown bold and headers
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
    return text.strip()
 
def select_llm():
    print("Choose the LLM to generate the solution:")
    print("1. DeepSeek-R1 (685B)")
    print("2. Qwen3 (235B)")
    print("3. ChatGLM (32B)")
 
    choice = input("Enter 1, 2, or 3: ").strip()
 
    if choice == "1":
        return {
            "model": "deepseek/deepseek-r1-turbo",
            "api_url": "https://router.huggingface.co/novita/v3/openai/chat/completions"
        }
    elif choice == "2":
        return {
            "model": "Qwen/Qwen3-235B-A22B",
            "api_url": "https://router.huggingface.co/nebius/v1/chat/completions"
        }
    elif choice == "3":
        return {
            "model": "thudm/glm-z1-32b-0414",
            "api_url": "https://router.huggingface.co/novita/v3/openai/chat/completions"
        }
    else:
        print("Invalid choice, defaulting to DeepSeek-R1.")
        return {
            "model": "deepseek/deepseek-r1-turbo",
            "api_url": "https://router.huggingface.co/novita/v3/openai/chat/completions"
        }
 
def create_ai_solutions_table():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS AI_SOLUTIONS (
    number STRING PRIMARY KEY,
    priority STRING,
    service STRING,
    opened_at TIMESTAMP,
    short_description STRING,
    customer_comments STRING,
    generated_rca STRING,
    "deepseek_solution" STRING,
    "qwen_solution" STRING,
    "glm_solution" STRING
    );
    """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    print("AI_SOLUTIONS table checked/created successfully.")
 
 
def fetch_latest_unprocessed_alert():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = """
            SELECT number, caller, category, subcategory, service, service_offering,
            configuration_item, channel, state, impact, urgency, priority, assignment_group,
            assigned_to, short_description, description, opened_at, processed
            FROM alerts_data
            WHERE rca_processed = TRUE AND number NOT IN (SELECT number FROM AI_SOLUTIONS)
            ORDER BY opened_at DESC LIMIT 1;
            """
    cursor.execute(query)
    row = cursor.fetchone()
    colnames = [desc[0].lower() for desc in cursor.description]
    cursor.close()
    conn.close()
    return dict(zip(colnames, row)) if row else None
 
def is_incident_already_processed(incident_number):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = "SELECT 1 FROM AI_SOLUTIONS WHERE number = %s LIMIT 1"
    cursor.execute(query, (incident_number,))
    exists = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return exists
 
def fetch_generated_rca(incident_number):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = "SELECT rca FROM rca_results WHERE number = %s"
    cursor.execute(query, (incident_number,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else ""
 
def generate_solution(alert_data, similar_solution, llm, user_input=""):
    if not alert_data:
        return "No alert data provided."
 
    prompt = (
        f"{user_input.strip()}\n\n"
        f"Incident Details:\n"
        f"Number: {alert_data.get('number', 'N/A')}\n"
        f"Short Description: {alert_data.get('short_description', 'N/A')}\n"
        f"Description: {alert_data.get('description', 'N/A')}\n"
        f"Priority: {alert_data.get('priority', 'N/A')}\n"
        f"Business Service: {alert_data.get('business_service', 'N/A')}\n"
        f"Customer Comments: {alert_data.get('customer_comments', 'N/A')}\n"
    )
    if similar_solution:
        prompt += f"\nSimilar Solution:\n{similar_solution.strip()}\n"
 
    try:
        response = requests.post(
            llm["api_url"],
            headers=HEADERS,
            json={
                "model": llm["model"],
                "messages": [{"role": "user", "content": prompt}]
            }
        )
        response.raise_for_status()
        result = response.json()
        message = result["choices"][0]["message"]["content"]
       
        cleaned_message = clean_think_block(message.strip())
        print(f"Final RCA:\n{cleaned_message}")
       
        return cleaned_message
    except Exception as e:
        print(f"Error during RCA generation: {e}")
        return None
 
def store_ai_solution(alert_data, ai_solution, generated_rca, model_name):
    column_map = {
        "deepseek/deepseek-r1-turbo": "DEEPSEEK_SOLUTION",
        "Qwen/Qwen3-235B-A22B": "QWEN_SOLUTION",
        "thudm/glm-z1-32b-0414": "GLM_SOLUTION"
    }
 
    if model_name not in column_map:
        raise ValueError(f"Model name '{model_name}' not available/specified properly.")
 
    target_column = column_map[model_name]
 
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = f"""
    INSERT INTO AI_SOLUTIONS (
        number, priority, service, opened_at, short_description,
        customer_comments, generated_rca, {target_column}
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        alert_data['number'],
        alert_data.get('priority'),
        alert_data.get('service'),
        alert_data.get('opened_at'),
        alert_data.get('short_description'),
        alert_data.get('customer_comments', ""),
        generated_rca,
        ai_solution
    )
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"\nStored AI solution for Incident {alert_data['number']} in column {target_column}")
 
 
def mark_alert_processed(alert_number):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = "UPDATE alerts_data SET processed = TRUE WHERE number = %s"
    cursor.execute(query, (alert_number,))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Marked alert {alert_number} as processed.")
 
def load_existing_embeddings(model_name):
    column_map = {
        "deepseek/deepseek-r1-turbo": "deepseek_solution",
        "Qwen/Qwen3-235B-A22B": "qwen_solution",
        "thudm/glm-z1-32b-0414": "glm_solution"
    }
 
    if model_name not in column_map:
        raise ValueError(f"Model name '{model_name}' not available/specified properly.")
 
    target_column = column_map[model_name]
 
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = f"""
        SELECT number, short_description, {target_column}, opened_at
        FROM AI_SOLUTIONS
        WHERE {target_column} IS NOT NULL
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
 
    if not rows:
        return [], [], [], []
 
    incident_ids = [row[0] for row in rows]
    descriptions = [row[1] or "" for row in rows]
    solutions = [row[2] for row in rows]
    opened_dates = [row[3] for row in rows]
    return incident_ids, descriptions, solutions, opened_dates
 
def build_faiss_index(descriptions):
    embeddings = embedding_model.encode(descriptions, convert_to_numpy=True)
    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(embeddings)
    return index
 
def check_for_similar_alert(new_description, index, solutions, opened_dates, threshold=0.85):
    if not new_description or index is None or not solutions:
        return None
    embedding = embedding_model.encode([new_description], convert_to_numpy=True)
    D, I = index.search(embedding, k=1)
    similarity = 1 / (1 + D[0][0])
    idx = I[0][0]
    if similarity >= threshold and idx < len(solutions):
        solution_date = opened_dates[idx]
        if solution_date and datetime.utcnow() - solution_date <= timedelta(days=60):
            return solutions[idx]
    return None
 
def main(incident_number=None):
    llm = select_llm()
    model_name = llm["model"]
    conn = cur = None
    try:
        if not incident_number:
            alert_data = fetch_latest_unprocessed_alert()
            if not alert_data:
                print("No unprocessed alert found.")
                return
            incident_number = alert_data['number']
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT * FROM alerts_data WHERE number = %s", (incident_number,))
        alert = cur.fetchone()
        if not alert:
            print(f"No alert found for incident {incident_number}.")
            return
        colnames = [desc[0].lower() for desc in cur.description]
        alert_data = dict(zip(colnames, alert))
        print(f"Processing incident {incident_number}")
 
        user_input=input("Enter your query or additional details for solution generation:").strip()
        incident_ids, descriptions, solutions, opened_dates = load_existing_embeddings(model_name)
        index = build_faiss_index(descriptions) if descriptions else None
        similar_solution = check_for_similar_alert(
            alert_data.get("short_description", ""), index, solutions, opened_dates
        )
        generated_rca = fetch_generated_rca(incident_number)
       
        if generated_rca:
            generated_rca = clean_think_block(generated_rca.strip())
 
        full_prompt = user_input + "\n\n" + generated_rca if generated_rca else user_input
       
        ai_solution = generate_solution(alert_data, similar_solution, llm, user_input = full_prompt)
       
        print("\nGenerated Solution:\n")
        print(ai_solution)
       
        store_ai_solution(alert_data, ai_solution, generated_rca, model_name)
        mark_alert_processed(incident_number)
       
        print(f"Processed solutions for Incident {incident_number}")
       
        conn.commit()
   
    except snowflake.connector.errors.Error as e:
        print(f"Snowflake error: {e}")
   
    except Exception as e:
        print(f"Unexpected error: {e}")
   
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
 
if __name__ == "__main__":
    main()
 