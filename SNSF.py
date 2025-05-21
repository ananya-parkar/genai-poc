import requests
import configparser
import re
import snowflake.connector
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
from datetime import datetime, timedelta
 
# Load configuration
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
 
API_URL = "https://router.huggingface.co/novita/v3/openai/chat/completions"
API_KEY = config["huggingface"]["api_key"]
 
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
 
 
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
            ai_generated_solution STRING
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
        SELECT number, caller, category, subcategory, service, service_offering, configuration_item, channel,
               state, impact, urgency, priority, assignment_group, assigned_to, short_description, description,
               opened_at, processed
        FROM alerts_data
        WHERE rca_processed = TRUE AND number NOT IN (SELECT number FROM AI_SOLUTIONS)
        ORDER BY opened_at DESC
        LIMIT 1;
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
 
def generate_solution(alert_data, similar_solution):
    if not alert_data:
        return "No new alerts found."
 
    short_desc = alert_data.get('short_description') or "No description provided."
    description = alert_data.get('description') or "No detailed description."
 
    formatted_alert = f"""
        Incident ID: {alert_data['number']}
        Caller: {alert_data.get('caller', 'N/A')}
        Category: {alert_data.get('category', 'N/A')}
        Subcategory: {alert_data.get('subcategory', 'N/A')}
        Service: {alert_data.get('service', 'N/A')}
        Service Offering: {alert_data.get('service_offering', 'N/A')}
        Configuration Item: {alert_data.get('configuration_item', 'N/A')}
        Channel: {alert_data.get('channel', 'N/A')}
        State: {alert_data.get('state', 'N/A')}
        Impact: {alert_data.get('impact', 'N/A')}
        Urgency: {alert_data.get('urgency', 'N/A')}
        Priority: {alert_data.get('priority', 'N/A')}
        Assignment Group: {alert_data.get('assignment_group', 'N/A')}
        Assigned To: {alert_data.get('assigned_to', 'N/A')}
        Short Description: {short_desc}
        Description: {description}
        Opened At: {alert_data.get('opened_at', 'N/A')}
    """
 
    # Structured prompt
    base_prompt = (
        "You are a cross-platform database and infrastructure troubleshooting expert. "
        "Based on the following incident or log details, provide a detailed solution using this format:\n\n"
        "1. Issue Summary: \n"
        "- Describe the main issue in 1-2 sentences.\n\n"
        "2. Remediation Steps: \n"
        "- Provide a detailed, step-by-step resolution plan. Use SQL or CLI commands where appropriate.\n\n"
        "3. Verification: \n"
        "- Explain how to validate that the issue has been resolved.\n\n"
        "4. Generalization: \n"
        "- Suggest how the solution can be adapted to similar systems.\n\n"
    )
 
    if similar_solution:
        prompt = f"{base_prompt}Incident Details:\n{formatted_alert}\nSimilar Solution:\n{similar_solution}"
    else:
        prompt = f"{base_prompt}Incident Details:\n{formatted_alert}"
 
    payload = {
        "model": "deepseek/deepseek-prover-v2-671b",
        "messages": [{"role": "user", "content": prompt}]
    }
 
    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload)
        response.raise_for_status()
        result = response.json()
        message = result["choices"][0]["message"]["content"]
 
        # Clean formatting
        message = re.sub(r"\*\*(.*?)\*\*", r"\1", message)
        message = re.sub(r"#+\s*", "", message)
 
        return message.strip()
    except Exception as e:
        print(f"Error: {e}")
        return "API request failed."
 
 
def store_ai_solution(alert_data, ai_solution):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = """
        INSERT INTO AI_SOLUTIONS (number, priority, service, opened_at, short_description, ai_generated_solution)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = (
        alert_data['number'],
        alert_data.get('priority'),
        alert_data.get('service'),
        alert_data.get('opened_at'),
        alert_data.get('short_description'),
        ai_solution
    )
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"\nStored AI solution for Incident {alert_data['number']}")
 
 
def mark_alert_processed(alert_number):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = "UPDATE alerts_data SET processed = TRUE WHERE number = %s"
    cursor.execute(query, (alert_number,))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Marked alert {alert_number} as processed.")
 
 
def load_existing_embeddings():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = "SELECT number, short_description, ai_generated_solution, opened_at FROM AI_SOLUTIONS;"
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
 
 
def check_for_similar_alert(new_description, index, solutions, threshold=0.85):
    if not new_description or index is None or not solutions:
        return None
 
    embedding = embedding_model.encode([new_description], convert_to_numpy=True)
    D, I = index.search(embedding, k=1)
    similarity = 1 / (1 + D[0][0])
    if similarity >= threshold:
        idx = I[0][0]
        if idx < len(solutions):
            return solutions[idx]
    return None
 

def main(incident_number):
    conn = cur = None
    try:
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

        incident_ids, descriptions, solutions, _ = load_existing_embeddings()
        index = build_faiss_index(descriptions) if descriptions else None
        similar_solution = check_for_similar_alert(alert_data.get("short_description",""), index, solutions)
        ai_solution = generate_solution(alert_data, similar_solution)

        print("\n Generated Solution:\n")
        print(ai_solution)

        store_ai_solution(alert_data, ai_solution)
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