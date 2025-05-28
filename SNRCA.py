import requests
import snowflake.connector
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import re
import configparser
 
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

ALERTS_TABLE = "emailfetch.email_schema.alerts_data"
def clean_think_block(text):
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)      
    text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)  
    return text.strip()
 
def load_existing_solutions(cur):
    cur.execute("SELECT number, short_description, ai_generated_solution FROM AI_SOLUTIONS")
    rows = cur.fetchall()
    incident_ids = [row[0] for row in rows]
    descriptions = [row[1] if row[1] is not None else "" for row in rows]
    solutions = [row[2] if row[2] is not None else "" for row in rows]
    return incident_ids, descriptions, solutions
 
def build_faiss_index(descriptions):
    if not descriptions:
        return None
    embeddings = embedding_model.encode(descriptions, convert_to_numpy=True)
    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(embeddings)
    return index
 
def find_similar_solution(description, index, solutions, threshold=0.85):
    if not description or index is None or not solutions:
        return None
    embedding = embedding_model.encode([description], convert_to_numpy=True)
    D, I = index.search(embedding, k=1)
    similarity = 1 / (1 + D[0][0])
    if similarity >= threshold:
        idx = I[0][0]
        if idx < len(solutions):
            return solutions[idx]
    return None
 
def generate_root_cause_analysis(alert_data, similar_solution=None):
    if not alert_data:
        print("No alert data provided for RCA generation.")
        return None
 
    base_prompt = (
        "You are a cross-platform database and infrastructure log expert. "
        "Review the following log entry and generate a root cause analysis using the following structure:\n\n"
        "1. Detection of Issues:\n"
        "- List key symptoms or anomalies observed in the log.\n\n"
        "2. Root Cause Analysis:\n"
        "- Explain the most probable root causes of the issue, using technical insight derived from the log.\n\n"
        "3. Impact Assessment:\n"
        "- Briefly describe how the issue might affect systems, services, or users.\n\n"
        "4. Supporting Evidence:\n"
        "- Point out any log entries or technical signals that justify your analysis.\n"
    )
   
    prompt = (
        f"{base_prompt}\n\nIncident Details:\n"
        f"Number: {alert_data.get('number', 'N/A')}\n"
        f"Short Description: {alert_data.get('short_description', 'N/A')}\n"
        f"Description: {alert_data.get('description', 'N/A')}\n"
        f"Priority: {alert_data.get('priority', 'N/A')}\n"
        f"Business Service: {alert_data.get('business_service', 'N/A')}\n"
        f"Customer Comments: {alert_data.get('customer_comments', 'N/A')}\n"
    )
   
    if similar_solution:
        prompt += f"\nSimilar Solution:\n{similar_solution.strip()}\n"
   
    prompt += (
        "\nBased on the above, provide the complete structured root cause analysis as described. "
        "Focus on technical accuracy and clarity."
    )
   
    try:
        response = requests.post( API_URL, headers=HEADERS, json={
            "model": "deepseek-ai/DeepSeek-R1",
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
 
def store_rca_results(cur, alert_data, root_cause_analysis):
    if not alert_data or not root_cause_analysis:
        print("Missing alert data or RCA; skipping store.")
        return
 
    query = """
            INSERT INTO RCA_RESULTS (
            NUMBER, SHORT_DESCRIPTION, DESCRIPTION, PRIORITY, BUSINESS_SERVICE, RCA, CUSTOMER_COMMENTS
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
    values = (
        alert_data.get('number'),
        alert_data.get('short_description'),
        alert_data.get('description'),
        alert_data.get('priority'),
        alert_data.get('business_service'),
        root_cause_analysis,
        alert_data.get('customer_comments')
    )
   
    try:
        cur.execute(query, values)
        print(f"\nStored RCA for Incident {alert_data.get('number')}")
    except Exception as e:
        print(f"Error storing RCA results: {e}")
 
def fetch_most_recent_unprocessed_alert(cur, incident_number):
    query = f"""
            SELECT * FROM {ALERTS_TABLE}
            WHERE rca_processed = FALSE AND NUMBER = %s
            ORDER BY opened_at DESC LIMIT 1
            """
    cur.execute(query, (incident_number,))
    row = cur.fetchone()
    if not row:
        return None
   
    colnames = [desc[0].lower() for desc in cur.description]
    return dict(zip(colnames, row))
 
def mark_alert_processed(cur, incident_number):
    try:
        cur.execute( "UPDATE alerts_data SET rca_processed = TRUE WHERE number = %s", (incident_number,) )
        print(f"Marked alert {incident_number} as processed.")
    except Exception as e:
        print(f"Error marking alert processed: {e}")
 
def main(incident_number):
    conn = None
    cur = None
   
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
       
        alert = fetch_most_recent_unprocessed_alert(cur, incident_number)
       
        if not alert:
            print("No new alerts to process.")
            return
 
        print(f"Processing Incident: {alert.get('number', '<unknown>')}")
 
        incident_ids, descriptions, solutions = load_existing_solutions(cur)
 
        similar_solution = None
        if descriptions:
            index = build_faiss_index(descriptions)
            similar_solution = find_similar_solution(alert.get('short_description', ''), index, solutions)
   
        rca = generate_root_cause_analysis(alert, similar_solution)
   
        if rca:
            store_rca_results(cur, alert, rca)
            conn.commit()
        else:
            print("RCA generation failed; skipping storing results.")
   
        mark_alert_processed(cur, alert.get('number'))
        conn.commit()
   
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Hugging Face API: {e}")
    except snowflake.connector.errors.Error as e:
        print(f"Snowflake error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()