import snowflake.connector
import configparser
import requests
import json
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta 

# Load Hugging Face transformer model
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

# Load config
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

API_URL = "https://api-inference.huggingface.co/models/mistralai/Mistral-7B-Instruct-v0.2"
API_KEY = config["huggingface"]["api_key"]
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

def create_ai_solutions_table():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    query = """
    CREATE TABLE IF NOT EXISTS AI_SOLUTIONS (
        number STRING PRIMARY KEY,
        incident_subject STRING,
        priority STRING,
        business_service STRING,
        opened TIMESTAMP,
        short_description STRING,
        ai_generated_solution STRING
    );
    """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    
    # print("AI_SOLUTIONS table checked/created successfully.")

def fetch_latest_email_alert():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    query = """
    SELECT 
    Number, Configuration_Item, Incident_Subject, Business_Service, 
    Opened, Alert_Time, State, Closed_by, Closed, Priority, 
    Ticket_Opened_by, Incident_Duration, Action_to_Resolve_Incident, 
    Resolution_Type, Resolved_By, Short_Description, Next_Steps
    FROM alerts
    ORDER BY ALERT_TIME DESC
    LIMIT 1;
    """
    cursor.execute(query)
    latest_alert = cursor.fetchone()
    column_names = [desc[0].lower() for desc in cursor.description]
    cursor.close()
    conn.close()
    return dict(zip(column_names, latest_alert)) if latest_alert else None

def generate_solution(alert_data):
    if not alert_data:
        return "No new alerts found."
    formatted_alert = f"""
    Incident Number: {alert_data['number']}
    Subject: {alert_data['incident_subject']}
    Priority: {alert_data['priority']}
    Business Service: {alert_data['business_service']}
    Opened: {alert_data['opened']}
    Description: {alert_data['short_description']}
    """
    data = {"inputs": f"Analyze this email alert and provide a solution with steps:\n\n{formatted_alert}"}
    try:
        response = requests.post(API_URL, headers=HEADERS, json=data)
        response.raise_for_status()
        result = response.json()
        if isinstance(result, list) and result:
            return result[0].get("generated_text", "Solution not found.")
        return "Solution not found."
    except Exception as e:
        print(f"Error: {e}")
        return "API request failed."

def store_ai_solution(alert_data, ai_solution):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = """
    INSERT INTO AI_SOLUTIONS (number, incident_subject, priority, business_service, opened, short_description, ai_generated_solution)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        alert_data['number'], alert_data['incident_subject'], alert_data['priority'],
        alert_data['business_service'], alert_data['opened'],
        alert_data['short_description'], ai_solution
    )
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    conn.close()
    print("Solution stored successfully.")

def load_existing_embeddings():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    query = """
        SELECT number, short_description, ai_generated_solution, opened
        FROM AI_SOLUTIONS;
    """
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            return [], [], [], []
        incident_ids = [row[0] for row in rows]          # number
        descriptions = [row[1] for row in rows]          # short_description
        solutions = [row[2] for row in rows]             # ai_generated_solution
        timestamps = [row[3] for row in rows]            # opened timestamp
        return incident_ids, descriptions, solutions, timestamps
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"SQL Error: {e}")
        return [], [], [], []
    finally:
        cursor.close()
        conn.close()

def build_faiss_index(descriptions):
    embeddings = embedding_model.encode(descriptions, convert_to_numpy=True)
    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(embeddings)
    return index

def check_for_similar_alert(new_description, index, descriptions, incident_ids, solutions, timestamps, threshold=0.85):
    new_embedding = embedding_model.encode([new_description], convert_to_numpy=True)
    D, I = index.search(new_embedding, k=1)
    similarity = 1 / (1 + D[0][0])  # Convert L2 distance to similarity score
    
    if similarity >= threshold:
        match_idx = I[0][0]
        print(f"Similar alert found! Similarity: {similarity:.2f}")
        
        matched_timestamp = timestamps[match_idx]
        current_date = datetime.now()
        
        diff = relativedelta(current_date, matched_timestamp)
        month_difference = diff.years * 12 + diff.months
        
        if month_difference > 2:
            print(f"Matched solution is older than 2 months (Age: {month_difference} months). Generating a new solution.")
            return None
        
        return solutions[match_idx]
    
    print(f"No similar alert found. Similarity: {similarity:.2f}")
    return None

def get_ai_solutions_count():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM AI_SOLUTIONS"
        cursor.execute(query)
        result = cursor.fetchone()
        count = result[0] if result else 0
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"Error fetching AI solutions count: {e}")
        return 0
def main():
    create_ai_solutions_table()
    latest_alert = fetch_latest_email_alert()
    if latest_alert:
        incident_ids, descriptions, solutions, timestamps = load_existing_embeddings()
        if descriptions:
            index = build_faiss_index(descriptions)
            matched_solution = check_for_similar_alert(
                latest_alert["short_description"], index, descriptions, incident_ids, solutions, timestamps
            )
        else:
            matched_solution = None
        if matched_solution:
            store_ai_solution(latest_alert, matched_solution)
        else:
            generated_solution = generate_solution(latest_alert)
            if generated_solution:
                store_ai_solution(latest_alert, generated_solution)
