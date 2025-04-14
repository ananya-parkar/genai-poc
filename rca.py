import requests
import configparser
import snowflake.connector
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
from datetime import datetime, timedelta
import re

def main():
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

    # Hugging Face API configuration for Mistral v2
    API_URL = "https://api-inference.huggingface.co/models/mistralai/Mistral-7B-Instruct-v0.2"
    API_KEY = config["huggingface"]["api_key"]
    HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

   

    # Step 1: Create AI Solutions Table in Snowflake
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
            ai_generated_Solution STRING
        );
        """
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()

    # Step 2: Fetch Latest Email Alert from Snowflake
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

    def generate_root_cause_analysis(alert_data, similar_solution=None):
        if not alert_data:
            return None

        prompt = f"User: Short Description: {alert_data.get('short_description', 'N/A')}\n"

        if similar_solution:
            prompt += f"Similar Solution: {similar_solution}\n"
        
        prompt+= "What is The root cause of the incident:\n "

        

        data = {"inputs": prompt}

        try:
            print("Sending request to Hugging Face API...")
            response = requests.post(API_URL, headers=HEADERS, json=data)
            response.raise_for_status()
            print("Hugging Face API request successful.")
            result = response.json()
            # print("Raw response from HF:", result)

            if isinstance(result, list) and result:
                generated_text =  result[0].get("generated_text", "").strip()
                # print("Generated Text:\n", generated_text)
                
                match = re.search(r"(The root cause.*?\.)", generated_text, re.IGNORECASE | re.DOTALL)
                if match:
                    root_cause = match.group(1).strip()
                    # print("Extracted Root Cause:", root_cause)
                    print(root_cause)
                    return root_cause
                else:
                    print("Could not extract root cause sentence. Using fully generated text as fallback.")
                    return generated_text
            else:
                print("Unexpected API response format.")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error in RCA generation: {e}")
            return None

    def store_rca_results(alert_data, root_cause_analysis):
        if not alert_data or not root_cause_analysis:
            return

        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        query = """
            INSERT INTO RCA_RESULTS (
                NUMBER, CONFIGURATION_ITEM, INCIDENT_SUBJECT, BUSINESS_SERVICE, OPENED,
                ALERT_TIME, STATE, CLOSED_BY, CLOSED, PRIORITY,
                TICKET_OPENED_BY, INCIDENT_DURATION, ACTION_TO_RESOLVE_INCIDENT,
                RESOLUTION_TYPE, RESOLVED_BY, SHORT_DESCRIPTION,
                NEXT_STEPS, RCA
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s
            )
        """

        values = (
            alert_data.get('Number', None),
            alert_data.get('Configuration_Item', None),
            alert_data.get('Incident_Subject', None),
            alert_data.get('Business_Service', None),
            alert_data.get('Opened', None),
            alert_data.get('Alert_Time', None),
            alert_data.get('State', None),
            alert_data.get('Closed_by', None),
            alert_data.get('Closed', None),
            alert_data.get('Priority', None),
            alert_data.get('Ticket_Opened_by', None),
            alert_data.get('Incident_Duration', None),
            alert_data.get('Action_to_Resolve_Incident', None),
            alert_data.get('Resolution_Type', None),
            alert_data.get('Resolved_By', None),
            alert_data.get('Short_Description', None),
            alert_data.get('Next_Steps', None),
            root_cause_analysis
        )

        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()

    def load_existing_embeddings():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        query = """
            SELECT number, short_description, ai_generated_Solution, opened
            FROM AI_SOLUTIONS;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        incident_ids = [row[0] for row in rows]
        descriptions = [row[1] for row in rows]
        solutions = [row[2] for row in rows]
        timestamps = [row[3] for row in rows]

        return incident_ids, descriptions, solutions, timestamps

    def build_faiss_index(descriptions):
        embeddings = embedding_model.encode(descriptions, convert_to_numpy=True)
        dim = embeddings.shape[1]
        index = faiss.IndexFlatL2(dim)
        index.add(embeddings)
        return index

    def check_for_similar_alert(new_description, index, descriptions, incident_ids, solutions, timestamps, threshold=0.85):
        new_embedding = embedding_model.encode([new_description], convert_to_numpy=True)
        D, I = index.search(new_embedding, k=1)
        similarity = 1 / (1 + D[0][0])

        if similarity >= threshold:
            match_idx = I[0][0]
            matched_solution_timestamp = timestamps[match_idx]

            # Time threshold check
            two_months_ago = datetime.now() - timedelta(days=60)
            if matched_solution_timestamp < two_months_ago:
                return None,False  # Force new solution generation, Match too old
            return solutions[match_idx], True #valid recent match
        return None, False #No similar match

        return None

    # Main Workflow Execution
    print("Executing RCA workflow...")  # Added print statement
    create_ai_solutions_table()

    latest_alert = fetch_latest_email_alert()

    if latest_alert:
        print("Latest Alert Data:", latest_alert)

        incident_ids, descriptions, solutions, timestamps = load_existing_embeddings()

        if descriptions:
            index = build_faiss_index(descriptions)
            matched_solution, is_recent_match = check_for_similar_alert(
                latest_alert.get("short_description", ""), index, descriptions, 
                incident_ids, solutions, timestamps
            )
        else:
            matched_solution = None

        if is_recent_match and matched_solution:
            print("Similar recent solution found, generating RCA with similar solution.")
            root_cause_analysis = generate_root_cause_analysis(latest_alert, matched_solution)
        else:
            print("No similar recent solution found, generating new RCA.")
            root_cause_analysis = generate_root_cause_analysis(latest_alert)

        if root_cause_analysis:
            store_rca_results(latest_alert, root_cause_analysis)
        else:
            print("Failed to generate RCA.")
    else:
        print("No latest alert found.")


if __name__ == "__main__":
    print("This is the RCA Module, the outputs will automatically be generated when main.py is run. ")
