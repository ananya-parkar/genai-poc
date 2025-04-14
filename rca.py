# rca.py
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

    # Load the model with authentication (if required)
    embedding_model = SentenceTransformer('all-MiniLM-L6-v2', use_auth_token=API_KEY)

    HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

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
            ai_generated_Solution STRING,
            root_cause_analysis STRING
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
            print("No alert data provided.")
            return None

        # Only include the detailed alert info in the prompt if no similar solution is found
        if not similar_solution:
            formatted_alert = f"""
            Incident Number: {alert_data.get('Number', 'N/A')}
            Configuration Item: {alert_data.get('Configuration_Item', 'N/A')}
            Incident Subject: {alert_data.get('Incident_Subject', 'N/A')}
            Business Service: {alert_data.get('Business_Service', 'N/A')}
            Opened: {alert_data.get('Opened', 'N/A')}
            Alert Time: {alert_data.get('Alert_Time', 'N/A')}
            State: {alert_data.get('State', 'N/A')}
            Closed By: {alert_data.get('Closed_by', 'N/A')}
            Closed: {alert_data.get('Closed', 'N/A')}
            Priority: {alert_data.get('Priority', 'N/A')}
            Ticket Opened By: {alert_data.get('Ticket_Opened_by', 'N/A')}
            Incident Duration: {alert_data.get('Incident_Duration', 'N/A')}
            Action to Resolve Incident: {alert_data.get('Action_to_Resolve_Incident', 'N/A')}
            Resolution Type: {alert_data.get('Resolution_Type', 'N/A')}
            Resolved By: {alert_data.get('Resolved_By', 'N/A')}
            Short Description: {alert_data.get('Short_Description', 'N/A')}
            Next Steps: {alert_data.get('Next_Steps', 'N/A')}
            """
        else:
            formatted_alert = ""

        if similar_solution:
            prompt = f"""
            Analyze this alert and its similar solution to provide a detailed root cause analysis:\n\n{formatted_alert}\n\nSimilar Solution:\n{similar_solution}
            """
        else:
            prompt = f"""
            Analyze this alert and provide a detailed root cause analysis:\n\n{formatted_alert}
            """

        data = {"inputs": prompt}

        try:
            # print("Sending request to Hugging Face API...")
            response = requests.post(API_URL, headers=HEADERS, json=data)
            response.raise_for_status()
            # print("Hugging Face API request successful.")
            result = response.json()

            if isinstance(result, list) and result:
                rca_details = result[0].get("generated_text", "")
                if rca_details:
                    # Extract Root Cause Analysis and Summarize
                    start_index = rca_details.find("Root Cause Analysis:")
                    if start_index != -1:
                        rca_section = rca_details[start_index + len("Root Cause Analysis:"):].strip()
                        # Remove everything after "Further Investigation"
                        further_investigation_index = rca_section.find("Further Investigation:")
                        if further_investigation_index != -1:
                            rca_section = rca_section[:further_investigation_index].strip()

                        # Clean up and format RCA lines
                        rca_lines = [line.strip() for line in rca_section.splitlines() if line.strip()]

                        # Summarize RCA in 2-3 lines
                        summary = ". ".join(rca_lines[:3]) + "." if len(rca_lines) > 0 else "No root cause analysis found."
                        print(f"Root Cause Analysis for Incident: {summary}")
                        return summary  # Return Summary
                    # Check if the message returns with API Failed
                    elif "API request failed" in rca_details:
                        print("The root cause analysis indicates that the API request failed.")
                        return "API request failed. Please check the Hugging Face API."
                    else:
                        print("No root cause analysis found in the generated text.")
                        return None
                else:
                    print("No generated text found in the API response.")
                    return None
            else:
                print("Unexpected API response format:", result)
                return None

        except requests.exceptions.RequestException as e:
            print(f"Hugging Face API request failed: {e}")
            return None
        except Exception as e:
            print(f"An error occurred while processing the API response: {e}")
            return None

    # Step 4: Store AI Solution in Snowflake
    def store_rca_results(alert_data, root_cause_analysis):
        if not alert_data or not root_cause_analysis:
            print("Skipping store_rca_results due to missing alert_data or root_cause_analysis.")
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

    # Step 5: Load Existing Embeddings from Snowflake
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
                return None  # Force new solution generation
            return solutions[match_idx]

        return None

    # Main Workflow Execution
    create_ai_solutions_table()

    latest_alert = fetch_latest_email_alert()

    if latest_alert:
        print("Latest Alert Data:", latest_alert)

        incident_ids, descriptions, solutions, timestamps = load_existing_embeddings()

        if descriptions:
            index = build_faiss_index(descriptions)
            matched_solution = check_for_similar_alert(
                latest_alert.get("short_description", ""), index, descriptions, 
                incident_ids, solutions, timestamps
            )
        else:
            matched_solution = None

        if matched_solution:
            print("Similar solution found, generating RCA with similar solution.")
            root_cause_analysis = generate_root_cause_analysis(latest_alert, matched_solution)
            if not root_cause_analysis or root_cause_analysis == "No new alerts found.":
                print("Failed to generate RCA with similar solution. Generating a new solution...")
                root_cause_analysis = generate_root_cause_analysis(latest_alert)  # Generate new solution
        else:
            print("No similar solution found, generating new RCA.")
            root_cause_analysis = generate_root_cause_analysis(latest_alert)

        if root_cause_analysis and root_cause_analysis != "No new alerts found.":
            store_rca_results(latest_alert, root_cause_analysis)
        else:
            print("Failed to generate RCA.")
    else:
        print("No latest alert found.")
