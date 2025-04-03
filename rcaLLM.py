import json
from transformers import pipeline
import torch
import sys
import io

# Force UTF-8 encoding for output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Step 1: Load Email Alert Data from JSON File
def load_email_data(file_path):
    """Load email alert data from a JSON file."""
    try:
        with open(file_path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return None
    except json.JSONDecodeError:
        print("Error: Invalid JSON format.")
        return None

# Step 2: Post-process Model Output
def post_process_response(response):
    """Extract concise root cause analysis from model response."""
    sentences = response.split('.')
    return sentences[0].strip() + '.' if sentences else response.strip()

# Step 3: Analyze Incident with LLM for Root Cause Prediction
def analyze_incident_with_llm(incident, llm_model):
    """Use an LLM to perform root cause analysis."""
    prompt = (
        f"Incident Report:\n"
        f"Subject: {incident.get('SUBJECT', 'N/A')}\n"
        f"Incident ID: {incident.get('INCIDENT_ID', 'Unknown ID')}\n"
        f"Description: {incident.get('DESCRIPTION', 'No description available')}\n\n"
        "Based on the information above, provide a concise root cause analysis in one sentence:"
    )
    
    # Generate response using max_new_tokens instead of max_length
    response = llm_model(prompt, max_new_tokens=50, num_return_sequences=1, pad_token_id=50256)
    
    # Post-process response to extract concise output
    return post_process_response(response[0]['generated_text'].strip())

# Step 4: Generate RCA Report in JSON Format
def generate_rca_report(incidents, output_file="rca_report.json"):
    """Generate a JSON report with RCA results."""
    with open(output_file, "w") as f:
        json.dump(incidents, f, indent=4)
    
    print(f"✅ RCA JSON Report Generated: {output_file}")

# Main Execution
if __name__ == "__main__":
    # Path to JSON file containing email alert data
    JSON_FILE = "C:/Users/Ananya.Mehta/OneDrive - Parkar Digital/Desktop/code-genai/genai-poc/incident_data.json"  # Replace with your actual file path
    
    print(f"Loading email alert data from {JSON_FILE}...")
    incidents = load_email_data(JSON_FILE)

    if incidents is not None:
        print(f"Loaded {len(incidents)} incidents.")

        # Initialize LLM pipeline once (to avoid repeated device messages)
        device = 0 if torch.cuda.is_available() else -1  # Use GPU if available; otherwise CPU
        llm_model = pipeline("text-generation", model="gpt2", device=device)

        # Analyze each incident using the LLM
        rca_results = []
        
        for incident in incidents:
            print(f"\nAnalyzing Incident ID: {incident.get('INCIDENT_ID', 'Unknown ID')}")
            
            # Perform root cause analysis
            root_cause_analysis = analyze_incident_with_llm(incident, llm_model)
            incident["root_cause_analysis"] = root_cause_analysis
            
            # Append results to list
            rca_results.append(incident)

            # Display concise RCA result for current incident
            print("Root Cause Analysis:")
            print(root_cause_analysis)

        # Step 4: Generate RCA Report in JSON Format
        generate_rca_report(rca_results)
