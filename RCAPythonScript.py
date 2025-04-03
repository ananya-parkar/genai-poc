import json
import pandas as pd
import networkx as nx
import os
import sys
import io

# Force UTF-8 encoding for output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def load_incident_data(file_path):
    """Load incident data from JSON file."""
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return None

    try:
        with open(file_path, encoding="utf-8") as f:  # Ensure UTF-8 encoding for file reading
            data = json.load(f)
        
        # Convert list of incidents to DataFrame
        df = pd.DataFrame(data)
        
        # Remove duplicate incidents based on INCIDENT_ID and TIMESTAMP
        df = df.drop_duplicates(subset=['INCIDENT_ID', 'TIMESTAMP'])
        
        return df
    except json.JSONDecodeError:
        print("Error: Invalid JSON format.")
        return None

def analyze_individual_incident(row):
    """Analyze a single incident to find potential root causes."""
    G = nx.DiGraph()

    # Handle missing values gracefully
    service_node = f"Service: {row['AFFECTED_SERVICE']}" if row['AFFECTED_SERVICE'] else "Unknown Service"
    incident_node = f"Incident: {row['INCIDENT_ID']}"

    # Add nodes for incidents and services
    G.add_node(service_node, type='service')
    G.add_node(incident_node, type='incident', impact=row['IMPACT_LEVEL'], description=row['DESCRIPTION'])

    # Add edge from service to incident if service is known
    if row['AFFECTED_SERVICE']:
        G.add_edge(service_node, incident_node)

    # Debugging output to visualize the graph structure
    print("Graph Nodes:", G.nodes(data=True))
    print("Graph Edges:", list(G.edges(data=True)))

    # Analyze graph to find potential root causes (in this case, just the current incident)
    root_causes = [service_node] if G.in_degree(incident_node) == 0 else []

    return root_causes

if __name__ == "__main__":
    # Absolute path or relative path to your JSON file
    JSON_FILE = "C:/Users/Ananya.Mehta/OneDrive - Parkar Digital/Desktop/code-genai/genai-poc/incident_data.json"

    print(f"Loading incident data from {JSON_FILE}...")
    incidents_df = load_incident_data(JSON_FILE)

    if incidents_df is not None:
        print(f"Loaded {len(incidents_df)} incidents.")

        # Iterate through each incident and analyze individually
        for index, row in incidents_df.iterrows():
            print(f"\nAnalyzing Incident ID: {row['INCIDENT_ID']}")
            root_causes = analyze_individual_incident(row)

            if root_causes:
                print("Potential Root Causes:")
                for cause in root_causes:
                    print(f"- {cause}")
            else:
                print("No clear root causes identified for this incident.")
