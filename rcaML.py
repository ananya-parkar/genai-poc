import json
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import sys
import io

# Force UTF-8 encoding for output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Step 1: Load Email Alert Data from JSON File
def load_email_data(file_path):
    """Load email alert data from a JSON file."""
    try:
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return None
    except json.JSONDecodeError:
        print("Error: Invalid JSON format.")
        return None

# Step 2: Keyword-Based Classification for Root Cause Analysis
def classify_root_cause(text):
    """Classify root cause based on keywords in the text."""
    root_cause_mapping = {
        "Delivery Failure": ["delivery failed", "bounced", "undelivered"],
        "Authentication Issue": ["invalid credentials", "access denied"],
        "System Error": ["server down", "timeout", "error 500", "critical system failure"],
        "Security Alert": ["suspicious login", "unauthorized access"],
        "Spam/Fraud Alert": ["blocked email", "phishing attempt"]
    }
    
    text = text.lower()
    for category, keywords in root_cause_mapping.items():
        if any(keyword in text for keyword in keywords):
            return category
    return "Unknown Issue"

# Step 3: Clustering Similar Alerts Using TF-IDF and K-Means
def cluster_alerts(df):
    """Cluster similar alerts using TF-IDF vectorization and K-Means."""
    # Combine subject and description for clustering
    df["combined_text"] = df["SUBJECT"].fillna("") + " " + df["DESCRIPTION"].fillna("")
    
    # TF-IDF vectorization
    vectorizer = TfidfVectorizer(stop_words="english")
    X = vectorizer.fit_transform(df["combined_text"])
    
    # Apply K-Means clustering
    num_clusters = 5  # Number of clusters (can be adjusted)
    kmeans = KMeans(n_clusters=num_clusters, random_state=42)
    df["cluster"] = kmeans.fit_predict(X)
    
    # Map cluster labels to root causes (manual mapping based on observation)
    cluster_labels = {
        0: "System Error",
        1: "Security Alert",
        2: "Spam/Fraud Alert",
        3: "Authentication Issue",
        4: "Delivery Failure"
    }
    
    df["cluster_root_cause"] = df["cluster"].map(cluster_labels)
    return df

# Step 4: Generate RCA Report in JSON Format
def generate_rca_report(df, output_file="rca_report.json"):
    """Generate a JSON report with RCA results."""
    rca_data = df[["INCIDENT_ID", "SUBJECT", "root_cause", "cluster_root_cause"]].to_dict(orient="records")
    
    with open(output_file, "w") as f:
        json.dump(rca_data, f, indent=4)
    
    print(f"✅ RCA JSON Report Generated: {output_file}")

# Main Execution
if __name__ == "__main__":
    # Path to JSON file containing email alert data
    JSON_FILE = "C:/Users/Ananya.Mehta/OneDrive - Parkar Digital/Desktop/code-genai/genai-poc/incident_data.json"  
    
    print(f"Loading email alert data from {JSON_FILE}...")
    email_df = load_email_data(JSON_FILE)

    if email_df is not None:
        print(f"Loaded {len(email_df)} alerts.")

        # Step 2: Apply Keyword-Based Classification
        print("Classifying root causes based on keywords...")
        email_df["root_cause"] = email_df["SUBJECT"].apply(classify_root_cause)

        # Step 3: Cluster Similar Alerts Using ML Techniques
        print("Clustering similar alerts...")
        email_df = cluster_alerts(email_df)

        # Display Results
        print("\u2705 RCA Completed:\n", email_df[["INCIDENT_ID", "SUBJECT", "root_cause", "cluster_root_cause"]])

        # Step 4: Generate RCA Report in JSON Format
        generate_rca_report(email_df)
