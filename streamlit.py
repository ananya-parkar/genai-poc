import streamlit as st
import requests
import snowflake.connector
from streamlit_lottie import st_lottie
from solutionfaiss import (
    create_ai_solutions_table,
    fetch_latest_email_alert,
    generate_solution,
    store_ai_solution,
    load_existing_embeddings,
    build_faiss_index,
    check_for_similar_alert,
    SNOWFLAKE_CONFIG,
    API_URL,
    API_KEY,
    HEADERS,
    get_ai_solutions_count
)
import pandas as pd
from sentence_transformers import SentenceTransformer
import faiss
from ftrca import generate_root_cause_analysis, store_rca_results

# Streamlit styling
st.markdown(
    """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;700&display=swap');

    .main {
        background-color: #DBE9F4;
    }
    [data-testid="stSidebar"] {
        background-color: #A7D1EA;
        font-family: 'Roboto', sans-serif;
        color: #000000;
    }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #000000;
        font-family: 'Roboto', sans-serif;
    }
    h1 {
        color: #000000;
        font-family: 'Roboto', sans-serif;
    }
    h2,
    h3 {
        color: #000000;
        font-family: 'Roboto', sans-serif;
    }
    .stButton button {
        background-color: white;
        color: #000000;
        border-color:white;
        width: 85%;
        font-family: 'Roboto', sans-serif;
        font-size: 20px; /* Adjust the value as needed */
    }
    .stButton button:hover {
        background-color: #317aa8;
    }
    body {
        font-family: 'Roboto', sans-serif;
        color: #000000;
    }
    .stMarkdown {
        font-family: 'Roboto', sans-serif;
        color: #000000;
    }
    .stTable {
        background-color: #ffffff;
        border-radius: 8px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }

    /* Center headings below images */
    .css-12oz5ky { /* Target the container of image and markdown */
        display: flex;
        flex-direction: column;
        align-items: center; /* Center items horizontally */
        text-align: center;
    }

     /* Add indent to About Us text */
    .about-us-text {
        text-align: justify;
        margin-left: 20px; /* Adjust the value as needed */
    }

    .feature-text {
        font-size: 18px; /* Adjust the size as needed */
    }
</style>
""",
    unsafe_allow_html=True,
)


# embedding model (make sure it exists in solutionfaiss.py)
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")


# --- Functions ---
def load_lottie_url(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error loading Lottie animation: {e}")
        return None


def fetch_incidents_from_snowflake(
    priority_filter, month_filter, incident_id_search
):
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        query = f"""
            SELECT * FROM alerts WHERE 1=1
        """
        if priority_filter != "All":
            query += f" AND priority = '{priority_filter}'"
        if month_filter != "All":
            query += f" AND EXTRACT(MONTH FROM alert_time) = {month_filter}"
        if incident_id_search:
            query += f" AND number = '{incident_id_search}'"
        cursor.execute(query)
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        return result, column_names
    except Exception as e:
        st.error(f"Error fetching incidents from Snowflake: {e}")
        return [], []


def find_similar_alerts(latest_alert, num_results=3):
    incident_ids, descriptions, solutions = load_existing_embeddings()

    # Check if embeddings exist
    if not descriptions:
        st.warning("No existing solutions found in AI_SOLUTIONS table.")
        return pd.DataFrame()

    # Encode new alert and search for similar alerts
    index = build_faiss_index(descriptions)
    new_embedding = embedding_model.encode(
        [latest_alert["short_description"]], convert_to_numpy=True
    )
    D, I = index.search(new_embedding, k=num_results)  # D is distances, I is indices
    similar_alerts = []
    for i in range(len(I[0])):  # Iterate through each of the results
        idx = I[0][i]  # Get the index of the ith most similar result
        similarity_score = 1 / (1 + D[0][i]) if D[0][i] >= 0 else 0
        if idx < len(incident_ids):  # Make sure index is within bounds
            incident_id = incident_ids[idx]
            description = descriptions[idx]
            solution = solutions[idx]
            similar_alerts.append(
                {
                    "Incident ID": incident_id,
                    "Description": description,
                    "AI Generated Solution": solution,
                    "Similarity Score": similarity_score,
                }
            )
        else:
            print(f"Index {idx} out of bounds. Total descriptions: {len(descriptions)}")
    return pd.DataFrame(similar_alerts)


def fetch_and_show_solution():
    latest_alert = fetch_latest_email_alert()

    if latest_alert:
        st.write("### Latest Alert Details")
        st.write(f"*🔔 **Alert Number** :* {latest_alert['number']}")
        st.write(f"*📝 **Incident Subject** :* {latest_alert['incident_subject']}")
        st.write(f"*⚠️ **Priority** :* {latest_alert['priority']}")
        st.write(f"*🛠️ **Affected Service** :* {latest_alert['business_service']}")
        st.write(f"*📄 **Description** :* {latest_alert['short_description']}")
        st.write("---")

        col1, col2 = st.columns(2)
        with col1:
            fetch_solution_pressed = st.button("Fetch Solution")
        with col2:
            show_rca_pressed = st.button("Show Probable Root Cause")

        if fetch_solution_pressed:
            ai_solution = generate_solution(latest_alert)
            if ai_solution:
                st.markdown("### Solution")
                st.write(ai_solution)
                store_ai_solution(latest_alert, ai_solution)
            else:
                st.write("Solution not found or error occurred.")

        if show_rca_pressed:
            root_cause = generate_root_cause_analysis(latest_alert)
            if root_cause:
                st.write("### Probable Root Cause")
                st.write(root_cause)
                store_rca_results(latest_alert, root_cause)
            else:
                st.write("Root cause analysis not found or error occurred.")
        # Find and display similar alerts
        st.write("---")
        st.write("### Similar Alerts")
        similar_alerts_df = find_similar_alerts(latest_alert)
        if not similar_alerts_df.empty:
            st.dataframe(similar_alerts_df, use_container_width=True)
        else:
            st.write("No similar alerts found.")

    else:
        st.write("No alert found.")


def home_page():
    st.title("Incident Resolution Automation")
    st.subheader("AI-Powered Solutions for Faster Incident Resolution")
    st.markdown(
        f"""
        <div style="text-align: justify;">
            Our platform leverages cutting-edge artificial intelligence to revolutionize
            incident management. We analyze incoming alerts with unparalleled
            precision, instantly pinpoint root causes, and generate effective
            solutions. Our goal is to empower your team to resolve incidents faster,
            reduce downtime, and optimize operational efficiency.
        </div>
        """,
        unsafe_allow_html=True,
    )
    ai_solutions_count = get_ai_solutions_count()
    st.markdown(f"**Incidents Resolved:** {ai_solutions_count}")

    st.markdown("## Key Features")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.image(
            "https://as2.ftcdn.net/jpg/03/31/54/45/1000_F_331544598_m6ClVmywz7YrjReVe9UBPhgU2gi0nbZQ.jpg",
            width=160,
        )
        st.markdown("**Automated Alert Analysis**")
           

    with col2:
        st.image(
            "https://www.instituteofcustomerservice.com/wp-content/uploads/2023/09/Root-Cause-Analysis.png",
            width=160,
        )
        st.markdown("**Root Cause Identification**")

    with col3:
        st.image(
            "https://media.licdn.com/dms/image/D4D12AQHSjTvlK4d_Yw/article-cover_image-shrink_720_1280/0/1699567307145?e=2147483647&v=beta&t=jWUVxfVYoR7F5qPUseEmzQb1ZDPZEiMzx8GEmQKOndo",
            width=180,
        )
        st.markdown("**AI-Powered Solutions**")
            

    lottie_ai = load_lottie_url(
        "https://assets9.lottiefiles.com/packages/lf20_pwohahvd.json"
    )
    if lottie_ai:
        st_lottie(lottie_ai, speed=1, loop=True, quality="high", height=300)
    else:
        st.write("Failed to load animation.")

    st.markdown("## About Us")
    st.markdown(
        f"""
        <div class="about-us-text">
            Incident IQ is more than just a platform; it's a vision brought to life by a team of dedicated innovators at [Your Company Name].
            We are a collective of AI aficionados, seasoned IT experts, and inventive problem-solvers, united by a singular mission: to transform incident management through the power of artificial intelligence.
            Our Journey:
            From the outset, we recognized the transformative potential of AI in revolutionizing IT incident management. Our journey began with a deep dive into the challenges faced by IT teams, from alert overloads to intricate root cause analyses. We envisioned a future where AI could alleviate these burdens, empowering IT professionals to concentrate on strategic endeavors.
            Our Commitment:
            At Incident IQ, we're committed to making AI accessible and user-friendly for businesses of all sizes. Whether you're a burgeoning startup or a global enterprise, our solutions are designed to seamlessly integrate into your existing workflows, delivering tangible value from day one.
            Join us as we pioneer the next era of incident management, where AI-driven automation and intelligent insights converge to create a smarter, more efficient IT landscape.
        </div>
        """,
        unsafe_allow_html=True,
    )


def view_incident_table():
    st.write("### Incident Table")
    priority_filter = st.selectbox(
        "Filter by Priority", ["All", "High", "Medium", "Low"]
    )
    month_filter = st.selectbox("Filter by Month", ["All", "4", "5", "6"])
    incident_id_search = st.text_input("Search by Incident ID")
    incidents, column_names = fetch_incidents_from_snowflake(
        priority_filter, month_filter, incident_id_search
    )
    if incidents:
        import pandas as pd

        df = pd.DataFrame(incidents, columns=column_names)
        st.dataframe(df, use_container_width=True, height=400)
    else:
        st.write("No incidents found matching the filters.")


def main():
    st.sidebar.title("Navigation")
    if "page" not in st.session_state:
        st.session_state.page = "Home"

    if st.sidebar.button("Home"):
        st.session_state.page = "Home"
    if st.sidebar.button("Fetch Latest Alert and Solution"):
        st.session_state.page = "Fetch Latest Alert and Solution"
    if st.sidebar.button("View Incident Table"):
        st.session_state.page = "View Incident Table"

    if st.session_state.page == "Home":
        home_page()
    elif st.session_state.page == "Fetch Latest Alert and Solution":
        fetch_and_show_solution()
    elif st.session_state.page == "View Incident Table":
        view_incident_table()


if __name__ == "__main__":
    main()
