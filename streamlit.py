# %%
import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime
# Custom CSS Styling
st.markdown("""
<style>
   /* Set background color of the main app */
   .stApp {
       background-color: #e6f2ff;
   }
   /* Title styling */
   .title-text {
       color: #0a2d5f;
       font-size: 42px;
       font-weight: 800;
       text-align: center;
       margin-bottom: 30px;
   }
   /* Sidebar background and text */
   section[data-testid="stSidebar"] {
       background-color: #0d47a1;
   }
   section[data-testid="stSidebar"] h2,
   section[data-testid="stSidebar"] label,
   .stRadio label {
       color: white !important;
    }
   /* Sidebar button text */
   .sidebar .element-container button {
       color: white !important;
   }
   /* Box around alert */
   .alert-box {
       background-color: #ffffff;
       border: 1px solid #90caf9;
       border-radius: 10px;
       padding: 20px;
       max-height: 300px;
       overflow-y: auto;
       margin-top: 20px;
   }
</style>
""", unsafe_allow_html=True)
st.markdown('<div class="title-text">AI INCIDENT ALERT DASHBOARD</div>', unsafe_allow_html=True)
# Sidebar navigation
st.sidebar.markdown("## Navigation")
selection = st.sidebar.radio("", ["Fetch Alert", "View Incidents Table"])
# Connect to Snowflake
@st.cache_resource
def get_connection():
   return snowflake.connector.connect(
       user="Eshita05",
       password="Eshita@05032003",
       account="av91825.central-india.azure",
       database="EMAILFETCH",
       schema="email_schema"
   )
conn = get_connection()
# Fetch alert
def fetch_latest_alert():
   query = "SELECT * FROM alerts ORDER BY Alert_Time DESC LIMIT 1"
   df = pd.read_sql(query, conn)
   return df
# Fetch full table with filters
def fetch_incident_table(priority=None, month=None):
   query = "SELECT * FROM alerts"
   df = pd.read_sql(query, conn)
   if priority:
       df = df[df["PRIORITY"].str.lower() == priority.lower()]
   if month:
       df["month"] = pd.to_datetime(df["ALERT_TIME"]).dt.strftime("%B")
       df = df[df["month"] == month]
   return df
def get_csv_download_link(df):
   csv = df.to_csv(index=False).encode()
   st.download_button("Download Data as CSV", csv, "incident_data.csv", "text/csv")
# Page Logic
if selection == "Fetch Alert":
   if st.button("Fetch Latest Alert"):
       with st.spinner("Fetching alert..."):
           df = fetch_latest_alert()
           if not df.empty:
               alert = df.iloc[0]
               st.markdown('<div class="alert-box">', unsafe_allow_html=True)
               st.markdown(f"**📛 INCIDENT ID:** `{alert['NUMBER']}`")
               st.markdown(f"**🖥 CONFIGURATION ITEM:** `{alert['CONFIGURATION_ITEM']}`")
               st.markdown(f"**📝 INCIDENT SUBJECT:** `{alert['INCIDENT_SUBJECT']}`")
               st.markdown(f"**📊 BUSINESS SERVICE:** `{alert['BUSINESS_SERVICE']}`")
               st.markdown(f"**⏰ ALERT TIME:** `{alert['ALERT_TIME']}`")
               st.markdown(f"**⚙️ STATE:** `{alert['STATE']}`")
               st.markdown(f"**🎯 PRIORITY:** `{alert['PRIORITY']}`")
               st.markdown(f"**📆 OPENED:** `{alert['OPENED']}`")
               st.markdown(f"**🛠 ACTION TO RESOLVE:** `{alert['ACTION_TO_RESOLVE_INCIDENT']}`")
               st.markdown(f"**✅ RESOLVED BY:** `{alert['RESOLVED_BY']}`")
               st.markdown('</div>', unsafe_allow_html=True)
           else:
               st.error("No alert found.")
else:
   priorities = ["High", "Medium", "Low"]
   months = pd.date_range(start="2024-01-01", end=datetime.today(), freq='MS').strftime("%B").tolist()
   col1, col2 = st.columns(2)
   with col1:
       priority_filter = st.selectbox("Filter by Priority", ["All"] + priorities)
   with col2:
       month_filter = st.selectbox("Filter by Month", ["All"] + months)
   filtered_df = fetch_incident_table(
       priority=None if priority_filter == "All" else priority_filter,
       month=None if month_filter == "All" else month_filter
   )
   st.dataframe(filtered_df)
   if not filtered_df.empty:
       get_csv_download_link(filtered_df)
# %%