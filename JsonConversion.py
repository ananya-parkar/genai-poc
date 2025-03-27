import snowflake.connector
import json
from datetime import datetime

def json_serializer(obj):
    """Custom serializer for non-JSON-native objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

# Snowflake connection
conn = snowflake.connector.connect(
    user='Eshita05',
    password='Eshita@05032003',
    account='av91825.central-india.azure',
    warehouse='emailsdata',
    database='emailfetch',
    schema='email_schema'
)

# Fetch cleaned incident data
cursor = conn.cursor()
cursor.execute("SELECT * FROM cleaned_alerts")  # Your cleaned table
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

# Convert to JSON with datetime handling
incident_data = [dict(zip(columns, row)) for row in rows]
incident_json = json.dumps(incident_data, 
                          default=json_serializer,  # Add custom serializer
                          indent=4)

# Save JSON to file
with open("incident_data.json", "w") as f:
    f.write(incident_json)

# Close connections
cursor.close()
conn.close()
