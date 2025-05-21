import requests
import snowflake.connector
 
# ServiceNow credentials and instance
SN_INSTANCE = ''
SN_USER = ''
SN_PASSWORD = ''
 
# Snowflake credentials
SF_USER = ''
SF_PASSWORD = ''
SF_ACCOUNT = ''
 
# Snowflake database and schema
SF_DATABASE = 'emailfetch'
SF_SCHEMA = 'email_schema'
 
# Fields to extract and store
FIELDS = [
    'number', 'caller', 'category', 'subcategory', 'service', 'service_offering',
    'configuration_item', 'channel', 'state', 'impact', 'urgency', 'priority',
    'assignment_group', 'assigned_to', 'short_description', 'description'
]
 
def extract_value(field):
    """Extract value from ServiceNow field (dict or scalar)."""
    if isinstance(field, dict):
        return field.get('display_value') or field.get('value')
    return field
 
def fetch_servicenow_incident_by_number(incident_number):
    url = f'https://{SN_INSTANCE}.service-now.com/api/now/table/incident'
    params = {
        'sysparm_fields': ','.join(FIELDS),
        'sysparm_query': f'number={incident_number}',
        'sysparm_limit': 1
    }
    headers = {"Accept": "application/json"}
    print(f"Fetching incident {incident_number} from ServiceNow...")
    response = requests.get(url, auth=(SN_USER, SN_PASSWORD), headers=headers, params=params)
    response.raise_for_status()
    return response.json()['result']
 
def create_table_if_not_exists(cur):
    columns_ddl = ",\n".join([f"{col} STRING" for col in FIELDS])
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS alerts_data (
            {columns_ddl}
        );
    """
    print("Ensuring alerts_data table exists...")
    cur.execute(create_table_sql)
 
def insert_incident(cur, incident):
    row = [extract_value(incident.get(f)) for f in FIELDS]
 
    # Check for duplicates
    cur.execute("SELECT 1 FROM alerts_data WHERE number = %s", (row[0],))
    if cur.fetchone():
        print(f"Incident {row[0]} already exists. Skipping insert.")
        return False
 
    placeholders = ','.join(['%s'] * len(FIELDS))
    insert_sql = f"INSERT INTO alerts_data ({','.join(FIELDS)}) VALUES ({placeholders})"
 
    cur.execute(insert_sql, tuple(row))
    print(f"Inserted incident {row[0]}")
    return True
 
def main():
    try:
        incident_number = input("Enter the incident number to fetch: ").strip()
        if not incident_number:
            print("No incident number provided. Exiting.")
            return None
 
        incidents = fetch_servicenow_incident_by_number(incident_number)
        if not incidents:
            print(f"No incident found with number {incident_number}.")
            return None
 
        conn = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            database=SF_DATABASE,
            schema=SF_SCHEMA
        )
        cur = conn.cursor()
 
        create_table_if_not_exists(cur)
 
        inserted_count = 0
        for incident in incidents:
            if insert_incident(cur, incident):
                inserted_count += 1

        conn.commit()
        print(f"Inserted {inserted_count} new incident(s) into Snowflake.")
        return incident_number
 
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from ServiceNow: {e}")
    except snowflake.connector.errors.Error as e:
        print(f"Snowflake error: {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass
 
if __name__ == "__main__":
    main()
 