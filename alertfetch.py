import requests
import snowflake.connector


# ServiceNow credentials and instance
SN_INSTANCE = 'dev309703' 
SN_USER = 'admin'             
SN_PASSWORD = 'fdF-hl$KF63A'  
# Snowflake credentials
SF_USER = 'Eshita05'
SF_PASSWORD = 'Eshita@05032003'
SF_ACCOUNT = 'av91825.central-india.azure' 

# Snowflake database and schema
SF_DATABASE = 'emailfetch'
SF_SCHEMA = 'email_schema'

# Columns to fetch from ServiceNow and store in Snowflake

FIELDS = [
    'number', 'caller', 'category', 'subcategory', 'service', 'service_offering',
    'configuration_item', 'channel', 'state', 'impact', 'urgency', 'priority', 'assignment_group',
    'assigned_to', 'short_description', 'description'
]



def extract_value(field):
    """Extract string value from ServiceNow field which may be dict or scalar."""
    if isinstance(field, dict):
        return field.get('display_value') or field.get('value')
    return field

def fetch_servicenow_incident_by_number(incident_number):
    url = f'https://{SN_INSTANCE}.service-now.com/api/now/table/incident'
    api_fields = [f.replace('_', ' ') for f in FIELDS]
    params = {
        'sysparm_fields': ','.join(api_fields),
        'sysparm_query': f'number={incident_number}',
        'sysparm_limit': 1
    }
    headers = {"Accept": "application/json"}

    response = requests.get(url, auth=(SN_USER, SN_PASSWORD), headers=headers, params=params)
    response.raise_for_status()
    return response.json()['result']

def create_table_if_not_exists(cur):
    # Create table with columns as STRING, underscores in column names
    columns_ddl = ",\n".join([f"{col} STRING" for col in FIELDS])
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS alertss (
            {columns_ddl}
        );
    """
    cur.execute(create_table_sql)

def insert_incident(cur, incident):
    # Prepare row data in order of FIELDS
    row = [extract_value(incident.get(f.replace('_', ' '))) for f in FIELDS]

    # Check if incident number exists
    cur.execute("SELECT 1 FROM alertss WHERE number = %s", (row[0],))
    if cur.fetchone():
        print(f"Incident {row[0]} already exists. Skipping insert.")
        return False

    placeholders = ','.join(['%s'] * len(FIELDS))
    insert_sql = f"INSERT INTO alertss ({','.join(FIELDS)}) VALUES ({placeholders})"
    cur.execute(insert_sql, row)
    print(f"Inserted incident {row[0]}")
    return True

def main():
    try:
        # Prompt user for incident number
        incident_number = input("Enter the incident number to fetch: ").strip()
        if not incident_number:
            print("No incident number provided. Exiting.")
            return

        # Fetch the specified incident from ServiceNow
        incidents = fetch_servicenow_incident_by_number(incident_number)
        if not incidents:
            print(f"No incident found with number {incident_number}.")
            return
        print(f"Fetched incident {incident_number} from ServiceNow.")

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            database=SF_DATABASE,
            schema=SF_SCHEMA
        )
        cur = conn.cursor()

        # Create table if not exists
        create_table_if_not_exists(cur)

        # Insert the incident
        inserted_count = 0
        for incident in incidents:
            if insert_incident(cur, incident):
                inserted_count += 1

        conn.commit()
        print(f"Inserted {inserted_count} new incident(s) into Snowflake.")

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
