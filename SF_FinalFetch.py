import imaplib
import email
import keyring
import snowflake.connector
import re  # Import the regular expression module

# Gmail credentials
USERNAME = 'ananyamehta.work@gmail.com'
APP_PASSWORD = keyring.get_password("my_app", USERNAME)
if not APP_PASSWORD:
    raise ValueError("App password not found in keyring. Please store it first.")

# Connect to Gmail IMAP
mail = imaplib.IMAP4_SSL('imap.gmail.com')
mail.login(USERNAME, APP_PASSWORD)
mail.select('inbox')

# Search for alert emails
status, messages = mail.search(None, '(SUBJECT "alert" SINCE "18-Mar-2025")')
email_ids = messages[0].split()

# Snowflake connection details
SNOWFLAKE_USER = "Eshita05"
SNOWFLAKE_PASSWORD = "Eshita@05032003"
SNOWFLAKE_ACCOUNT = "av91825.central-india.azure.snowflakecomputing.com"  # Corrected account format
SNOWFLAKE_DATABASE = "EMAILFETCH"
SNOWFLAKE_SCHEMA = "email_schema"

# Connect to Snowflake
conn = None  # Initialize conn to None
cur = None   # Initialize cur to None

try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT  # Use the corrected account format
    )
    cur = conn.cursor()

    # Use database and schema
    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE};")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA};")

    # Create a table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS email_alerts (
            subject STRING,
            incident_id STRING,
            impact_level STRING,
            affected_service STRING,
            timestamp STRING,
            description STRING
        );
    """)
    print(f"Found {len(email_ids)} alert emails. Storing in Snowflake...")

    # Fetch and store each email
    for email_id in email_ids:
        status, msg_data = mail.fetch(email_id, '(RFC822)')
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                subject = msg.get('subject', '')

                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == 'text/plain':
                            body = part.get_payload(decode=True).decode()
                            break
                else:
                    body = msg.get_payload(decode=True).decode()

                # Extract data using regular expressions
                incident_id_match = re.search(r"Incident ID: (.*)", body)
                incident_id = incident_id_match.group(1).strip() if incident_id_match else None

                impact_level_match = re.search(r"Impact Level: (.*)", body)
                impact_level = impact_level_match.group(1).strip() if impact_level_match else None

                affected_service_match = re.search(r"Affected Service: (.*)", body)
                affected_service = affected_service_match.group(1).strip() if affected_service_match else None

                timestamp_match = re.search(r"at (\d{2}:\d{2} [AP]M UTC)", body)
                timestamp = timestamp_match.group(1).strip() if timestamp_match else None

                description = body.split(" Incident Details:")[0].strip()

                # Insert into Snowflake
                try:
                    cur.execute("""
                        INSERT INTO email_alerts (subject, incident_id, impact_level, affected_service, timestamp, description)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (subject, incident_id, impact_level, affected_service, timestamp, description))
                    print(f"Stored: {subject} - Incident ID: {incident_id}")
                except Exception as e:
                    print(f"Error inserting data: {e}")

    # Commit the transaction
    conn.commit()

except snowflake.connector.errors.Error as e:
    print(f"Snowflake connection or query error: {e}")
    print(f"Error details: {e}") # Print the full error for debugging
    # Check if the error is SSL related and suggest troubleshooting steps
    if "SSL" in str(e):
        print("It appears to be an SSL-related error. Please ensure that your network allows connections to Snowflake's hostnames and port numbers as listed in SYSTEM$ALLOWLIST.")
        print("Refer to Snowflake's documentation for connectivity troubleshooting: https://docs.snowflake.com/en/user-guide/client-connectivity-troubleshooting/overview")

except Exception as e:
    print(f"An unexpected error occurred: {e}")


finally:
    # Cleanup
    mail.close()
    mail.logout()
    if cur:
        cur.close()
    if conn:
        conn.close()
