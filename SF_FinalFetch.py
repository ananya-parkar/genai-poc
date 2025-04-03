import imaplib
import email
import keyring
import snowflake.connector
from email.header import decode_header
import datetime
 
# Gmail credentials
USERNAME = 'eshitakhare05@gmail.com'
APP_PASSWORD = keyring.get_password("my_app", USERNAME)
if not APP_PASSWORD:
    raise ValueError("App password not found in keyring. Please store it first.")
 
# Connect to Gmail IMAP
mail = imaplib.IMAP4_SSL('imap.gmail.com')
mail.login(USERNAME, APP_PASSWORD)
mail.select('inbox')
 
# Get today's date in the correct IMAP format (DD-MMM-YYYY)
today = datetime.datetime.today().strftime("%d-%b-%Y")
 
# Search for all alert emails received today
status, messages = mail.search(None, f'(SINCE "{today}" SUBJECT "alert")')
email_ids = messages[0].split()
 
# Check if any emails were found
if not email_ids:
    print(f"No alert emails found today ({today}).")
    exit()
 
# Snowflake connection details
SNOWFLAKE_USER = "Eshita05"
SNOWFLAKE_PASSWORD = "Eshita@05032003"
SNOWFLAKE_ACCOUNT = "av91825.central-india.azure"
SNOWFLAKE_DATABASE = "EMAILFETCH"
SNOWFLAKE_SCHEMA = "email_schema"
 
# Connect to Snowflake
conn = None
cur = None
 
try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT
    )
    cur = conn.cursor()
 
    # Use database and schema
    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE};")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA};")
 
    # Create a table if it doesn't exist with additional columns
    cur.execute("""
        CREATE TABLE IF NOT EXISTS email_alertdata (
            subject STRING,
            INCIDENT_ID STRING,
            impact_level STRING,
            affected_service STRING,
            timestamp TIMESTAMP,
            description STRING
        );
    """)
   
    print(f"Fetching {len(email_ids)} alert emails received today ({today})...")
 
    # Fetch and store each alert email found today
    for recent_email_id in email_ids:
        status, msg_data = mail.fetch(recent_email_id, '(RFC822)')
       
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
               
                # Decode the subject properly
                raw_subject = msg.get('Subject', '')
                decoded_subject, encoding = decode_header(raw_subject)[0]
                if isinstance(decoded_subject, bytes):
                    SUBJECT = decoded_subject.decode(encoding if encoding else 'utf-8')
                else:
                    SUBJECT = decoded_subject
               
                sender = msg.get('from', '')
                TIMESTAMP = msg.get('date')
 
                # Initialize variables for new fields
                INCIDENT_ID, IMPACT_LEVEL, AFFECTED_SERVICE, DESCRIPTION = (None,) * 4
 
                if msg.is_multipart():
                    body = ""
                    for part in msg.walk():
                        if part.get_content_type() == 'text/plain' or part.get_content_type() == 'text/html':
                            body = part.get_payload(decode=True).decode('utf-8', errors='ignore')  # Properly decode using UTF-8
                            break
                else:
                    body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')  # Properly decode using UTF-8
 
                # Extract details from the body using simple string operations
                if "Incident ID:" in body:
                    INCIDENT_ID = body.split("Incident ID:")[1].splitlines()[0].strip()
                if "Severity:" in body:
                    IMPACT_LEVEL = body.split("Severity:")[1].splitlines()[0].strip()
                if "Affected System:" in body:
                    AFFECTED_SERVICE = body.split("Affected System:")[1].splitlines()[0].strip()
                   
                # Extract description (text after 'Dear Team,' and before 'Incident Details:')
                if "Dear Team," in body and "Incident Details:" in body:
                    start_index = body.find("Dear Team,") + len("Dear Team,")
                    end_index = body.find("Incident Details:")
                    DESCRIPTION = body[start_index:end_index].strip()  # Get text between 'Dear Team,' and 'Incident Details:'
 
                # Insert into Snowflake
                try:
                    cur.execute("""
                        INSERT INTO email_alertdata (INCIDENT_ID, SUBJECT, IMPACT_LEVEL, AFFECTED_SERVICE, TIMESTAMP, DESCRIPTION)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (INCIDENT_ID, SUBJECT, IMPACT_LEVEL, AFFECTED_SERVICE, TIMESTAMP, DESCRIPTION))
                    print(f"Stored: {SUBJECT} from {sender}")
                except Exception as e:
                    print(f"Error inserting data: {e}")
 
    # Commit the transaction
    conn.commit()
 
except snowflake.connector.errors.Error as e:
    print(f"Snowflake connection or query error: {e}")
    if "SSL" in str(e):
        print("It appears to be an SSL-related error. Please ensure that your network allows connections to Snowflake's hostnames and port numbers as listed in SYSTEM$ALLOWLIST.")
        print("Refer to Snowflake's documentation for connectivity troubleshooting.")
 
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
 
 
 