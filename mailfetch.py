import configparser
import imaplib
import email
import keyring
import snowflake.connector
from email.header import decode_header
import datetime
import re
from dateutil import parser

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Email settings
EMAIL_USERNAME = config['Email']['username']
EMAIL_APP_PASSWORD = keyring.get_password("my_app", EMAIL_USERNAME)
if not EMAIL_APP_PASSWORD:
    raise ValueError("App password not found in keyring. Please store it using keyring.set_password().")

# Database settings
SNOWFLAKE_USER = config['snowflake']['user']
SNOWFLAKE_PASSWORD = config['snowflake']['password']
SNOWFLAKE_ACCOUNT = config['snowflake']['account']

# General settings
DATABASE_NAME = config['snowflake']['database']
SCHEMA_NAME = config['snowflake']['schema']

def connect_to_gmail():
    """Connect to Gmail via IMAP."""
    mail = imaplib.IMAP4_SSL('imap.gmail.com')
    mail.login(EMAIL_USERNAME, EMAIL_APP_PASSWORD)
    mail.select('inbox')
    return mail

def fetch_emails(mail):
    """Fetch emails from Gmail."""
    today = datetime.datetime.today().strftime("%d-%b-%Y")
    status, messages = mail.search(None, f'(SINCE "{today}" SUBJECT "alert")')
    email_ids = messages[0].split()
    return email_ids[-2:]  # Fetch only the 2 most recent emails

def process_email_content(msg):
    """Extract fields from an email."""
    subject, encoding = decode_header(msg.get("Subject"))[0]
    subject = subject.decode(encoding or 'utf-8') if isinstance(subject, bytes) else subject

    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                break
    else:
        body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')

    def extract_field(text, label):
        match = re.search(rf"{label}:\s*(.*)", text)
        return match.group(1).strip() if match else None

    def clean_datetime(dt_str):
        try:
            dt = parser.parse(dt_str, ignoretz=True)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None

    return {
        "number": extract_field(body, "Incident ID"),
        "config_item": extract_field(body, "Configuration Item"),
        "incident_subject": extract_field(body, "Incident Subject"),
        "business_service": extract_field(body, "Business Service"),
        "opened": clean_datetime(extract_field(body, "Alert Opened")),
        "alert_time": clean_datetime(extract_field(body, "Alert Time")),
        "state": extract_field(body, "State"),
        "closed_by": extract_field(body, "Closed By") or "N/A",
        "closed": clean_datetime(extract_field(body, "Closed")),
        "priority": extract_field(body, "Priority"),
        "ticket_opened_by": "Yes" if extract_field(body, "Ticket Opened") == "Yes" else "No",
        "incident_duration": extract_field(body, "Incident Duration"),
        "action_to_resolve_incident": extract_field(body, "Action to Resolve Incident"),
        "resolution_type": extract_field(body, "Resolution Type"),
        "resolved_by": extract_field(body, "Closed By") or "Pending",
        "short_description": extract_field(body, "Short Description"),
        "next_steps": extract_field(body, "Next Steps"),
    }

def connect_to_snowflake():
    """Connect to Snowflake."""
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT
    )
    return conn

def insert_into_snowflake(conn, data):
    """Insert data into Snowflake."""
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {DATABASE_NAME};")
    cur.execute(f"USE SCHEMA {SCHEMA_NAME};")

    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            Number STRING,
            Configuration_item STRING,
            Incident_Subject STRING,
            Business_Service STRING,
            Opened TIMESTAMP,
            Alert_Time TIMESTAMP,
            State STRING,
            Closed_by STRING,
            Closed TIMESTAMP,
            Priority STRING,
            Ticket_Opened_by STRING,
            Incident_Duration STRING,
            Action_to_Resolve_Incident STRING,
            Resolution_Type STRING,
            Resolved_By STRING,
            Short_Description STRING,
            Next_Steps STRING
        );
    """)

    try:
        cur.execute("""
            INSERT INTO alerts (
                Number, Configuration_item, Incident_Subject, Business_Service,
                Opened, Alert_Time, State, Closed_by, Closed, Priority,
                Ticket_Opened_by, Incident_Duration, Action_to_Resolve_Incident,
                Resolution_Type, Resolved_By, Short_Description, Next_Steps
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data["number"], data["config_item"], data["incident_subject"], data["business_service"],
            data["opened"], data["alert_time"], data["state"], data["closed_by"], data["closed"], data["priority"],
            data["ticket_opened_by"], data["incident_duration"], data["action_to_resolve_incident"],
            data["resolution_type"], data["resolved_by"], data["short_description"], data["next_steps"]
        ))
        print("Inserted alert successfully.")
    except Exception as e:
        print(f"Insert failed: {e}")

def main():
    print("Starting email fetching process...")
    mail = connect_to_gmail()
    email_ids = fetch_emails(mail)

    if not email_ids:
        print("No new alert emails found today.")
        mail.close()
        mail.logout()
        return False

    conn = connect_to_snowflake()
    
    for email_id in email_ids:
        status, msg_data = mail.fetch(email_id, '(RFC822)')
        
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                alert_data = process_email_content(msg)
                insert_into_snowflake(conn=conn, data=alert_data)

    conn.commit()
    
    mail.close()
    mail.logout()
    return True  # Return True if any emails were processed

if __name__ == "__main__":
    main()
