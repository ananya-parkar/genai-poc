import imaplib
import email
import keyring
import snowflake.connector
from email.header import decode_header
import datetime
import re
from dateutil import parser
 
# Credentials
USERNAME = 'eshitakhare05@gmail.com'
APP_PASSWORD = keyring.get_password("my_app", USERNAME)
 
if not APP_PASSWORD:
    raise ValueError("App password not found in keyring. Please store it using keyring.set_password().")
 
# Connect to Gmail
mail = imaplib.IMAP4_SSL('imap.gmail.com')
mail.login(USERNAME, APP_PASSWORD)
mail.select('inbox')
 
# Today’s date
today = datetime.datetime.today().strftime("%d-%b-%Y")
status, messages = mail.search(None, f'(SINCE "{today}" SUBJECT "alert")')
email_ids = messages[0].split()
email_ids = email_ids[-1:]  # Most recent email
 
if not email_ids:
    print("No alert emails found.")
    exit()
 
# Snowflake connection
conn = snowflake.connector.connect(
    user="Eshita05",
    password="Eshita@05032003",
    account="av91825.central-india.azure"
)
 
cur = conn.cursor()
cur.execute("USE DATABASE EMAILFETCH;")
cur.execute("USE SCHEMA email_schema;")
 
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
 
# Helper to extract and clean
def extract_field(text, label):
    pattern = rf"{label}\s*:\s*(.+)"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        value = match.group(1).strip()
        value = value.split('\n')[0].strip()
        return value
    return None
 
# Clean date string for parsing
def clean_datetime(dt_str):
    try:
        if dt_str is None:
            return None
        cleaned = re.sub(r'(\s*\b[A-Z]{2,}\b)$', '', dt_str.strip())  # remove 'IST'
        cleaned = re.sub(r',\s*(\d{2}:\d{2}\s*[APMapm]{2})', r' \1', cleaned)  # remove comma before time
        return parser.parse(cleaned, fuzzy=True).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Date parsing failed for '{dt_str}': {e}")
        return None
 
# Process emails
for email_id in email_ids:
    status, msg_data = mail.fetch(email_id, '(RFC822)')
    for response_part in msg_data:
        if isinstance(response_part, tuple):
            msg = email.message_from_bytes(response_part[1])
            subject, encoding = decode_header(msg.get("Subject"))[0]
            subject = subject.decode(encoding or 'utf-8') if isinstance(subject, bytes) else subject
 
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                        break
            else:
                body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
 
            # Extract fields
            number = extract_field(body, "Number")
            config_item = extract_field(body, "Configuration Item")
            incident_subject = extract_field(body, "Incident Subject")
            business_service = extract_field(body, "Business Service")
 
            opened = clean_datetime(extract_field(body, "Opened"))
 
            alert_time_raw = extract_field(body, "Alert Time")
            print(f"Alert Time Raw Extracted: {alert_time_raw}")
            alert_time = clean_datetime(alert_time_raw)
 
            state = extract_field(body, "State")
            closed_by = extract_field(body, "Closed By") or "N/A"
            closed = clean_datetime(extract_field(body, "Closed"))
            priority = extract_field(body, "Priority")
            ticket_opened_by = "Yes" if extract_field(body, "Ticket Opened by") == "Yes" else "No"
 
            duration_raw = extract_field(body, "Incident Duration")
            incident_duration = ''.join(filter(str.isdigit, duration_raw)) if duration_raw else "0"
 
            action = extract_field(body, "Action to Resolve Incident")
            resolution_type = extract_field(body, "Resolution Type")
            resolved_by = extract_field(body, "Resolved By") or closed_by
            short_description = extract_field(body, "Short Description")
            next_steps = extract_field(body, "Next Steps")
 
            # Insert into Snowflake
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
                    number, config_item, incident_subject, business_service,
                    opened, alert_time, state, closed_by, closed, priority,
                    ticket_opened_by, incident_duration, action,
                    resolution_type, resolved_by, short_description, next_steps
                ))
                print(f"Inserted: {subject}")
            except Exception as e:
                print(f"Insert failed: {e}")
 
# Cleanup
conn.commit()
mail.close()
mail.logout()
cur.close()
conn.close()
 