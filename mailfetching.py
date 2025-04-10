#%%
import imaplib
import email
import keyring
import snowflake.connector
from email.header import decode_header
import datetime
import re
from dateutil import parser

# Credentials
USERNAME = 'anshika.ranjan0926@gmail.com'
APP_PASSWORD = keyring.get_password("my_app", USERNAME)
if not APP_PASSWORD:
    raise ValueError("App password not found in keyring. Please store it using keyring.set_password().")
def main():
    # Connect to Gmail

    mail = imaplib.IMAP4_SSL('imap.gmail.com')
    mail.login(USERNAME, APP_PASSWORD)
    mail.select('inbox')

    # Get today’s date in IMAP format
    today = datetime.datetime.today().strftime("%d-%b-%Y")
    status, messages = mail.search(None, f'(SINCE "{today}" SUBJECT "alert")')
    email_ids = messages[0].split()
    email_ids = email_ids[-2:]  # Fetch only the 2 most recent
    if not email_ids:
        print("No new alert emails found today.")
        mail.close()
        mail.logout()
        return False  # No new alerts

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
            Resolution_Type  STRING,
            Resolved_By STRING,
            Short_Description STRING,
            Next_Steps STRING
        );
    """)
    def extract_field(text, label):
        match = re.search(rf"{label}:\s*(.*)", text)
        return match.group(1).strip() if match else None
    
    def clean_datetime(dt_str):
        try:
            dt = parser.parse(dt_str, ignoretz=True)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    inserted_count = 0
    # Process emails

    for email_id in email_ids:

        status, msg_data = mail.fetch(email_id, '(RFC822)')

        for response_part in msg_data:

            if isinstance(response_part, tuple):

                msg = email.message_from_bytes(response_part[1])

                subject, encoding = decode_header(msg.get("Subject"))[0]

                subject = subject.decode(encoding or 'utf-8') if isinstance(subject, bytes) else subject

                if msg.is_multipart():

                    for part in msg.walk():

                        if part.get_content_type() == "text/plain":

                            body = part.get_payload(decode=True).decode('utf-8', errors='ignore')

                            break

                else:

                    body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')

                number = extract_field(body, "Incident ID")

                config_item = extract_field(body, "Configuration Item")

                incident_subject = extract_field(body, "Incident Subject")

                business_service = extract_field(body, "Business Service")

                opened = clean_datetime(extract_field(body, "Alert Opened"))

                alert_time = clean_datetime(extract_field(body, "Alert Time"))

                state = extract_field(body, "State")

                closed_by = extract_field(body, "Closed By") or "N/A"

                closed = clean_datetime(extract_field(body, "Closed"))

                priority = extract_field(body, "Priority")

                ticket_opened_by = "Yes" if extract_field(body, "Ticket Opened") == "Yes" else "No"

                incident_duration = extract_field(body, "Incident Duration")

                action = extract_field(body, "Action to Resolve Incident")

                resolution_TYPE = extract_field(body, "Resolution Type")

                resolved_by = closed_by if closed_by != "N/A" else "Pending"

                short_description = extract_field(body, "Short Description")

                next_steps = extract_field(body, "Next Steps")

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

                        ticket_opened_by, incident_duration, action, resolution_TYPE,

                        resolved_by, short_description, next_steps

                    ))

                    print(f"Inserted: {subject}")

                    inserted_count += 1

                except Exception as e:

                    print(f"Insert failed: {e}")

    conn.commit()
    mail.close()
    mail.logout()
    cur.close()
    conn.close()
    return inserted_count > 0  # True if any rows inserted
#%%
if __name__ == "__main__":

    main() 



