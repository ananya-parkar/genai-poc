# main.py
 
import SNFetch
import SNRCA
import SNSF
 
def main():
    print("\n=== Starting Alert Processing Pipeline ===")
 
    # Step 1: Prompt user and fetch/insert incident by incident number
    print("\n[1] Fetching incident from ServiceNow and inserting into Snowflake (if not already present)...")
    incident_number = SNFetch.main()  # This will handle user input and insertion/skipping
    if not incident_number:
        print("No incident number provided. Exiting.")
        return

 
    # Step 2: Generate RCA for unprocessed alerts (processed=FALSE)
    print("\n[2] Generating RCA for unprocessed alerts...")
    SNRCA.main(incident_number)
    print("RCA generation completed.")
 
    # Step 3: Generate or reuse solutions for alerts and mark as processed
    print("\n[3] Generating or reusing solutions for alerts...")
    SNSF.main(incident_number)
    print("Solution generation and storage completed.")
 
    print("\n=== Pipeline finished ===")
 
if __name__ == "__main__":
    main()