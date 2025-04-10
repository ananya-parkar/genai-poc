# main.py
import mailfetching
import jsonconv
import solutionfaiss
#import rca
def main():
   print("Starting alert pipeline...")
   new_alerts_found = mailfetching.main()  
   if not new_alerts_found:
       print("No new alerts found today. Skipping the rest of the pipeline.")
       return
   print("New alerts found. Proceeding to JSON conversion...")
   jsonconv.main()
   #print("Proceeding to RCA generation...")
   #rca.main()
   print("Proceeding to generate or reuse solutions...")
   solutionfaiss.main()
if __name__ == "__main__":
   main()