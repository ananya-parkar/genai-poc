import snowflake.connector
conn = snowflake.connector.connect(
   user='Eshita05',
   password='Eshita@05032003',
   account='av91825.central-india.azure',
    insecure_mode=True
)
print("Connected successfully!")  
conn.close()