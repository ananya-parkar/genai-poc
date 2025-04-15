# jsonconv.py
import snowflake.connector
import json
from datetime import datetime
import configparser

def main():
    """
    Fetches data from Snowflake, converts it to JSON format,
    and saves it to a file.
    """

    def json_serializer(obj):
        """Custom serializer for non-JSON-native objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    # Read Snowflake connection details from config.ini
    config = configparser.ConfigParser()
    config.read('config.ini')

    SNOWFLAKE_USER = config['snowflake']['user']
    SNOWFLAKE_PASSWORD = config['snowflake']['password']
    SNOWFLAKE_ACCOUNT = config['snowflake']['account']
    SNOWFLAKE_WAREHOUSE = config['snowflake']['warehouse']
    SNOWFLAKE_DATABASE = config['snowflake']['database']
    SNOWFLAKE_SCHEMA = config['snowflake']['schema']

    # Establish Snowflake connection
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()

        # Fetch data from Snowflake
        try:
            cursor.execute("SELECT * FROM rca_results")  # Your RCA results table
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            # Convert to JSON with datetime handling
            rca_results = [dict(zip(columns, row)) for row in rows]
            rca_json = json.dumps(rca_results,
                                  default=json_serializer,
                                  indent=4)

            # Save JSON to file
            try:
                with open("rca_results.json", "w") as f:
                    f.write(rca_json)
                print("JSON data saved to rca_results.json")

            except Exception as e:
                print(f"Error saving JSON to file: {e}")

        except Exception as e:
            print(f"Error fetching data from Snowflake: {e}")

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")

if __name__ == "__main__":
    main()
