import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import os
from dotenv import load_dotenv



class SnowflakeConnector:
    
    def __init__(self):
        # load variables from .env
        load_dotenv()
        self.key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")

    def establish_connection(self):

        with open(self.key_path, "rb") as key_file:
            self.p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

        pkb = self.p_key.private_bytes(
            encoding=serialization.Encoding.DER,  # Translate this to binary
            format=serialization.PrivateFormat.PKCS8, # Structure it the way Snowflake accepts
            encryption_algorithm=serialization.NoEncryption()
        )

        print("Connecting to Snowflake...")
        try:
            self.conn = snowflake.connector.connect(
                user='PYTHON_CONNECTOR_USER', 
                account=os.getenv("SNOWFLAKE_ACCOUNT"), 
                private_key=pkb,
                role='SYSADMIN',
                warehouse='UFC_WH',
                database='UFC_ANALYTICS',
                schema='RAW'
            )
            
            print("Successfully connected to Snowflake using Key-Pair Auth")
        
        except Exception as e:
            print(f"Error while connecting to Snowflake: {e}")

    def get_max_event_date(self):

        self.cursor = self.conn.cursor()
        
        try:
            self.cursor.execute("SELECT max(event_date) FROM UFC_ANALYTICS.RAW.RAW_UFC_EVENTS")
            max_date = self.cursor.fetchone()[0]
            return max_date
        except Exception as e:
            print(f"Error while getting max date: {e}")
        finally:
            self.cursor.close()

    def close_connection(self):
        print("Closing Snowflake connection...")
        self.conn.close()
        print("Snowlfake Connection closed")

    


    




