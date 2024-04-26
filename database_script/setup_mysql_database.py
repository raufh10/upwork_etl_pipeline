from dotenv import load_dotenv

import os
import mysql.connector

def setup_database_table():
    
    # Load environment variables from .env file
    load_dotenv()

    # Get the credentials
    user = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD')
    print("Credentials loaded successfully.")

    # Establish MySQL connection
    cnx = mysql.connector.connect(user=user, password=password,
                                host='localhost')
        
    cursor = cnx.cursor()

    try:
        # Create a new database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS upwork_data")

        # Use the newly created database
        cursor.execute("USE upwork_data")

        # Create a table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_data (
                job_id BIGINT PRIMARY KEY,
                title VARCHAR(255),
                job_url TEXT,
                domestic_label VARCHAR(255),
                description TEXT,
                project_type VARCHAR(255),
                hours_per_week VARCHAR(255),
                duration VARCHAR(255),
                experience_level VARCHAR(255),
                hourly_rate VARCHAR(255),
                fixed_price VARCHAR(255),
                contract_to_hire BOOLEAN,
                remote_job BOOLEAN,
                skills TEXT
            )
        """)

        print("Database and table setup successfully!")
    except mysql.connector.Error as err:
        print("Error:", err)
    finally:
        # Close cursor and connection
        cursor.close()
        cnx.close()

if __name__ == "__main__":
    setup_database_table()
