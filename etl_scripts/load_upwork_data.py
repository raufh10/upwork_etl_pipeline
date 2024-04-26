from dotenv import load_dotenv

import os
import json
import mysql.connector

def main():

    file_path = r'/home/raufhamidy/Documents/upwork_etl_pipeline/job_data.json'

    try:
        load_data(file_path)
        delete_file(file_path)
    except Exception as e:
        print(f"An error occurred: {e}")

def load_data(file_path):

    print("Loading data from JSON file to MySQL database...")

    # Load data from JSON file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print("Data loaded successfully.")

    # Load environment variables from .env file
    load_dotenv()

    # Get the credentials
    user = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD')
    print("Credentials loaded successfully.")

    # Establish MySQL connection
    cnx = mysql.connector.connect(user=user, password=password,
                                host='localhost',
                                database='upwork_data')
    print("Connected to MySQL database.")

    cursor = cnx.cursor()

    print("Inserting data into MySQL database...")
    # Iterate over the list of dictionaries
    for item in data:

        # Prepare SQL INSERT statement
        placeholders = ', '.join(['%s'] * len(item))
        columns = ', '.join(item.keys())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % ('job_data', columns, placeholders)

        # Execute the SQL statement
        try:
            cursor.execute(sql, list(item.values()))
        except mysql.connector.Error as e:
            print(f"An error occurred: {e}")

    # Commit the transaction
    cnx.commit()
    print("Data loading process completed successfully.")

    # Close the cursor and connection
    cursor.close()
    cnx.close()
    print("Connection closed.")

def delete_file(file_path):
    # Use OS to delete the file job_data.json
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"The file {file_path} has been deleted.")
    else:
        print(f"The file {file_path} does not exist.")

if __name__ == '__main__':
    main()
