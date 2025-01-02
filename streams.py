import os
import psycopg2
import uuid
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv
from data import DSP_MAPPING, RELEASES
import schedule
import time

# Load environment variables
load_dotenv()

# Parse the DATABASE_URL
db_url = os.getenv("DATABASE_URL")
parsed_url = urlparse(db_url)

# Extract connection details
DB_NAME = parsed_url.path[1:]
DB_USER = parsed_url.username
DB_PASSWORD = parsed_url.password
DB_HOST = parsed_url.hostname
DB_PORT = parsed_url.port or 5432

# API Configuration
API_URL = "https://trends.dittomusic.com/api/v1/trends/stores/stream"
TOKEN = os.getenv("API_TOKEN")  # Use environment variable for the token

# Headers
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": "https://dashboard.dittomusic.com/",
    "Origin": "https://dashboard.dittomusic.com",
}


# Match DSP names to DSP IDs
dsp_mapping = {dsp["name"]: dsp["id"] for dsp in DSP_MAPPING}

# Connect to PostgreSQL
def connect_to_database():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        print("Database connection successful!")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Initialize database table
def setup_database(conn):
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "Streams" (
                "id" UUID PRIMARY KEY,
                "dspId" TEXT NOT NULL,
                "total" INTEGER NOT NULL,
                "date" TIMESTAMP NOT NULL,
                "audioId" TEXT NOT NULL
            )
        ''')
        conn.commit()
        cursor.close()
        print("Database setup completed.")
    except Exception as e:
        print(f"Error setting up database: {e}")

# Check if record exists
def record_exists(conn, date, dsp_id, audio_id):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*) FROM "Streams"
        WHERE "date" = %s AND "dspId" = %s AND "audioId" = %s
    ''', (date, dsp_id, audio_id))
    exists = cursor.fetchone()[0] > 0
    cursor.close()
    return exists

# Insert multiple rows into the `Streams` table
def insert_data(conn, rows):
    cursor = conn.cursor()
    query = '''
        INSERT INTO "Streams" ("id", "dspId", "total", "date", "audioId")
        VALUES (%s, %s, %s, %s, %s)
    '''
    for row in rows:
        row_id = str(uuid.uuid4())
        cursor.execute(query, (row_id, row["dspId"], row["total"], row["date"], row["audioId"]))
    conn.commit()
    cursor.close()


# Transform API data
def transform_data(api_data, audio_id):
    transformed_data = []
    for dsp_data in api_data["data"]:
        dsp_name = dsp_data["key"]
        dsp_id = dsp_mapping.get(dsp_name)
        if not dsp_id:
            continue
        for entry in dsp_data["series"]:
            transformed_data.append({
                "dspId": dsp_id,
                "total": entry["total"],
                "date": entry["date"],
                "audioId": audio_id,
            })
    return transformed_data

# Fetch data from API and insert into the database
def fetch_and_insert_data():
    conn = connect_to_database()
    if not conn:
        print("Unable to connect to the database.")
        return

    setup_database(conn)

    try:
        for release in RELEASES:
            release_id = release["release_id"]
            audio_id = release["id"]
            params = {"days": 1, "releases[]": release_id}

            response = requests.get(API_URL, headers=HEADERS, params=params)

            if response.status_code == 200:
                api_data = response.json()
                print(f"Data fetched successfully for release_id: {release_id}")

                # Check if data is not empty
                if "data" in api_data and api_data["data"]:
                    # Transform data
                    transformed_data = transform_data(api_data, audio_id)

                    # Insert data into the database
                    if transformed_data:  # Ensure transformed data is not empty
                        for record in transformed_data:
                            if not record_exists(conn, record["date"], record["dspId"], record["audioId"]):
                                insert_data(conn, [record])
                                print("Rows inserted successfully.")
                    else:
                        print(f"No valid data to transform for release_id {release_id}.")
                else:
                    print(f"No data available for release_id {release_id}.")
            else:
                print(f"Failed to fetch data for release_id {release_id}. Status: {response.status_code}, Response: {response.text}")

    except Exception as e:
        print(f"Error during data fetch and insert: {e}")
    finally:
        conn.close()

# Schedule the task to run every 24 hours
def schedule_task():
    schedule.every(24).hours.do(fetch_and_insert_data)
    print("Scheduled task to run every 24 hours.")

    while True:
        schedule.run_pending()
        time.sleep(1)

# Main Execution
if __name__ == "__main__":
    fetch_and_insert_data()