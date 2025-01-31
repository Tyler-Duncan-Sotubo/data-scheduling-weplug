import os
import psycopg2
import uuid
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv
from data import RELEASE_ISRC
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
BASE_SALES_API_URL = "https://sales.dittomusic.com/sales-area/track-sales/tracks"
TOKEN = os.getenv("API_TOKEN")  # Use environment variable for the token

# Release to ISRC Mapping


# Headers
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": "https://sales.dittomusic.com/",
    "Origin": "https://sales.dittomusic.com",
}

# Query Parameters
PARAMS = {
    "groupBy": "months",
    "sortColumn": "earnings",
    "sortOrder": "desc",
    "limit": 99999,
    "offset": 0,
}

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
            CREATE TABLE IF NOT EXISTS "SalesByMonth" (
                "id" UUID PRIMARY KEY,
                "audioId" TEXT NOT NULL,
                "ISRC" TEXT NOT NULL,
                "date" TEXT NOT NULL,
                "month" INTEGER NOT NULL,
                "year" INTEGER NOT NULL,
                "streams" INTEGER NOT NULL,
                "trackDownloads" INTEGER NOT NULL,
                "totalSales" INTEGER NOT NULL,
                "earnings" NUMERIC NOT NULL,
                UNIQUE ("audioId", "ISRC", "month", "year")
            )
        ''')
        conn.commit()
        cursor.close()
        print("Database setup completed.")
    except Exception as e:
        print(f"Error setting up database: {e}")

# Insert or update sales data by month
def insert_or_update_data(conn, rows):
    cursor = conn.cursor()
    query = '''
        INSERT INTO "SalesByMonth" ("id", "audioId", "ISRC", "date", "month", "year", "streams", "trackDownloads", "totalSales", "earnings")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("audioId", "ISRC", "month", "year") 
        DO UPDATE SET
            "streams" = EXCLUDED."streams",
            "trackDownloads" = EXCLUDED."trackDownloads",
            "totalSales" = EXCLUDED."totalSales",
            "earnings" = EXCLUDED."earnings"
    '''
    for row in rows:
        row_id = str(uuid.uuid4())
        cursor.execute(query, (row_id, row["audioId"], row["ISRC"], row["date"], row["month"], row["year"],
                               row["streams"], row["trackDownloads"], row["totalSales"], row["earnings"]))
    conn.commit()
    cursor.close()
    print(f"{len(rows)} sales by month rows upserted successfully.")

# Transform data function
def transform_sales_data(sales_data, audio_id, isrc):
    transformed_data = []
    if "data" in sales_data and sales_data["data"]:
        for entry in sales_data["data"]:
            transformed_data.append({
                "audioId": audio_id,
                "ISRC": isrc,  # Include ISRC in the transformed data
                "date": entry["date"],
                "month": entry["month"],
                "year": entry["year"],
                "streams": entry["streams"],
                "trackDownloads": entry["trackDownloads"],
                "totalSales": entry["totalSales"],
                "earnings": entry["earnings"]
            })
    return transformed_data

# Fetch and transform sales data
def fetch_and_insert_data():
    conn = connect_to_database()
    if not conn:
        print("Unable to connect to the database.")
        return

    setup_database(conn)
    try:
        for release in RELEASE_ISRC:
            isrc = release["ISRC"]
            audio_id = release["id"]

            # Construct API URL dynamically
            sales_api_url = f"{BASE_SALES_API_URL}/{isrc}"
            response = requests.get(sales_api_url, headers=HEADERS, params=PARAMS)

            if response.status_code == 200:
                sales_data = response.json()
                print(f"Sales data for ISRC {isrc} fetched successfully.")

                # Check if data is not empty
                if "data" in sales_data and sales_data["data"]:
                    # Transform data using the new function
                    transformed_data = transform_sales_data(sales_data, audio_id, isrc)

                    if transformed_data:  # Ensure transformed data is not empty
                        insert_or_update_data(conn, transformed_data)
                    else:
                        print(f"No valid data to transform for ISRC {isrc}.")
                else:
                    print(f"No data available for ISRC {isrc}.")
            else:
                print(f"Failed to fetch sales data for ISRC {isrc}. Status: {response.status_code}, Response: {response.text}")

    except Exception as e:
        print(f"Error during data fetch and insert: {e}")
    finally:
        conn.close()

