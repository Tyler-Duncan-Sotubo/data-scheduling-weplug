import os
import psycopg2
import uuid
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Parse the DATABASE_URL
db_url = os.getenv("DATABASE_URL")
parsed_url = urlparse(db_url)

# Extract connection details
DB_NAME = parsed_url.path[1:]  # Remove leading slash
DB_USER = parsed_url.username
DB_PASSWORD = parsed_url.password
DB_HOST = parsed_url.hostname
DB_PORT = parsed_url.port or 5432  # Default to 5432 if not set

# Connect to PostgreSQL
def connect_to_database():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Database connection successful!")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Initialize database table
def setup_database():
    conn = connect_to_database()
    if conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS streams (
                id UUID PRIMARY KEY,
                dsp_id TEXT NOT NULL,
                total INTEGER NOT NULL,
                date DATE NOT NULL,
                audio_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        cursor.close()
        conn.close()
        print("Database setup completed.")
    else:
        print("Failed to setup database.")

# Insert multiple rows into the `streams` table
def insert_data(rows):
    conn = connect_to_database()
    if conn:
        cursor = conn.cursor()
        try:
            # Prepare the insert query
            query = '''
                INSERT INTO streams (id, dsp_id, total, date, audio_id)
                VALUES (%s, %s, %s, %s, %s)
            '''
            # Generate UUIDs and insert each row
            for row in rows:
                row_id = str(uuid.uuid4())
                cursor.execute(query, (row_id, row["dsp_id"], row["total"], row["date"], row["audio_id"]))
            conn.commit()
            print(f"{len(rows)} rows inserted successfully.")
        except Exception as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    else:
        print("Failed to insert data.")


def record_exists(date, dsp_id):
    conn = connect_to_database()
    if conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM streams
            WHERE date = %s AND dsp_id = %s
        ''', (date, dsp_id))
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count > 0
    else:
        print("Failed to check if record exists.")
        return False