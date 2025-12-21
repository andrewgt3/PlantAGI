import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DT_POSTGRES_DB", "pdm_timeseries")
DB_USER = os.getenv("DT_POSTGRES_USER", "postgres")
DB_PASS = os.getenv("DT_POSTGRES_PASSWORD", "password")

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cur = conn.cursor()
    
    # Check count for specific ID
    target_id = "H29457"
    cur.execute("SELECT COUNT(*) FROM sensor_readings WHERE machine_id = %s", (target_id,))
    count = cur.fetchone()[0]
    
    # Check latest timestamp
    cur.execute("SELECT MAX(timestamp) FROM sensor_readings WHERE machine_id = %s", (target_id,))
    latest = cur.fetchone()[0]
    
    print(f"ID: {target_id}")
    print(f"Row Count: {count}")
    print(f"Latest Timestamp: {latest}")
    
    # Check total rows
    cur.execute("SELECT COUNT(*) FROM sensor_readings")
    total = cur.fetchone()[0]
    print(f"Total Rows in DB: {total}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
