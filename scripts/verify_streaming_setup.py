
import redis
import psycopg2
import os
import time

def check_env():
    print("🚀 Verifying Streaming Data Environment...")
    
    # Check Redis
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✅ Redis Connection Successful (Pong received)")
    except Exception as e:
        print(f"❌ Redis Connection Failed: {e}")

    # Check Postgres
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            dbname=os.getenv("DT_POSTGRES_DB", "pdm_timeseries"),
            user=os.getenv("DT_POSTGRES_USER", "postgres"),
            password=os.getenv("DT_POSTGRES_PASSWORD", "password")
        )
        print("✅ Postgres Connection Successful")
        conn.close()
    except Exception as e:
        print(f"❌ Postgres Connection Failed: {e}")
        
    print("✅ 'time' library imported successfully.")

if __name__ == "__main__":
    check_env()
