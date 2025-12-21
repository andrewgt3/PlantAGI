
import redis
import psycopg2
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
CHANNEL = "sensor_stream"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = "5432"
DB_NAME = os.getenv("DT_POSTGRES_DB", "pdm_timeseries")
DB_USER = os.getenv("DT_POSTGRES_USER", "postgres")
DB_PASS = os.getenv("DT_POSTGRES_PASSWORD", "password")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def consume_stream():
    print("🚀 Starting Stream Consumer (Redis -> TimescaleDB)...")
    
    print("📥 Waiting for messages...")
    
    count = 0
    while True:
        try:
            # 1. Connect to Redis (Retry Loop)
            try:
                r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
                pubsub = r.pubsub()
                pubsub.subscribe(CHANNEL)
                # Check connection
                r.ping()
                print(f"✅ (Re)Connected to Redis channel: {CHANNEL}")
            except Exception as e:
                print(f"⚠️ Redis Connection Failed: {e}. Retrying in 5s...")
                import time
                time.sleep(5)
                continue

            # 2. Connect to DB (Retry Loop)
            try:
                 conn = get_db_connection()
                 cur = conn.cursor()
                 print("✅ (Re)Connected to TimescaleDB")
            except Exception as e:
                 print(f"⚠️ DB Connection Failed: {e}. Retrying in 5s...")
                 import time
                 time.sleep(5)
                 continue
            
            # 3. Listen Loop
            print("🟢 Listening execution loop active.")
            try:
                for message in pubsub.listen():
                    if message['type'] == 'message':
                        data = json.loads(message['data'])
                        
                        # Parse
                        ts = data.get('timestamp')
                        mid = data.get('machine_id')
                        speed = data.get('rotational_speed')
                        temp = data.get('temperature_air')
                        torque = data.get('torque')
                        wear = data.get('tool_wear')
                        
                        # Insert
                        query = """
                            INSERT INTO sensor_readings 
                            (timestamp, machine_id, rotational_speed, temperature_air, torque, tool_wear)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        cur.execute(query, (ts, mid, speed, temp, torque, wear))
                        conn.commit()
                        
                        count += 1
                        if count % 100 == 0:
                            print(f"💾 Processed {count} events...", end='\r')
            except Exception as inner_e:
                print(f"❌ Connection Interrupted: {inner_e}")
                raise inner_e # Break to outer retry loop
                
        except KeyboardInterrupt:
            print("\n🛑 Consumer stopped by user.")
            break
        except Exception as e:
            print(f"\n❌ Consumer Error: {e}. Restarting service in 5s...")
            import time
            time.sleep(5)
        finally:
            # Clean up before retry
            try:
                if 'cur' in locals() and cur: cur.close()
                if 'conn' in locals() and conn: conn.close()
            except: pass

if __name__ == "__main__":
    consume_stream()
