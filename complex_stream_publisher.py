
import redis
import pandas as pd
import numpy as np
import json
import time
import os
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pickle

load_dotenv()

# Configuration
CSV_PATH = "data/feature_store.csv"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
CHANNEL = "sensor_stream"

def stream_merged_data():
    print(f"🚀 Starting Complex Stream Publisher (Feature Store)...")
    
    # 1. Connect to Redis
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        r.ping()
        print(f"✅ Connected to Redis channel: {CHANNEL}")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        return

    # 2. Load Feature Store (Single Source of Truth)
    print(f"📖 Loading Feature Store from {CSV_PATH}...")
    try:
        if not os.path.exists(CSV_PATH):
             print(f"❌ CSV not found: {CSV_PATH}")
             return
        df = pd.read_csv(CSV_PATH)
        # Ensure timestamp is string/ISO
        # df['timestamp'] = pd.to_datetime(df['timestamp']).astype(str) 
        print(f"   ✅ Loaded {len(df)} records with IDs: {df['machine_id'].unique()[:5]}...")
    except Exception as e:
        print(f"❌ Error loading Feature Store: {e}")
        return
    
    # 3. Stream Loop
    print(f"📡 Streaming {len(df)} records...")
    sleep_delay = 0.1 # 10Hz
    
    try:
        # Loop indefinitely for demo purposes (restart from top)
        while True:
            for idx, row in df.iterrows():
                # Construct Payload from CSV columns
                # Schema matches DB expected by Consumer
                
                # Check for NaNs and handle
                row = row.fillna(0)
                
                payload = {
                    "timestamp": str(row.get('timestamp', datetime.utcnow().isoformat())),
                    "machine_id": str(row['machine_id']),
                    "temperature_air": float(row.get('temperature_air', 300.0)),
                    "torque": float(row.get('torque', 0.0)),
                    "pressure": float(row.get('pressure', 1000.0)), # Default if missing
                    "vibration_rms": float(row.get('vibration_rms', 0.0)), # Could be 0 if absent in CSV, assuming present or computed
                    "rotational_speed": float(row.get('rotational_speed', 0.0)),
                    "tool_wear": float(row.get('tool_wear', 0.0)),
                    "current_tool_wear_pct": float(row.get('current_tool_wear_pct', 0.0)),
                    "criticality_rating": str(row.get('criticality_rating', 'C')),
                    "sensor_data": {
                        "Speed": float(row.get('rotational_speed', 0)),
                        "Vibration": float(row.get('vibration_rms', 0)), # Ensure CSV has this or we mock it
                        "Pressure": float(row.get('pressure', 1000)),
                        "Tool Wear": float(row.get('tool_wear', 0)),
                        "Health": 100 * (1.0 - float(row.get('current_tool_wear_pct', 0)))
                    },
                     # Mock machine_failure for training vs inference? Inference usually predicts it.
                     # But we pass it if known for validation
                    "machine_failure": int(row.get('machine_failure', 0))
                }
                
                r.publish(CHANNEL, json.dumps(payload))
                
                if idx % 100 == 0:
                     print(f"📡 {idx} sent. ID={payload['machine_id']}", end='\r')
                
                time.sleep(sleep_delay)
            
            print("\n🔄 Restarting stream loop...")
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Stream stopped.")
    except Exception as e:
        print(f"\n❌ Stream Error: {e}")

if __name__ == "__main__":
    stream_merged_data()
