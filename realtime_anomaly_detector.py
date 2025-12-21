
import os
import json
import redis
import joblib
import numpy as np
import pandas as pd
from pymongo import MongoClient
from sklearn.neighbors import LocalOutlierFactor
from datetime import datetime
import warnings
# Import Topology Graph
try:
    from services.inference.app.dependency_graph import plant_graph
except ImportError:
    # Fallback if running from root without package structure
    import sys
    sys.path.append(os.getcwd())
    from services.inference.app.dependency_graph import plant_graph


# Suppress warnings
warnings.filterwarnings("ignore")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
CHANNEL = "sensor_stream"

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASS = os.getenv("MONGO_PASSWORD", "password")

MODEL_DIR = "models"
ISO_MODEL_PATH = f"{MODEL_DIR}/isolation_forest.pkl"
CONFIG_PATH = f"{MODEL_DIR}/model_config.json"

# Features expected by IF model
FEATURES = ['vibration_rms', 'temperature_air', 'torque', 'pressure', 'rotational_speed', 'tool_wear']

class RealTimeDetector:
    def __init__(self):
        print("🔧 Initializing Real-Time Hybrid Detector...")
        
        # 1. Connect Redis
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        self.pubsub = self.r.pubsub()
        self.pubsub.subscribe(CHANNEL)
        print(f"   ✅ Redis connected (Channel: {CHANNEL})")
        
        # 2. Connect MongoDB
        self.mongo = MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/")
        self.db = self.mongo["pdm_logs"]
        self.coll = self.db["anomaly_events"]
        self.audit_coll = self.db["model_audit_log"] # [NEW] Audit Log
        print("   ✅ MongoDB connected")
        
        # 3. Load Models & Config
        self.iso_forest = joblib.load(ISO_MODEL_PATH)
        print("   ✅ Isolation Forest loaded")
        
        with open(CONFIG_PATH, "r") as f:
            self.config = json.load(f)
            
        self.spc_stats = self.config.get("spc", {})
        print(f"   ✅ SPC Stats loaded: {self.spc_stats}")
        
        # 4. State Buffers
        self.lof_window = []  # Sliding window for LOF (last 50 points)
        self.lof_model = LocalOutlierFactor(n_neighbors=20, contamination=0.1)
        
        self.spc_counters = {
            "torque": 0,
            "temp": 0
        }
        
    def process_stream(self):
        print("🚀 Detector Service Running...")
        while True:
            try:
                # Re-check/Connect Redis if not connected
                try:
                    self.r.ping()
                except redis.exceptions.ConnectionError:
                    print("⚠️ Redis lost. Attempting to reconnect...")
                    self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
                    self.pubsub = self.r.pubsub()
                    self.pubsub.subscribe(CHANNEL)
                    print("✅ Redis Reconnected.")

                for message in self.pubsub.listen():
                    if message['type'] == 'message':
                        try:
                            data = json.loads(message['data'])
                            self.analyze(data)
                        except Exception as e:
                            print(f"❌ Error processing message: {e}")
            except redis.exceptions.ConnectionError:
                print("❌ Redis connection dropped. Retrying in 5s...")
                import time
                time.sleep(5)
            except Exception as e:
                print(f"❌ Unexpected Consumer Error: {e}. Retrying in 5s...")
                import time
                time.sleep(5)

    def analyze(self, data):
        # 1. Feature Extraction (Handle defaults)
        # Expected: ['vibration_rms', 'temperature_air', 'torque', 'pressure', 'rotational_speed', 'tool_wear']
        
        # Extract
        try:
            feats = {
                'vibration_rms': float(data.get('vibration_rms', 0.0)),
                'temperature_air': float(data.get('temperature_air', 300.0)),
                'torque': float(data.get('torque', 0.0)),
                'pressure': float(data.get('pressure', 1000.0)),
                # Defaults matches training script
                'rotational_speed': float(data.get('rotational_speed', 2000.0)), 
                'tool_wear': float(data.get('tool_wear', 0.0))
            }
            
            # Vector for Model
            vector = np.array([[feats[f] for f in FEATURES]])
            
        except Exception as e:
            # print(f"Skipping malformed record: {e}")
            return

        timestamp = data.get('timestamp') or datetime.utcnow().isoformat()
        machine_id = data.get('machine_id', 'unknown')
        alerts = []

        # --- A. Isolation Forest (Global Anomaly) ---
        # Predict: 1 = Inlier, -1 = Outlier
        iso_pred = self.iso_forest.predict(vector)[0]
        if iso_pred == -1:
            alerts.append({
                "type": "IF_ANOMALY",
                "severity": "warning",
                "message": "Global multivariate anomaly detected (Isolation Forest)",
                "score": -1.0,
                "rca_context": plant_graph.get_upstream_dependencies(machine_id)
            })

        # --- B. Streaming LOF (Local Outlier) ---
        # Update Buffer
        self.lof_window.append(vector[0])
        if len(self.lof_window) > 50:
            self.lof_window.pop(0)
            
        # Run LOF if buffer is full enough
        if len(self.lof_window) >= 25:
            # We fit on window and check if the *last* point (current) is outlier
            # LOF doesn't have predict(), it has fit_predict(). 
            # We are asking: is this NEW point an outlier relative to recent history?
            # Efficient way: fit_predict on window. Last element status.
            X_window = np.array(self.lof_window)
            lof_preds = self.lof_model.fit_predict(X_window)
            
            if lof_preds[-1] == -1:
                 # Calculate score (negative_outlier_factor_)
                 # factor < -1.5 is usually anomaly
                 factor = -self.lof_model.negative_outlier_factor_[-1]
                 if factor > 1.5:
                     alerts.append({
                        "type": "LOF_ANOMALY",
                        "severity": "info", # LOF is noisy
                        "message": f"Local deviation detected (Factor: {factor:.2f})",
                        "score": float(factor),
                        "rca_context": plant_graph.get_upstream_dependencies(machine_id)
                     })

        # --- C. SPC (Statistical Process Control) ---
        # 3-Sigma Rule: Value > Mean + 3*Std
        
        # Torque
        t_mean = self.spc_stats.get('torque_mean', 100)
        t_std = self.spc_stats.get('torque_std', 0.5) 
        # Handle 0 std (unlikely with noise injection, but safety first)
        if t_std < 0.01: t_std = 0.01
        
        t_ucl = t_mean + (3 * t_std)
        t_lcl = t_mean - (3 * t_std)
        
        val_t = feats['torque']
        if val_t > t_ucl or val_t < t_lcl:
            self.spc_counters['torque'] += 1
        else:
            self.spc_counters['torque'] = 0
            
        if self.spc_counters['torque'] >= 3:
            alerts.append({
                "type": "SPC_VIOLATION",
                "feature": "Torque",
                "severity": "critical",
                "message": f"Torque Control Limit Exceeded ({val_t:.1f} > UCL {t_ucl:.1f}) x3",
                "value": val_t,
                "limit": t_ucl,
                "rca_context": plant_graph.get_upstream_dependencies(machine_id)
            })
            # Reset/Debounce? Let's keep alerting if it persists
            
        # Temp
        tmp_mean = self.spc_stats.get('temp_mean', 1580)
        tmp_std = self.spc_stats.get('temp_std', 10)
        if tmp_std < 0.01: tmp_std = 0.01
        
        tmp_ucl = tmp_mean + (3 * tmp_std)
        tmp_lcl = tmp_mean - (3 * tmp_std)
        
        val_tmp = feats['temperature_air']
        if val_tmp > tmp_ucl or val_tmp < tmp_lcl:
             self.spc_counters['temp'] += 1
        else:
             self.spc_counters['temp'] = 0
             
        if self.spc_counters['temp'] >= 3:
             alerts.append({
                "type": "SPC_VIOLATION",
                "feature": "Temperature",
                "severity": "critical",
                "message": f"Temp Control Limit Exceeded ({val_tmp:.1f} > UCL {tmp_ucl:.1f}) x3",
                "value": val_tmp,
                "limit": tmp_ucl,
                "rca_context": plant_graph.get_upstream_dependencies(machine_id)
            })

        # --- D. Persist Alerts ---
        if alerts:
            # print(f"⚠️ {machine_id}: {len(alerts)} Alerts")
            record = {
                "timestamp": timestamp,
                "machine_id": machine_id,
                "alerts": alerts,
                "raw_features": feats
            }
            try:
                self.coll.insert_one(record)
            except Exception as e:
                print(f"MongoDB Write Error (Alerts): {e}")

        # --- E. Audit Logging (All Predictions) ---
        # Measure latency (approximate end of processing)
        latency_ms = (datetime.utcnow() - datetime.fromisoformat(timestamp)).total_seconds() * 1000 if 'T' in timestamp else 0
        
        audit_record = {
            "timestamp": timestamp,
            "machine_id": machine_id,
            "model_version": "v2.1 (XGB+IF+SPC)",
            "prediction": int(iso_pred), # IF Prediction result
            "anomaly_score": float(factor) if 'factor' in locals() else 0.0,
            "latency_ms": latency_ms,
            "features": feats
        }
        try:
            self.audit_coll.insert_one(audit_record)
        except Exception as e:
            # Silent fail for audit to not crash stream
            pass

if __name__ == "__main__":
    detector = RealTimeDetector()
    detector.process_stream()
