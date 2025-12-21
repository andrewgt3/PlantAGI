"""
Test MongoDB Connection and Audit Logging
"""

from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB Configuration
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB = "pdm_audit"

def test_mongodb_connection():
    """Test MongoDB connection and create sample audit log"""
    print("🔍 Testing MongoDB Connection...")
    print("=" * 60)
    
    try:
        # Connect to MongoDB
        mongo_client = MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/")
        
        # Test connection
        mongo_client.admin.command('ping')
        print("✅ MongoDB connection successful!")
        
        # Access database and collection
        mongo_db = mongo_client[MONGO_DB]
        audit_collection = mongo_db["model_audit_log"]
        
        print(f"\n📊 Database: {MONGO_DB}")
        print(f"📋 Collection: model_audit_log")
        
        # Insert test audit record
        test_record = {
            "timestamp": datetime.utcnow(),
            "machine_id": "TEST_MACHINE_001",
            "predicted_probability": 0.75,
            "rul_prediction": 48.5,
            "prediction_latency_ms": 12.34,
            "model_version": "v2.0_augmented",
            "status": "At Risk",
            "sensor_data": {
                "Rotational Speed": 1500.0,
                "Temperature": 305.2,
                "Torque": 45.3,
                "Tool Wear": 120.0
            }
        }
        
        result = audit_collection.insert_one(test_record)
        print(f"\n✅ Test audit record inserted!")
        print(f"   Record ID: {result.inserted_id}")
        
        # Query the record back
        count = audit_collection.count_documents({})
        print(f"\n📈 Total audit records: {count}")
        
        # Show recent records
        recent_records = list(audit_collection.find().sort("timestamp", -1).limit(5))
        print(f"\n📋 Recent Audit Records:")
        for i, record in enumerate(recent_records, 1):
            print(f"   {i}. Machine: {record['machine_id']}, "
                  f"Prob: {record.get('predicted_probability', 'N/A')}, "
                  f"Latency: {record.get('prediction_latency_ms', 'N/A')}ms, "
                  f"Version: {record.get('model_version', 'N/A')}")
        
        print(f"\n{'='*60}")
        print("✅ MongoDB Audit System Ready!")
        print(f"{'='*60}")
        
        return True
        
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("\n💡 Make sure MongoDB is running:")
        print("   docker-compose up -d mongodb")
        return False

if __name__ == "__main__":
    test_mongodb_connection()
