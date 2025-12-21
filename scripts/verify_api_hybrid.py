
import requests
import json
import sys

URL = "http://localhost:8000/api/v1/predict/machine/1st_test_bearing_2"

try:
    print(f"📡 Calling API: {URL}")
    response = requests.get(URL, timeout=5)
    
    if response.status_code == 200:
        data = response.json()
        print("✅ API Responded")
        
        # Check Hybrid Fields
        if 'alerts' in data:
            print(f"✅ 'alerts' field present: {len(data['alerts'])} items")
            print(f"   Sample: {data['alerts']}")
        else:
            print("❌ 'alerts' field MISSING")
            
        if 'spc_limits' in data:
            print("✅ 'spc_limits' field present")
            print(f"   Values: {data['spc_limits']}")
        else:
            print("❌ 'spc_limits' field MISSING")
            
        if 'alerts' in data and 'spc_limits' in data:
            sys.exit(0)
        else:
            sys.exit(1)
            
    else:
        print(f"❌ API Error: {response.status_code}")
        print(f"   Detail: {response.text}")
        sys.exit(1)

except Exception as e:
    print(f"❌ Connection Failed: {e}")
    sys.exit(1)
