
import redis
import json
import os

r = redis.Redis(host="localhost", port=6379, db=0)
pubsub = r.pubsub()
pubsub.subscribe("sensor_stream")

print("Peeking Redis...")
count = 0
for msg in pubsub.listen():
    if msg['type'] == 'message':
        data = json.loads(msg['data'])
        print(f"Sample: {json.dumps(data, indent=2)}")
        break
