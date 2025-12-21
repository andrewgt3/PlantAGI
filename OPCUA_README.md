# PlantAGI - OPC UA Real-Time Fleet Monitoring

## Quick Start

### 1. Start OPC UA Virtual Fleet Server
```bash
python3 opcua_fleet_server.py
```

This creates a virtual OPC UA server at `opc.tcp://localhost:4840` with 4 robots.

**Cascade Failure Demo:**
- The server has a writable variable `Cascade_Failure_Active`
- Toggle it to `True` to trigger the cascade scenario:
  - **ROBOT_1**: Torque spikes (conveyor jam)
  - **ROBOT_2 & ROBOT_3**: Torque drops to 0 (starved)
  - **ROBOT_4**: Vibration increases (compensating)

### 2. Start Data Streaming Client
```bash
python3 opcua_fleet_client.py
```

This connects to the OPC UA server and streams data to TimescaleDB in real-time (2Hz).

### 3. View Live Dashboard
```bash
streamlit run dashboard_streamlit.py
```

Open http://localhost:8501 to see live data updating every 2 seconds.

---

## System Architecture

```
OPC UA Server (Virtual Robots)
         ↓
    Port 4840
         ↓
  OPC UA Client
         ↓
  TimescaleDB (sensors table)
         ↓
 Streamlit Dashboard (Live View)
```

## Features

- ✅ **Real-Time Streaming**: 2Hz data acquisition
- ✅ **Batch Inserts**: Optimized database writes (20 samples/batch)
- ✅ **Cascade Simulation**: Toggle-able fleet failure scenario
- ✅ **Auto RUL Estimation**: Basic vibration-based RUL logic
- ✅ **Live Dashboard**: 2-second refresh with drill-down

## Triggering Cascade Failure

**Option 1: UaExpert (GUI)**
1. Download UA Expert: https://www.unified-automation.com/downloads/opc-ua-clients.html
2. Connect to `opc.tcp://localhost:4840`
3. Navigate to Objects → Cascade_Failure_Active
4. Write `True` to trigger cascade

**Option 2: Python Script**
```python
from asyncua import Client
import asyncio

async def trigger_cascade():
    client = Client("opc.tcp://localhost:4840/freeopcua/server/")
    await client.connect()
    
    # Get trigger node
    uri = "http://gaiapredictive.com/fleet"
    idx = await client.get_namespace_index(uri)
    trigger = await client.nodes.objects.get_child([f"{idx}:Cascade_Failure_Active"])
    
    # Activate cascade
    await trigger.write_value(True)
    print("🚨 CASCADE FAILURE ACTIVATED")
    
    await client.disconnect()

asyncio.run(trigger_cascade())
```

---

## Next Steps

1. **ML Integration**: Replace simple RUL estimation with XGBoost model
2. **Event Detection**: Auto-create events in database when anomalies detected
3. **Alerting**: Email/SMS when cascade failure detected
4. **Production OPC UA**: Connect to real PLCs/SCADA systems

---

**PlantAGI** - Real-Time Predictive Maintenance Platform
