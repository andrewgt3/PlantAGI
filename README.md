# PlantAGI

AI-powered predictive maintenance platform with contextual anomaly detection and RUL prediction.

## Quick Start

```bash
# 1. Start database
docker-compose up -d timescaledb

# 2. Generate data
python3 generate_synthetic_sensor_data.py
python3 etl_pipeline.py

# 3. Train model
python3 generate_training_data.py
python3 train_rul_model.py

# 4. Run analytics
python3 analytics_engine.py

# 5. Launch dashboard
streamlit run dashboard_streamlit.py
```

## Core Components

- `analyt ics_engine.py` - Anomaly detection + RUL predictions
- `train_rul_model.py` - XGBoost model training
- `dashboard_streamlit.py` - Real-time monitoring
- `etl_pipeline.py` - TimescaleDB ingestion
- `frontend/` - React dashboard

## Features

✅ Contextual anomaly correlation  
✅ XGBoost RUL prediction (±111hr MAE)  
✅ Severity warnings (72hr/24hr thresholds)  
✅ PDF work order generation  
✅ Real-time Streamlit dashboard

---
**PlantAGI** - Predictive Maintenance AI Platform
