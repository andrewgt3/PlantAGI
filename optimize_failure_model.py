
import os
import pandas as pd
import numpy as np
import joblib
from xgboost import XGBClassifier
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV, train_test_split, TimeSeriesSplit
from sklearn.metrics import f1_score, classification_report, make_scorer, recall_score
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# Configuration
MODEL_PATH = "models/failure_model.pkl"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = os.getenv("DT_POSTGRES_DB", "pdm_timeseries")
DB_USER = os.getenv("DT_POSTGRES_USER", "postgres")
DB_PASS = os.getenv("DT_POSTGRES_PASSWORD", "password")

def optimize_model():
    print("🚀 Starting Model Optimization (F1 Maximization)...")
    
    # 1. Load Data from DB
    # 1. Load Data
    try:
        db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        
        print("📥 Loading Context Data from 'data/feature_store.csv' (Proprietary Features)...")
        df = pd.read_csv("data/feature_store.csv")
        # Ensure timestamp is datetime for merge
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Encode Criticality (A=3, B=2, C=1)
        crit_map = {'A': 3, 'B': 2, 'C': 1}
        df['criticality_score'] = df['criticality_rating'].map(crit_map).fillna(1)
        
        print("📥 Fetching High-Frequency CWRU features from DB...")
        df_cwru = pd.read_sql("SELECT * FROM cwru_features", engine)
        
        # Ensure timestamp match (DB might be timezone aware)
        if df_cwru['timestamp'].dt.tz is None:
             df_cwru['timestamp'] = df_cwru['timestamp'].dt.tz_localize('UTC')
        
        # Normalize local if needed (CSV read usually ISO8601 with Z or offset)
        # pandas read_csv with ISO usually keeps it reasonable. 
        # Let's try merge.
        
        print(f"   Loaded {len(df_cwru)} CWRU records.")
        
        print(f"   Loaded {len(df_cwru)} CWRU records.")
        
        # Robust Merge Logic
        df = df.sort_values('timestamp')
        df_cwru = df_cwru.sort_values('timestamp')
        
        # Ensure machine_id types match
        df['machine_id'] = df['machine_id'].astype(str)
        df_cwru['machine_id'] = df_cwru['machine_id'].astype(str)
        
        # Use merge_asof with tolerance
        try:
            df = pd.merge_asof(
                df, 
                df_cwru, 
                on='timestamp', 
                by='machine_id', 
                direction='nearest',
                tolerance=pd.Timedelta('10min') # Allow slight drift
            )
        except Exception as merge_err:
            print(f"⚠️ merge_asof failed ({merge_err}), falling back to inner join...")
            df = pd.merge(df, df_cwru, on=['machine_id', 'timestamp'], how='inner')

        print(f"🔗 Merged Data Shape: {df.shape}")
        
    except Exception as e:
        print(f"❌ Failed to load validation data: {e}")
        return

    # 2. Advanced Feature Selection (Adding CWRU + Proprietary)
    # New features: criticality_score, current_tool_wear_pct
    # Check if CWRU cols exist (merged)
    # Default to just Context if merge yielded limited cols (though shape check handles empty)
    
    feature_cols = [
        c for c in df.columns 
        if '_8h' in c or 'slope_24h' in c or 'CSLM' in c or 
        c in ['rms', 'peak', 'kurtosis', 'dominant_freq', 'spectral_power', 'criticality_score', 'current_tool_wear_pct']
    ]
    
    # If CWRU features missing due to merge fail, we train on Context + Prop (Better than nothing)
    missing_cols = [c for c in ['rms', 'peak'] if c not in df.columns]
    if missing_cols:
        print(f"⚠️ Missing CWRU features: {missing_cols}. Training partial model.")
        
    print(f"🎯 Optimization Focus: {len(feature_cols)} Features")
    
    X = df[feature_cols]
    y = df['machine_failure']
    
    # Clean NaNs (from merge misses)
    df_clean = df.dropna(subset=feature_cols + ['machine_failure'])
    if len(df_clean) < 10:
        print("❌ Too few records after merge/cleaning. Aborting.")
        return
        
    X = df_clean[feature_cols]
    y = df_clean['machine_failure']

    
    # Check Balance
    num_pos = y.sum()
    num_neg = len(y) - num_pos
    balance_ratio = num_neg / num_pos if num_pos > 0 else 1.0
    print(f"⚖️ Raw Balance Ratio: {balance_ratio:.2f}")

    # 3. Grid Search Configuration
    # User requested: scale_pos_weight + hypertuning
    # [DRIFT FIX] Increase weight range to force Recall > 60% even in later folds
    # [DRIFT FIX] Use TimeSeriesSplit to validate against temporal drift (Walk-Forward)
    
    tscv = TimeSeriesSplit(n_splits=5)
    
    param_grid = {
        'scale_pos_weight': [balance_ratio * 2.0, balance_ratio * 3.0], # Aggressive Recall Boost for Fold 5 Safety
        'max_depth': [6, 8],
        'min_child_weight': [1, 5], # [NEW] Handle noisy data/drift
        'gamma': [0.0, 0.5], # [NEW] Regularization to prevent overfitting
        'learning_rate': [0.01, 0.05],
        'n_estimators': [200, 300],
        'subsample': [0.8], 
        'colsample_bytree': [0.8]
    }
    
    print(f"🔍 Starting GridSearchCV (Target: F1 | CV: TimeSeriesSplit-5)...")
    
    xgb = XGBClassifier(eval_metric='logloss', use_label_encoder=False, random_state=42)
    
    grid = GridSearchCV(
        estimator=xgb,
        param_grid=param_grid,
        scoring='f1', # F1 balances Recall/Precision. High weight -> High Recall.
        cv=tscv, # [DRIFT FIX] Walk-Forward Validation
        verbose=1,
        n_jobs=-1
    )
    
    grid.fit(X, y)
    
    print("\n✅ Optimization Complete!")
    print(f"🏆 Best F1-Score: {grid.best_score_:.4f}")
    print(f"⚙️ Best Parameters: {grid.best_params_}")
    
    # 4. Save Optimized Model
    best_model = grid.best_estimator_
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(best_model, MODEL_PATH)
    print(f"💾 Optimized model saved to {MODEL_PATH}")
    
    # 5. Output for Audit Script Update
    # I will print the params so I can update the audit script manually or programmatically.
    return grid.best_params_

if __name__ == "__main__":
    optimize_model()
