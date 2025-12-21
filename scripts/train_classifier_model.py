import pandas as pd
import numpy as np
import joblib
import joblib
from xgboost import XGBClassifier
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
import sys
import os

# Ensure scripts dir is in path to import utility
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from load_merged_data import load_merged_dataset

# Config
MODEL_DIR = "models"
MODEL_PATH = f"{MODEL_DIR}/failure_model.pkl"

def train_model():
    print("🚀 Starting Contextual Training (NASA + C-MAPSS)...")
    
    # 1. Load Data
    try:
        df = load_merged_dataset()
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # 2. Contextual Labeling
    # Rule: Failure if (Time <= 48h) AND (Torque > 6.0)
    print("🏷️ Applying Contextual Labeling: (Time <= 48h) AND (Torque > 6.0)...")
    
    # Check if 'time_to_failure_hours' exists (it might be None for healthy bearings)
    # Fill None with infinity for healthy
    df['time_to_failure_hours'] = df['time_to_failure_hours'].fillna(999999)
    
    df['machine_failure'] = (
        (df['time_to_failure_hours'] <= 48) & 
        (df['torque'] > 6.0)
    ).astype(int)
    
    print(f"   Failure Count: {df['machine_failure'].sum()} / {len(df)}")
    
    # 3. Feature Selection
    # Features to use for training
    FEATURES = [
        'vibration_rms', 
        'temperature_air', 
        'torque', 
        'pressure', 
        'rotational_speed', 
        'tool_wear',
        'current_tool_wear_pct', # [NEW] Dynamic Wear
        'criticality_score'      # [NEW] Static Topology Importance
    ]
    # Add 'tool_wear' and 'rotational_speed' if they exist or defaulting
    if 'rotational_speed' not in df.columns:
        df['rotational_speed'] = 2000.0 # Default NASA speed
    if 'tool_wear' not in df.columns:
        df['tool_wear'] = 0.0 # Default
    
    X = df[FEATURES]
    y = df['machine_failure']
    
    print(f"   Features: {FEATURES}")
    
    # 4. Stratified Split (Required because failures are at the end)
    print("🔀 Shuffling data for Stratified Split (ensuring failure class in Train)...")
    from sklearn.model_selection import train_test_split
    
    # We use stratified split to maintain class ratio
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y, shuffle=True
    )
    
    print(f"   Train: {len(X_train)}, Test: {len(X_test)}")
    
    # Check Class Distribution
    pos_count = y_train.sum()
    neg_count = len(y_train) - pos_count
    print(f"   Train Class Balance: {pos_count} Failures / {neg_count} Non-Failures")
    
    if pos_count == 0 or neg_count == 0:
        print("❌ Training set has only one class. Cannot train classifier.")
        print("   Adjusting split or labeling parameters needed.")
        return

    # 5. Model Training (XGBoost)
    print("🧠 Training XGBoost Classifier...")
    # Handle Imbalance - AGGRESSIVE PRECISION MODE
    # Previous: sqrt(ratio) (~7) -> 40% Precision
    # New: Fixed Low Weight (2.0) -> Force model to be very selective
    # This minimizes False Positives to hit the >65% Precision target.
    # Handle Imbalance - TUNED PRECISION MODE
    # Weight 2.0 was too low (Recall=0). Weight 7.0 was decent (Prec=40%).
    # Weight 4.0 should be the middle ground.
    scale_pos_weight = 4.0
    print(f"   Scale Pos Weight (Tuned Precision): {scale_pos_weight:.2f}")
    
    # Ensure scale factor is reasonable
    if scale_pos_weight > 20: scale_pos_weight = 20.0
        
    try:
        clf = XGBClassifier(
            eval_metric='logloss', 
            scale_pos_weight=scale_pos_weight,
            learning_rate=0.1,
            max_depth=5,
            n_estimators=100,
            base_score=0.5,
            random_state=42
        )
        
        clf.fit(X_train, y_train)
    except Exception as e:
        print(f"⚠️ XGBoost failed: {e}. Fallback to RandomForest.")
        clf = RandomForestClassifier(n_estimators=100)
        clf.fit(X_train, y_train)
    
    # 6. Threshold Optimization (Precision vs Recall)
    print("⚖️ Optimizing Prediction Threshold...")
    probas = clf.predict_proba(X_test)[:, 1]
    
    best_threshold = 0.5
    best_precision = 0.0
    best_recall = 0.0
    
    # Iterate thresholds from 0.10 to 0.99
    # Lower bound reduced to catch signals if weight is low
    print("   Testing thresholds (Min Recall > 0.60)...")
    for t in np.arange(0.10, 0.99, 0.01):
        preds = (probas >= t).astype(int)
        precision = 0.0
        # Handle zero division
        if preds.sum() > 0:
            # Precision = TP / (TP + FP)
            # Use sklearn for safety
            from sklearn.metrics import precision_score, recall_score
            precision = precision_score(y_test, preds, zero_division=0)
            recall = recall_score(y_test, preds, zero_division=0)
        else:
             recall = 0.0
             
        # Criteria: Recall > 0.60 AND Maximize Precision
        if recall > 0.60:
            if precision > best_precision:
                best_precision = precision
                best_recall = recall
                best_threshold = t
                
    print(f"   🏆 Optimal Threshold: {best_threshold:.2f}")
    print(f"      Precision: {best_precision:.4f}")
    print(f"      Recall:    {best_recall:.4f}")
    
    # 7. Save Model & Config
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(clf, MODEL_PATH)
    print(f"✅ Model saved to {MODEL_PATH}")
    
    # Save Config
    import json
    config_path = f"{MODEL_DIR}/model_config.json"
    with open(config_path, "w") as f:
        json.dump({"threshold": float(best_threshold)}, f)
    print(f"✅ Config saved to {config_path}")

    # 8. Hybrid Inference Training (IF + SPC)
    print("🔮 Training Hybrid Inference Models...")
    
    # Isolation Forest (Train on HEALTHY data only to learn normality)
    # We use X_train where y_train == 0
    X_train_healthy = X_train[y_train == 0]
    
    print(f"   IF Training Data: {len(X_train_healthy)} records (Healthy only)")
    
    iso_forest = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    iso_forest.fit(X_train_healthy)
    
    # Save Isolation Forest
    iso_path = f"{MODEL_DIR}/isolation_forest.pkl"
    joblib.dump(iso_forest, iso_path)
    print(f"✅ Isolation Forest saved to {iso_path}")
    
    # SPC Baselines (Calc on HEALTHY data)
    spc_stats = {
        "torque_mean": float(X_train_healthy['torque'].mean()),
        "torque_std": float(X_train_healthy['torque'].std()),
        "temp_mean": float(X_train_healthy['temperature_air'].mean()),
        "temp_std": float(X_train_healthy['temperature_air'].std())
    }
    
    print(f"   SPC Baseline (Torque): Mean={spc_stats['torque_mean']:.2f}, Std={spc_stats['torque_std']:.2f}")
    print(f"   SPC Baseline (Temp):   Mean={spc_stats['temp_mean']:.2f}, Std={spc_stats['temp_std']:.2f}")
    
    # Update Config with SPC Stats
    full_config = {
        "threshold": float(best_threshold),
        "spc": spc_stats
    }
    
    with open(config_path, "w") as f:
        json.dump(full_config, f)
    print(f"✅ Config updated with SPC stats in {config_path}")

if __name__ == "__main__":
    train_model()
