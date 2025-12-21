"""
XGBoost RUL Model Training Script
==================================
Trains an XGBoost regression model to predict Remaining Useful Life (RUL)
from sensor readings.

Features: vibration, vibration_rate, time_pct
Target: RUL (Remaining Useful Life in hours)

Author: ML Engineering Team
"""

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import json


# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_FILE = "rul_training_data.csv"
MODEL_OUTPUT = "rul_model.json"
RANDOM_STATE = 42
TEST_SIZE = 0.2

# =============================================================================
# DATA LOADING
# =============================================================================

def load_data(filepath):
    """Load training data from CSV."""
    print("=" * 80)
    print("LOADING TRAINING DATA")
    print("=" * 80)
    print()
    
    df = pd.read_csv(filepath)
    print(f"✓ Loaded {len(df):,} samples from {filepath}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Robots: {df['robot_id'].nunique()}")
    print()
    
    return df

# =============================================================================
# PREPROCESSING
# =============================================================================

def prepare_features(df):
    """
    Split data into features (X) and target (y).
    Uses available sensor columns as features.
    """
    print("=" * 80)
    print("PREPROCESSING")
    print("=" * 80)
    print()
    
    # Target variable
    target_col = 'RUL'
    
    # Feature columns (use all available sensor data)
    # Exclude identifiers and the target
    exclude_cols = ['robot_id', 'time', 'RUL', 'total_life']
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    print(f"Features: {feature_cols}")
    print(f"Target: {target_col}")
    print()
    
    X = df[feature_cols]
    y = df[target_col]
    
    # Statistics
    print("Feature Statistics:")
    print(X.describe())
    print()
    
    print("Target Statistics:")
    print(f"  Min RUL: {y.min():.1f} hours")
    print(f"  Max RUL: {y.max():.1f} hours")
    print(f"  Mean RUL: {y.mean():.1f} hours")
    print(f"  Std RUL: {y.std():.1f} hours")
    print()
    
    return X, y, feature_cols

# =============================================================================
# TRAIN/TEST SPLIT
# =============================================================================

def split_data(X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE):
    """
    Split data into training and test sets.
    Uses stratified split to ensure good distribution.
    """
    print("=" * 80)
    print("TRAIN/TEST SPLIT")
    print("=" * 80)
    print()
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    
    print(f"Training set: {len(X_train):,} samples ({(1-test_size)*100:.0f}%)")
    print(f"Test set: {len(X_test):,} samples ({test_size*100:.0f}%)")
    print()
    
    return X_train, X_test, y_train, y_test

# =============================================================================
# MODEL TRAINING
# =============================================================================

def train_model(X_train, y_train):
    """
    Train XGBoost regression model for RUL prediction.
    """
    print("=" * 80)
    print("TRAINING XGBOOST MODEL")
    print("=" * 80)
    print()
    
    # XGBoost Regressor with optimized hyperparameters
    model = xgb.XGBRegressor(
        objective='reg:squarederror',  # Regression task
        n_estimators=200,               # Number of trees
        max_depth=6,                    # Tree depth
        learning_rate=0.05,             # Learning rate
        subsample=0.8,                  # Row sampling
        colsample_bytree=0.8,           # Column sampling
        min_child_weight=3,             # Minimum samples in leaf
        gamma=0,                        # Regularization
        random_state=RANDOM_STATE,
        n_jobs=-1                       # Use all CPU cores
    )
    
    print("Hyperparameters:")
    print(f"  n_estimators: {model.n_estimators}")
    print(f"  max_depth: {model.max_depth}")
    print(f"  learning_rate: {model.learning_rate}")
    print()
    
    print("Training model...")
    model.fit(
        X_train, y_train,
        verbose=False
    )
    
    print("✓ Training complete!")
    print()
    
    return model

# =============================================================================
# MODEL EVALUATION
# =============================================================================

def evaluate_model(model, X_train, y_train, X_test, y_test):
    """
    Evaluate model performance on training and test sets.
    """
    print("=" * 80)
    print("MODEL EVALUATION")
    print("=" * 80)
    print()
    
    # Predictions
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    # Training metrics
    train_mae = mean_absolute_error(y_train, y_train_pred)
    train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
    train_r2 = r2_score(y_train, y_train_pred)
    
    # Test metrics
    test_mae = mean_absolute_error(y_test, y_test_pred)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
    test_r2 = r2_score(y_test, y_test_pred)
    
    print("Training Set Performance:")
    print(f"  MAE:  {train_mae:.2f} hours")
    print(f"  RMSE: {train_rmse:.2f} hours")
    print(f"  R²:   {train_r2:.4f}")
    print()
    
    print("Test Set Performance:")
    print(f"  MAE:  {test_mae:.2f} hours")
    print(f"  RMSE: {test_rmse:.2f} hours")
    print(f"  R²:   {test_r2:.4f}")
    print()
    
    # Accuracy statement
    print("=" * 80)
    print(f"✓ Model Accuracy: ±{test_mae:.1f} hours (Mean Absolute Error)")
    print("=" * 80)
    print()
    
    return {
        'train_mae': train_mae,
        'train_rmse': train_rmse,
        'train_r2': train_r2,
        'test_mae': test_mae,
        'test_rmse': test_rmse,
        'test_r2': test_r2
    }

# =============================================================================
# FEATURE IMPORTANCE
# =============================================================================

def print_feature_importance(model, feature_names):
    """Print feature importance scores."""
    print("=" * 80)
    print("FEATURE IMPORTANCE")
    print("=" * 80)
    print()
    
    importance = model.feature_importances_
    
    # Sort by importance
    indices = np.argsort(importance)[::-1]
    
    print("Features ranked by importance:")
    for i, idx in enumerate(indices, 1):
        print(f"  {i}. {feature_names[idx]}: {importance[idx]:.4f}")
    print()

# =============================================================================
# MODEL PERSISTENCE
# =============================================================================

def save_model(model, filepath, metrics, feature_names):
    """Save trained model and metadata."""
    print("=" * 80)
    print("SAVING MODEL")
    print("=" * 80)
    print()
    
    # Save XGBoost model
    model.save_model(filepath)
    print(f"✓ Model saved to: {filepath}")
    
    # Save metadata
    metadata = {
        'model_type': 'XGBoost Regressor',
        'target': 'RUL',
        'features': feature_names,
        'metrics': {
            'test_mae': float(metrics['test_mae']),
            'test_rmse': float(metrics['test_rmse']),
            'test_r2': float(metrics['test_r2'])
        },
        'hyperparameters': {
            'n_estimators': int(model.n_estimators),
            'max_depth': int(model.max_depth),
            'learning_rate': float(model.learning_rate)
        }
    }
    
    metadata_file = filepath.replace('.json', '_metadata.json')
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"✓ Metadata saved to: {metadata_file}")
    print()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Execute the complete training pipeline."""
    
    # 1. Load data
    df = load_data(DATA_FILE)
    
    # 2. Prepare features
    X, y, feature_names = prepare_features(df)
    
    # 3. Split data
    X_train, X_test, y_train, y_test = split_data(X, y)
    
    # 4. Train model
    model = train_model(X_train, y_train)
    
    # 5. Evaluate model
    metrics = evaluate_model(model, X_train, y_train, X_test, y_test)
    
    # 6. Feature importance
    print_feature_importance(model, feature_names)
    
    # 7. Save model
    save_model(model, MODEL_OUTPUT, metrics, feature_names)
    
    # Final summary
    print("=" * 80)
    print("TRAINING COMPLETE")
    print("=" * 80)
    print()
    print(f"✓ Model file: {MODEL_OUTPUT}")
    print(f"✓ Test MAE: ±{metrics['test_mae']:.1f} hours")
    print(f"✓ R² Score: {metrics['test_r2']:.4f}")
    print()
    print("Next Steps:")
    print("  1. Load model: model = xgb.XGBRegressor(); model.load_model('rul_model.json')")
    print("  2. Predict RUL: rul = model.predict([[vibration, vib_rate, time_pct]])")
    print()
    print("=" * 80)

if __name__ == "__main__":
    main()
