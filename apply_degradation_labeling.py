"""
Degradation-Based Labeling Strategy
Calculate degradation scores from advanced features and relabel target variable
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from scipy.stats import zscore

# Database configuration
DB_USER = "postgres"
DB_PASS = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "pdm_timeseries"

def get_db_engine():
    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)


def calculate_degradation_score(df_features):
    """
    Calculate continuous degradation score D_t ∈ [0, 1] based on advanced features.
    
    Strategy:
    1. Identify key degradation indicators (BPFO, BPFI, spectral_kurtosis)
    2. Normalize features to [0, 1] range per machine
    3. Calculate weighted average
    4. Apply temporal smoothing
    
    Args:
        df_features: DataFrame with advanced features
    
    Returns:
        pd.Series: Degradation scores D_t
    """
    print("📊 Calculating degradation scores from advanced features...")
    
    # Key degradation indicators
    degradation_features = [
        'bpfo_amplitude',      # Ball Pass Outer Race (primary failure indicator)
        'bpfi_amplitude',      # Ball Pass Inner Race
        'spectral_kurtosis',   # Peakedness (indicates impacts)
        'freq_entropy',        # Frequency disorder
        'mid_band_power'       # Mid-frequency energy
    ]
    
    # Check which features exist
    available_features = [f for f in degradation_features if f in df_features.columns]
    print(f"   Using {len(available_features)} degradation indicators: {available_features}")
    
    degradation_scores = []
    
    for machine_id in df_features['machine_id'].unique():
        machine_mask = df_features['machine_id'] == machine_id
        machine_df = df_features[machine_mask].copy()
        
        # Sort by timestamp
        machine_df = machine_df.sort_values('timestamp').reset_index(drop=True)
        
        # Extract degradation features
        feature_matrix = machine_df[available_features].values
        
        # Normalize each feature to [0, 1] using min-max scaling
        # This captures the growth from baseline to maximum
        normalized = np.zeros_like(feature_matrix)
        for i, col in enumerate(available_features):
            col_data = feature_matrix[:, i]
            min_val = np.min(col_data)
            max_val = np.max(col_data)
            
            if max_val > min_val:
                normalized[:, i] = (col_data - min_val) / (max_val - min_val)
            else:
                normalized[:, i] = 0.0
        
        # Weighted average (BPFO and BPFI are most important)
        weights = {
            'bpfo_amplitude': 0.35,
            'bpfi_amplitude': 0.35,
            'spectral_kurtosis': 0.15,
            'freq_entropy': 0.10,
            'mid_band_power': 0.05
        }
        
        weight_vector = np.array([weights.get(f, 0.2) for f in available_features])
        weight_vector = weight_vector / weight_vector.sum()  # Normalize weights
        
        # Calculate degradation score
        D_t = np.dot(normalized, weight_vector)
        
        # Apply exponential moving average for smoothing
        # This reduces noise and captures trend
        alpha = 0.3  # Smoothing factor
        D_t_smoothed = np.zeros_like(D_t)
        D_t_smoothed[0] = D_t[0]
        
        for t in range(1, len(D_t)):
            D_t_smoothed[t] = alpha * D_t[t] + (1 - alpha) * D_t_smoothed[t-1]
        
        # Store with original indices
        for idx, score in zip(machine_df.index, D_t_smoothed):
            degradation_scores.append({
                'index': idx,
                'degradation_score': score
            })
    
    # Create Series with original index
    score_df = pd.DataFrame(degradation_scores).set_index('index')
    degradation_series = score_df['degradation_score']
    
    print(f"   ✅ Calculated degradation scores")
    print(f"   Score range: [{degradation_series.min():.3f}, {degradation_series.max():.3f}]")
    print(f"   Mean: {degradation_series.mean():.3f}, Std: {degradation_series.std():.3f}")
    
    return degradation_series


def apply_degradation_labeling(threshold=0.85):
    """
    Apply degradation-based labeling strategy.
    
    Args:
        threshold: Degradation score threshold for failure label (default: 0.85)
    
    Returns:
        DataFrame with updated labels
    """
    print("🔄 Applying Degradation-Based Labeling Strategy")
    print("=" * 60)
    
    engine = get_db_engine()
    
    # Load features
    print("📥 Loading features from database...")
    df_features = pd.read_sql("SELECT * FROM cwru_features ORDER BY machine_id, timestamp", engine)
    print(f"   Loaded {len(df_features)} feature records")
    
    # Calculate degradation scores
    degradation_scores = calculate_degradation_score(df_features)
    
    # Add degradation scores to dataframe (align by index)
    df_features = df_features.reset_index(drop=True)
    df_features['degradation_score'] = degradation_scores.values
    
    # Apply threshold-based labeling
    print(f"\n🎯 Applying threshold: D_t > {threshold}")
    df_features['machine_failure_new'] = (df_features['degradation_score'] > threshold).astype(int)
    
    # Statistics
    old_failures = df_features['machine_failure'].sum() if 'machine_failure' in df_features.columns else 0
    new_failures = df_features['machine_failure_new'].sum()
    
    print(f"\n📊 Labeling Results:")
    print(f"   Old strategy (48-hour window): {old_failures} failures ({old_failures/len(df_features):.1%})")
    print(f"   New strategy (degradation-based): {new_failures} failures ({new_failures/len(df_features):.1%})")
    print(f"   Improvement: +{new_failures - old_failures} failure labels")
    
    # Distribution by machine
    print(f"\n🔍 Failure Distribution by Machine:")
    for machine_id in df_features['machine_id'].unique():
        machine_mask = df_features['machine_id'] == machine_id
        machine_failures = df_features[machine_mask]['machine_failure_new'].sum()
        machine_total = machine_mask.sum()
        print(f"   {machine_id}: {machine_failures}/{machine_total} ({machine_failures/machine_total:.1%})")
    
    # Update sensor_features table
    print(f"\n💾 Updating sensor_features table...")
    
    # Create update dataframe
    update_df = df_features[['machine_id', 'timestamp', 'machine_failure_new', 'degradation_score']].copy()
    update_df.rename(columns={'machine_failure_new': 'machine_failure'}, inplace=True)
    
    # Load existing sensor_features
    df_sensor = pd.read_sql("SELECT * FROM sensor_features", engine)
    
    # Merge to update machine_failure column
    df_sensor = df_sensor.drop(columns=['machine_failure'], errors='ignore')
    df_sensor = df_sensor.merge(
        update_df[['machine_id', 'timestamp', 'machine_failure']],
        on=['machine_id', 'timestamp'],
        how='left'
    )
    
    # Fill any NaN with 0
    df_sensor['machine_failure'] = df_sensor['machine_failure'].fillna(0).astype(int)
    
    # Save back to database
    df_sensor.to_sql('sensor_features', engine, if_exists='replace', index=False)
    
    print(f"✅ Updated {len(df_sensor)} records in sensor_features")
    
    # Also save degradation scores to cwru_features
    df_features[['machine_id', 'timestamp', 'degradation_score']].to_sql(
        'degradation_scores',
        engine,
        if_exists='replace',
        index=False
    )
    
    print(f"✅ Saved degradation scores to degradation_scores table")
    
    return df_features


def main():
    """
    Main execution: Apply degradation-based labeling
    """
    # Try different thresholds to find optimal balance
    # Based on observed range [0.049, 0.500], use lower thresholds
    thresholds = [0.30, 0.35, 0.40]
    
    print("🔬 Testing multiple thresholds...\n")
    
    for threshold in thresholds:
        print(f"\n{'='*60}")
        print(f"Testing threshold: {threshold}")
        print(f"{'='*60}")
        
        df = apply_degradation_labeling(threshold=threshold)
        
        failure_rate = df['machine_failure_new'].mean()
        
        # Target: 10-20% failure rate for good balance
        if 0.10 <= failure_rate <= 0.20:
            print(f"\n✅ Optimal threshold found: {threshold}")
            print(f"   Failure rate: {failure_rate:.1%} (good balance)")
            break
    else:
        # Use 0.35 as default (middle of observed range)
        print(f"\n⚠️  Using default threshold: 0.35")
        df = apply_degradation_labeling(threshold=0.35)
    
    print(f"\n{'='*60}")
    print("✅ Degradation-Based Labeling Complete!")
    print(f"{'='*60}")
    print("\nNext step: Run generate_audit_report.py to evaluate model performance")


if __name__ == "__main__":
    main()
