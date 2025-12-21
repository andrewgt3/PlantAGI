"""
Data Augmentation for Failure Samples
Create synthetic failure examples using Gaussian jitter
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Database configuration
DB_USER = "postgres"
DB_PASS = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "pdm_timeseries"

def get_db_engine():
    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)


def augment_failure_samples(n_copies=5, noise_level=0.05, degradation_threshold=0.25):
    """
    Create synthetic failure samples using Gaussian jitter.
    Targets samples with Degradation Score > threshold for augmentation.
    
    Args:
        n_copies: Number of synthetic copies per failure sample (3-5)
        noise_level: Standard deviation of Gaussian noise (default: 5% of feature value)
        degradation_threshold: Minimum degradation score to augment (default: 0.25)
    
    Returns:
        DataFrame with original + augmented samples
    """
    print("🔬 Data Augmentation for High-Degradation Samples")
    print("=" * 60)
    
    engine = get_db_engine()
    
    # Load features and labels
    print("📥 Loading data from database...")
    df_features = pd.read_sql("SELECT * FROM cwru_features ORDER BY machine_id, timestamp", engine)
    df_sensor = pd.read_sql("SELECT machine_id, timestamp, machine_failure FROM sensor_features", engine)
    
    # Load degradation scores
    try:
        df_degradation = pd.read_sql("SELECT machine_id, timestamp, degradation_score FROM degradation_scores", engine)
        df = df_features.merge(df_sensor, on=['machine_id', 'timestamp'], how='inner')
        df = df.merge(df_degradation, on=['machine_id', 'timestamp'], how='left')
        df['degradation_score'] = df['degradation_score'].fillna(0.0)
        print(f"   ✅ Loaded degradation scores")
    except:
        print(f"   ⚠️  No degradation_scores table found, using machine_failure labels only")
        df = df_features.merge(df_sensor, on=['machine_id', 'timestamp'], how='inner')
        df['degradation_score'] = df['machine_failure'].astype(float)
    
    print(f"   Total records: {len(df)}")
    print(f"   Failure samples: {df['machine_failure'].sum()} ({df['machine_failure'].mean():.1%})")
    print(f"   High degradation (>{degradation_threshold}): {(df['degradation_score'] > degradation_threshold).sum()}")
    
    # Identify feature columns (exclude metadata)
    feature_cols = [c for c in df.columns if c not in ['machine_id', 'timestamp', 'machine_failure', 'degradation_score']]
    print(f"   Feature columns: {len(feature_cols)}")
    
    # Target high-degradation samples for augmentation
    df_failures = df[df['degradation_score'] > degradation_threshold].copy()
    df_healthy = df[df['degradation_score'] <= degradation_threshold].copy()
    
    print(f"\n🎯 Augmenting {len(df_failures)} failure samples...")
    print(f"   Creating {n_copies} synthetic copies per sample")
    print(f"   Noise level: {noise_level*100:.1f}% of feature value")
    
    # Create augmented samples
    augmented_samples = []
    
    for idx, row in df_failures.iterrows():
        # Extract feature values
        features = row[feature_cols].values
        
        # Create n_copies synthetic samples
        for copy_num in range(n_copies):
            # Add Gaussian noise to each feature
            # Noise is proportional to feature value (relative noise)
            noise = np.random.normal(0, noise_level, size=len(features))
            augmented_features = features * (1 + noise)
            
            # Ensure features stay positive (for physical quantities)
            augmented_features = np.maximum(augmented_features, 0)
            
            # Create new record
            new_record = {
                'machine_id': f"{row['machine_id']}_aug_{idx}_{copy_num}",
                'timestamp': row['timestamp'],
                'machine_failure': 1,  # Keep failure label
                **dict(zip(feature_cols, augmented_features))
            }
            
            augmented_samples.append(new_record)
        
        if (idx + 1) % 100 == 0:
            print(f"   Processed {idx + 1}/{len(df_failures)} failure samples...", end='\r')
    
    print(f"\n   ✅ Created {len(augmented_samples)} synthetic failure samples")
    
    # Combine original and augmented data
    df_augmented = pd.DataFrame(augmented_samples)
    df_combined = pd.concat([df, df_augmented], ignore_index=True)
    
    print(f"\n📊 Final Dataset:")
    print(f"   Original samples: {len(df)}")
    print(f"   Augmented samples: {len(df_augmented)}")
    print(f"   Total samples: {len(df_combined)}")
    print(f"   Failure samples: {df_combined['machine_failure'].sum()} ({df_combined['machine_failure'].mean():.1%})")
    
    # Save augmented dataset
    print(f"\n💾 Saving augmented dataset...")
    
    # Save to new tables for training
    df_combined[['machine_id', 'timestamp', 'machine_failure']].to_sql(
        'sensor_features_augmented',
        engine,
        if_exists='replace',
        index=False
    )
    
    df_combined.to_sql(
        'cwru_features_augmented',
        engine,
        if_exists='replace',
        index=False
    )
    
    print(f"✅ Saved to sensor_features_augmented and cwru_features_augmented tables")
    
    return df_combined


def main():
    """
    Main execution: Augment high-degradation samples (score > 0.25)
    """
    # Create 5 synthetic copies per high-degradation sample
    # Use 5% noise level (maintains pattern while adding variation)
    df_augmented = augment_failure_samples(
        n_copies=5, 
        noise_level=0.05,
        degradation_threshold=0.25
    )
    
    print(f"\n{'='*60}")
    print("✅ Data Augmentation Complete!")
    print(f"{'='*60}")
    print("\nNext steps:")
    print("1. Retrain XGBoost model with augmented data")
    print("2. Run generate_audit_report.py to evaluate performance")



if __name__ == "__main__":
    main()
