"""
Extract advanced features from NASA vibration signals
"""

import pandas as pd
import numpy as np
from advanced_features import extract_fft_features, extract_envelope_features
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


def extract_features_from_nasa_data():
    """
    Extract FFT and envelope features from real NASA vibration signals.
    """
    print("🔬 Extracting features from NASA vibration data...")
    
    # Load vibration signals
    print("📥 Loading vibration signals...")
    df = pd.read_pickle('data/nasa_vibration_signals.pkl')
    print(f"   Loaded {len(df)} records")
    
    # Extract features for each record
    features_list = []
    
    for idx, row in df.iterrows():
        if idx % 100 == 0:
            print(f"   Processing {idx}/{len(df)}...", end='\r')
        
        # Get vibration signal (20,480 samples at 20 kHz)
        signal = row['vibration_signal']
        
        # Downsample to 12 kHz for consistency with existing pipeline
        # Take every ~1.67th sample: 20000/12000 = 1.67
        downsample_factor = int(20000 / 12000)
        signal_12k = signal[::downsample_factor][:1024]  # Take first 1024 samples
        
        # Extract FFT features
        fft_features = extract_fft_features(signal_12k, fs=12000)
        
        # Extract envelope features
        envelope_features = extract_envelope_features(signal_12k, fs=12000)
        
        # Combine features
        features = {
            'machine_id': row['machine_id'],
            'timestamp': row['timestamp'],
            **fft_features,
            **envelope_features
        }
        
        features_list.append(features)
    
    print(f"\n   ✅ Extracted features for {len(features_list)} records")
    
    # Create DataFrame
    df_features = pd.DataFrame(features_list)
    
    # Save to database
    print("\n💾 Saving to cwru_features table...")
    engine = get_db_engine()
    df_features.to_sql('cwru_features', engine, if_exists='replace', index=False)
    
    print(f"✅ Saved {len(df_features)} feature records")
    
    return df_features


if __name__ == "__main__":
    extract_features_from_nasa_data()
