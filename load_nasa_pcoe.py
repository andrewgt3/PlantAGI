"""
NASA IMS Bearing Data Loader
Loads real vibration data from NASA PCOE dataset
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime
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


def parse_filename_to_timestamp(filename):
    """
    Convert filename to timestamp.
    Format: YYYY.MM.DD.HH.MM.SS
    Example: 2003.10.22.12.06.24
    """
    parts = filename.split('.')
    year, month, day, hour, minute, second = map(int, parts)
    return datetime(year, month, day, hour, minute, second)


def load_vibration_file(filepath):
    """
    Load a single vibration data file.
    
    Returns:
        np.array: Shape (20480, 8) - 8 channels of vibration data
    """
    try:
        data = np.loadtxt(filepath, delimiter='\t')
        return data
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None


def identify_failure_bearing(test_name):
    """
    Identify which bearing failed in each test.
    
    Returns:
        tuple: (bearing_number, channel_indices)
    """
    failure_map = {
        '1st_test': (3, [4, 5]),  # Bearing 3, channels 5-6 (0-indexed: 4-5)
        '2nd_test': (1, [0, 1]),  # Bearing 1, channels 1-2 (0-indexed: 0-1)
        '3rd_test': (3, [4, 5])   # Bearing 3, channels 5-6 (0-indexed: 4-5)
    }
    return failure_map.get(test_name, (3, [4, 5]))


def load_test_run(test_dir, test_name, max_files=None):
    """
    Load all files from a test run.
    
    Args:
        test_dir: Path to test directory (e.g., 'data/NASA_PCOE_DATA/1st_test')
        test_name: Name of test ('1st_test', '2nd_test', '3rd_test')
        max_files: Maximum number of files to load (None = all)
    
    Returns:
        pd.DataFrame with columns: timestamp, bearing_id, vibration_data, machine_failure
    """
    print(f"📂 Loading {test_name}...")
    
    # Get all data files (exclude .rar and .pdf)
    files = sorted([
        f for f in os.listdir(test_dir) 
        if not f.endswith(('.rar', '.pdf')) and '.' in f and len(f.split('.')) == 6
    ])
    
    if max_files:
        files = files[:max_files]
    
    print(f"   Found {len(files)} files")
    
    # Identify failure bearing
    failure_bearing, failure_channels = identify_failure_bearing(test_name)
    
    # Get failure time (last file timestamp)
    failure_time = parse_filename_to_timestamp(files[-1])
    
    records = []
    
    for i, filename in enumerate(files):
        if i % 100 == 0:
            print(f"   Processing {i}/{len(files)}...", end='\r')
        
        filepath = os.path.join(test_dir, filename)
        timestamp = parse_filename_to_timestamp(filename)
        
        # Load vibration data
        vibration_data = load_vibration_file(filepath)
        if vibration_data is None:
            continue
        
        # Validate data shape (should have 8 columns)
        if vibration_data.ndim == 1 or vibration_data.shape[1] != 8:
            print(f"\n   ⚠️  Skipping {filename}: unexpected shape {vibration_data.shape}")
            continue
        
        # Calculate time to failure (in hours)
        time_to_failure = (failure_time - timestamp).total_seconds() / 3600
        
        # Label: 1 if within 48 hours of failure, 0 otherwise
        is_failure_imminent = 1 if time_to_failure <= 48 else 0
        
        # Create records for each bearing
        for bearing_id in range(1, 5):  # 4 bearings
            channel_start = (bearing_id - 1) * 2
            channel_end = channel_start + 2
            
            # Extract vibration signal for this bearing (2 channels)
            bearing_signal = vibration_data[:, channel_start:channel_end]
            
            # Use channel with higher RMS as primary signal
            rms_ch1 = np.sqrt(np.mean(bearing_signal[:, 0]**2))
            rms_ch2 = np.sqrt(np.mean(bearing_signal[:, 1]**2))
            
            primary_signal = bearing_signal[:, 0] if rms_ch1 > rms_ch2 else bearing_signal[:, 1]
            
            # Only label failure for the actual failing bearing
            label = is_failure_imminent if bearing_id == failure_bearing else 0
            
            records.append({
                'timestamp': timestamp,
                'machine_id': f'{test_name}_bearing_{bearing_id}',
                'bearing_id': bearing_id,
                'test_name': test_name,
                'vibration_signal': primary_signal,  # 20,480 samples
                'time_to_failure_hours': time_to_failure if bearing_id == failure_bearing else None,
                'machine_failure': label
            })
    
    print(f"\n   ✅ Loaded {len(records)} records")
    
    return pd.DataFrame(records)


def main():
    """
    Main execution: Load NASA data and save to database.
    """
    print("🚀 NASA IMS Bearing Data Integration")
    print("=" * 60)
    
    # Load test runs
    test_dirs = [
        ('data/NASA_PCOE_DATA/1st_test', '1st_test'),
        ('data/NASA_PCOE_DATA/2nd_test', '2nd_test'),
        ('data/NASA_PCOE_DATA/3rd_test', '3rd_test')
    ]
    
    all_data = []
    
    for test_dir, test_name in test_dirs:
        if os.path.exists(test_dir):
            # Load ALL files for maximum performance
            print(f"\n⚡ Loading COMPLETE dataset from {test_name}...")
            df = load_test_run(test_dir, test_name, max_files=None)  # Load ALL files
            all_data.append(df)
        else:
            print(f"⚠️  {test_dir} not found, skipping...")
    
    # Combine all tests
    df_combined = pd.concat(all_data, ignore_index=True)
    
    print(f"\n📊 Dataset Summary:")
    print(f"   Total records: {len(df_combined)}")
    print(f"   Unique machines: {df_combined['machine_id'].nunique()}")
    print(f"   Failure labels: {df_combined['machine_failure'].sum()} ({df_combined['machine_failure'].mean():.1%})")
    print(f"   Date range: {df_combined['timestamp'].min()} to {df_combined['timestamp'].max()}")
    
    # Save to database (without vibration_signal column for now)
    engine = get_db_engine()
    
    # Create simplified table for sensor_features
    df_features = df_combined[['machine_id', 'timestamp', 'machine_failure']].copy()
    
    # Add placeholder sensor values (will be replaced by real features)
    df_features['rotational_speed'] = 2000.0  # Constant for NASA data
    df_features['air_temperature'] = 298.0
    df_features['torque'] = 40.0
    df_features['tool_wear'] = 0.0
    
    # Save to database
    print("\n💾 Saving to database...")
    df_features.to_sql('sensor_features', engine, if_exists='replace', index=False)
    
    print(f"✅ Saved {len(df_features)} records to sensor_features table")
    
    # Save vibration signals to pickle for feature extraction
    print("\n💾 Saving vibration signals...")
    df_combined.to_pickle('data/nasa_vibration_signals.pkl')
    print("✅ Saved vibration signals to data/nasa_vibration_signals.pkl")
    
    return df_combined


if __name__ == "__main__":
    main()
