"""
RUL Training Data Generator
===========================
Generates synthetic run-to-failure data for XGBoost RUL prediction model.

Simulates 50 robots with exponential degradation curves from healthy to failure.

Author: ML Engineering Team
"""

import numpy as np
import pandas as pd
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

NUM_ROBOTS = 50  # Number of run-to-failure cycles
MIN_LIFETIME = 500  # Minimum operating hours to failure
MAX_LIFETIME = 1500  # Maximum operating hours to failure
SAMPLING_INTERVAL = 1  # Sample every N hours
OUTPUT_FILE = "rul_training_data.csv"

# Degradation parameters
START_VIBRATION = 0.1  # Healthy baseline (g)
FAILURE_THRESHOLD = 2.0  # Failure occurs above this (g)
NOISE_LEVEL = 0.05  # Random noise amplitude

# =============================================================================
# DEGRADATION CURVE FUNCTIONS
# =============================================================================

def generate_exponential_degradation(total_life, start_vib=START_VIBRATION, 
                                     failure_vib=FAILURE_THRESHOLD, noise=NOISE_LEVEL):
    """
    Generate exponential degradation curve from healthy to failure.
    
    Args:
        total_life: Total operating hours until failure
        start_vib: Starting vibration (healthy)
        failure_vib: Vibration at failure
        noise: Noise amplitude
    
    Returns:
        DataFrame with time, vibration, and RUL columns
    """
    # Time series from 0 to total_life
    time = np.arange(0, total_life + SAMPLING_INTERVAL, SAMPLING_INTERVAL)
    
    # Exponential growth: vib(t) = start_vib * exp(k*t)
    # Solve for k: failure_vib = start_vib * exp(k * total_life)
    # k = ln(failure_vib / start_vib) / total_life
    k = np.log(failure_vib / start_vib) / total_life
    
    # Generate exponential curve
    vibration_clean = start_vib * np.exp(k * time)
    
    # Add realistic noise (proportional to vibration level)
    noise_scale = noise * vibration_clean
    vibration_noisy = vibration_clean + np.random.normal(0, noise_scale, len(time))
    
    # Clip to ensure we don't go below start or way above failure
    vibration = np.clip(vibration_noisy, start_vib * 0.9, failure_vib * 1.2)
    
    # Calculate RUL (Remaining Useful Life)
    rul = total_life - time
    
    return pd.DataFrame({
        'time': time,
        'vibration': vibration,
        'RUL': rul
    })

# =============================================================================
# MAIN DATA GENERATION
# =============================================================================

def generate_training_data():
    """
    Generate complete training dataset with 50 run-to-failure cycles.
    """
    print("=" * 80)
    print("RUL TRAINING DATA GENERATOR")
    print("=" * 80)
    print()
    
    all_data = []
    
    for robot_id in range(1, NUM_ROBOTS + 1):
        # Random lifetime for this robot
        total_life = np.random.randint(MIN_LIFETIME, MAX_LIFETIME + 1)
        
        # Generate degradation curve
        robot_data = generate_exponential_degradation(total_life)
        
        # Add robot identifier
        robot_data['robot_id'] = robot_id
        robot_data['total_life'] = total_life
        
        # Add some additional features for ML model
        # (These can help the model learn degradation patterns)
        robot_data['time_pct'] = (robot_data['time'] / total_life) * 100  # % of lifetime elapsed
        robot_data['vibration_rate'] = robot_data['vibration'].diff().fillna(0)  # Rate of change
        
        all_data.append(robot_data)
        
        # Progress update
        if robot_id % 10 == 0:
            print(f"✓ Generated {robot_id}/{NUM_ROBOTS} robots...")
    
    # Combine all robots
    df = pd.concat(all_data, ignore_index=True)
    
    # Reorder columns for clarity
    df = df[['robot_id', 'time', 'vibration', 'vibration_rate', 'time_pct', 
             'RUL', 'total_life']]
    
    # Round for readability
    df['vibration'] = df['vibration'].round(4)
    df['vibration_rate'] = df['vibration_rate'].round(6)
    df['time_pct'] = df['time_pct'].round(2)
    
    return df

# =============================================================================
# DATA ANALYSIS & STATISTICS
# =============================================================================

def print_statistics(df):
    """Print summary statistics of the generated dataset."""
    print()
    print("=" * 80)
    print("DATASET STATISTICS")
    print("=" * 80)
    print()
    
    print(f"Total samples: {len(df):,}")
    print(f"Number of robots: {df['robot_id'].nunique()}")
    print()
    
    print("Lifetime Distribution:")
    print(f"  Min lifetime: {df['total_life'].min()} hours")
    print(f"  Max lifetime: {df['total_life'].max()} hours")
    print(f"  Avg lifetime: {df['total_life'].mean():.1f} hours")
    print()
    
    print("Vibration Statistics:")
    print(f"  Min vibration: {df['vibration'].min():.3f}g")
    print(f"  Max vibration: {df['vibration'].max():.3f}g")
    print(f"  Avg vibration: {df['vibration'].mean():.3f}g")
    print()
    
    print("RUL Statistics:")
    print(f"  Min RUL: {df['RUL'].min():.1f} hours")
    print(f"  Max RUL: {df['RUL'].max():.1f} hours")
    print(f"  Avg RUL: {df['RUL'].mean():.1f} hours")
    print()
    
    # Sample data
    print("Sample Data (first 10 rows):")
    print(df.head(10).to_string(index=False))
    print()
    
    print("Sample Data (random robot near failure):")
    # Get a robot near failure
    near_failure = df[df['RUL'] < 50].sample(min(10, len(df[df['RUL'] < 50])))
    if len(near_failure) > 0:
        print(near_failure.to_string(index=False))
    print()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Execute the data generation pipeline."""
    start_time = datetime.now()
    
    # Generate data
    df = generate_training_data()
    
    # Print statistics
    print_statistics(df)
    
    # Save to CSV
    print("=" * 80)
    print("SAVING DATA")
    print("=" * 80)
    print()
    
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✓ Saved training data to: {OUTPUT_FILE}")
    print(f"  File size: {len(df):,} rows × {len(df.columns)} columns")
    print()
    
    # Execution time
    duration = (datetime.now() - start_time).total_seconds()
    print(f"✓ Generation completed in {duration:.2f} seconds")
    print()
    
    # Next steps
    print("=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    print()
    print("1. Train XGBoost model:")
    print("   python train_xgboost_rul.py")
    print()
    print("2. Features available for training:")
    print("   - vibration (primary sensor)")
    print("   - vibration_rate (rate of change)")
    print("   - time_pct (% of lifetime elapsed)")
    print()
    print("3. Target variable: RUL (Remaining Useful Life)")
    print()
    print("=" * 80)
    print()

if __name__ == "__main__":
    main()
