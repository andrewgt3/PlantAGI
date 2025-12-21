"""
Generate time-lagged labels for sensor_features table.
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

def generate_degradation_timeline(num_steps, machine_id_hash=0):
    """Generate continuous degradation parameter D_t ∈ [0, 1]."""
    np.random.seed(machine_id_hash)
    
    t = np.arange(num_steps)
    D_t = 0.1 + (0.95 - 0.1) * (t / num_steps)
    noise = np.random.normal(0, 0.05, num_steps)
    D_t = D_t + noise
    D_t = np.clip(D_t, 0, 1)
    
    return D_t

def assign_lagged_labels(D_t, delta_t=50, threshold=0.5):
    """Assign failure labels Y_t based on lagged degradation D_{t-Δt}."""
    Y_t = np.zeros(len(D_t), dtype=int)
    
    for t in range(delta_t, len(D_t)):
        if D_t[t - delta_t] > threshold:
            Y_t[t] = 1
    
    return Y_t

def main():
    print("🔄 Generating time-lagged labels...")
    
    engine = get_db_engine()
    
    # Load sensor_features
    df = pd.read_sql("SELECT * FROM sensor_features ORDER BY machine_id, timestamp", engine)
    print(f"   Loaded {len(df)} records")
    
    # Generate labels for each machine
    df['machine_failure'] = 0
    
    for machine_id in df['machine_id'].unique():
        machine_mask = df['machine_id'] == machine_id
        machine_df = df[machine_mask]
        
        num_steps = len(machine_df)
        machine_id_hash = hash(machine_id) % 10000
        
        # Generate degradation timeline
        D_t = generate_degradation_timeline(num_steps, machine_id_hash)
        
        # Assign lagged labels (Δt=50, threshold=0.3 for balanced dataset)
        Y_t = assign_lagged_labels(D_t, delta_t=50, threshold=0.3)
        
        # Update dataframe
        df.loc[machine_mask, 'machine_failure'] = Y_t
    
    # Count labels
    failure_count = df['machine_failure'].sum()
    total_count = len(df)
    print(f"   Generated labels: {failure_count} failures / {total_count} total ({failure_count/total_count:.1%})")
    
    # Update database using pandas
    print("💾 Updating database...")
    df[['machine_id', 'timestamp', 'machine_failure']].to_sql(
        'sensor_features_temp',
        engine,
        if_exists='replace',
        index=False
    )
    
    # Update original table
    with engine.connect() as conn:
        conn.execute("""
            UPDATE sensor_features sf
            SET machine_failure = sft.machine_failure
            FROM sensor_features_temp sft
            WHERE sf.machine_id = sft.machine_id AND sf.timestamp = sft.timestamp
        """)
        conn.commit()
    
    print("✅ Labels updated successfully")

if __name__ == "__main__":
    main()
