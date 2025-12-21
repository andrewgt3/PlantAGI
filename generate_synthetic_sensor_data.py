"""
Synthetic Data Generator for Predictive Maintenance MVP
========================================================
Generates realistic "dirty" sensor data for ABB IRB 6700 Robotic Arm
with environmental context and hidden fault scenarios.

Author: Senior Data Scientist - Industrial IoT
Target: ABB IRB 6700 Robotic Arm
Purpose: Prove AI agent can handle noisy real-world data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION
# =============================================================================

# Simulation Parameters
SIMULATION_DAYS = 7
SENSOR_FREQUENCY_HZ = 1  # 1 row per second
ASSET_ID = "ABB_IRB6700_1"

# Data Quality Parameters
NULL_INJECTION_RATE = 0.03  # 3% of rows will have NaN values

# Timing Parameters
SHIFT_START_HOUR = 6   # 6 AM
SHIFT_END_HOUR = 22    # 10 PM
DEGRADATION_DELAY_MINUTES = 15  # Delay after cleaning crew event
DEGRADATION_DURATION_MINUTES = 120  # How long the effect lasts

# Sensor Baseline Parameters
TORQUE_AMPLITUDE = 50.0  # Nm (sine wave amplitude)
TORQUE_MEAN = 100.0      # Nm (sine wave center)
TORQUE_NOISE_STD = 5.0   # Nm (Gaussian noise)
TORQUE_CYCLE_PERIOD = 300  # seconds (5-minute cycles)

TEMP_BASE = 45.0         # °C (baseline temperature)
TEMP_SHIFT_INCREASE = 15.0  # °C (increase during shifts)
TEMP_NOISE_STD = 2.0     # °C (random variation)
TEMP_WARMUP_DURATION = 3600  # seconds (1 hour to reach operating temp)

VIBRATION_BASE = 0.2     # g (baseline vibration)
VIBRATION_NOISE_STD = 0.05  # g (normal noise)
VIBRATION_SPIKE_PROB = 0.02  # 2% chance of noise spike per second

# Degradation Parameters (Hidden Scenario)
VIBRATION_INCREASE_FACTOR = 1.20  # 20% increase after cleaning crew
TORQUE_SPIKE_MAGNITUDE = 15.0     # Nm (micro-spikes during degradation)
TORQUE_SPIKE_PROB = 0.05          # 5% chance during degradation period

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def generate_timestamps(start_date, days, freq_hz):
    """Generate timestamp array for the entire simulation period."""
    total_seconds = days * 24 * 3600
    timestamps = [
        start_date + timedelta(seconds=i)
        for i in range(0, total_seconds, 1//freq_hz)
    ]
    return timestamps

def is_shift_time(timestamp):
    """Check if timestamp falls within shift hours (6 AM - 10 PM)."""
    return SHIFT_START_HOUR <= timestamp.hour < SHIFT_END_HOUR

def generate_torque_signal(timestamps):
    """
    Generate realistic joint torque signal.
    - Sine wave pattern (simulating repetitive cycles)
    - Gaussian noise
    """
    torque_values = []
    for i, ts in enumerate(timestamps):
        # Sine wave component (repetitive work cycles)
        cycle_phase = (2 * np.pi * i) / (TORQUE_CYCLE_PERIOD * SENSOR_FREQUENCY_HZ)
        sine_component = TORQUE_AMPLITUDE * np.sin(cycle_phase)
        
        # Gaussian noise
        noise = np.random.normal(0, TORQUE_NOISE_STD)
        
        torque = TORQUE_MEAN + sine_component + noise
        torque_values.append(max(0, torque))  # Physical constraint: torque >= 0
    
    return np.array(torque_values)

def generate_temperature_signal(timestamps):
    """
    Generate motor temperature signal.
    - Slow drift upward during shift hours
    - Cooling down at night
    - Gradual warmup at shift start
    """
    temp_values = []
    current_temp = TEMP_BASE
    
    for i, ts in enumerate(timestamps):
        if is_shift_time(ts):
            # During shift: gradually warm up toward operating temp
            target_temp = TEMP_BASE + TEMP_SHIFT_INCREASE
            # Exponential approach to target
            delta = (target_temp - current_temp) * 0.0003  # Slow drift
            current_temp += delta
        else:
            # At night: cool down toward baseline
            delta = (TEMP_BASE - current_temp) * 0.0005  # Slightly faster cooling
            current_temp += delta
        
        # Add noise
        noise = np.random.normal(0, TEMP_NOISE_STD)
        temp_values.append(current_temp + noise)
    
    return np.array(temp_values)

def generate_vibration_signal(timestamps):
    """
    Generate vibration signal.
    - Baseline noise
    - Random spikes (NOT failures, just environmental noise)
    """
    vibration_values = []
    
    for ts in timestamps:
        # Baseline vibration with Gaussian noise
        base_vib = VIBRATION_BASE + np.random.normal(0, VIBRATION_NOISE_STD)
        
        # Occasional random spikes (environmental noise, not failures)
        if np.random.random() < VIBRATION_SPIKE_PROB:
            spike = np.random.uniform(0.1, 0.3)  # Random spike magnitude
            base_vib += spike
        
        vibration_values.append(max(0, base_vib))
    
    return np.array(vibration_values)

def inject_null_values(df, rate):
    """
    Randomly inject NaN values to simulate sensor packet loss.
    Applies to all sensor columns (not timestamp or asset_id).
    """
    sensor_columns = ['joint_1_torque', 'vibration_x', 'motor_temp_c']
    
    for col in sensor_columns:
        # Randomly select rows to nullify
        null_mask = np.random.random(len(df)) < rate
        df.loc[null_mask, col] = np.nan
    
    return df

def generate_context_events(start_date, days):
    """
    Generate plant context events (Stream B).
    Includes shift changes, maintenance, material loads, and cleaning crew events.
    """
    events = []
    current_date = start_date
    
    staff_pool = ['EMP_001', 'EMP_002', 'EMP_003', 'EMP_004', 'EMP_005']
    
    for day in range(days):
        day_start = current_date + timedelta(days=day)
        
        # Morning shift start
        events.append({
            'timestamp': day_start.replace(hour=SHIFT_START_HOUR, minute=0, second=0),
            'event_type': 'Shift_Start',
            'staff_id': np.random.choice(staff_pool)
        })
        
        # Material loads (3-5 times per shift)
        num_loads = np.random.randint(3, 6)
        for _ in range(num_loads):
            load_hour = np.random.randint(SHIFT_START_HOUR, SHIFT_END_HOUR)
            load_minute = np.random.randint(0, 60)
            events.append({
                'timestamp': day_start.replace(hour=load_hour, minute=load_minute, second=0),
                'event_type': 'Material_Load',
                'staff_id': np.random.choice(staff_pool)
            })
        
        # Maintenance entries (occasional, ~30% chance per day)
        if np.random.random() < 0.3:
            maint_hour = np.random.randint(SHIFT_START_HOUR, SHIFT_END_HOUR - 1)
            events.append({
                'timestamp': day_start.replace(hour=maint_hour, minute=30, second=0),
                'event_type': 'Maintenance_Entry',
                'staff_id': 'MAINT_TECH'
            })
        
        # THE HIDDEN SCENARIO: Cleaning Crew Zone 3
        # This event triggers the degradation
        # Occurs 2-3 times during the week
        if np.random.random() < 0.35:  # ~35% chance per day = 2-3 times over 7 days
            clean_hour = np.random.randint(SHIFT_END_HOUR - 2, SHIFT_END_HOUR)  # Late shift
            events.append({
                'timestamp': day_start.replace(hour=clean_hour, minute=45, second=0),
                'event_type': 'Cleaning_Crew_Zone_3',
                'staff_id': 'CLEAN_CREW'
            })
        
        # Evening shift end
        events.append({
            'timestamp': day_start.replace(hour=SHIFT_END_HOUR, minute=0, second=0),
            'event_type': 'Shift_End',
            'staff_id': np.random.choice(staff_pool)
        })
    
    # Sort by timestamp
    events_df = pd.DataFrame(events)
    events_df = events_df.sort_values('timestamp').reset_index(drop=True)
    
    return events_df

def apply_degradation_scenario(sensor_df, context_df):
    """
    Apply the hidden degradation scenario.
    
    Logic:
    - Every time 'Cleaning_Crew_Zone_3' occurs, trigger degradation
    - 15 minutes AFTER the event, increase vibration_x by 20%
    - Add micro-spikes to joint_1_torque
    - Effect lasts for ~2 hours
    """
    # Find all cleaning crew events
    cleaning_events = context_df[context_df['event_type'] == 'Cleaning_Crew_Zone_3']['timestamp'].values
    
    degradation_count = 0
    
    for event_time in cleaning_events:
        # Convert to datetime if needed
        if isinstance(event_time, str):
            event_time = pd.to_datetime(event_time)
        elif isinstance(event_time, np.datetime64):
            event_time = pd.Timestamp(event_time)
        
        # Define degradation window: starts 15 min after event, lasts 2 hours
        degradation_start = event_time + timedelta(minutes=DEGRADATION_DELAY_MINUTES)
        degradation_end = degradation_start + timedelta(minutes=DEGRADATION_DURATION_MINUTES)
        
        # Create mask for affected rows
        mask = (sensor_df['timestamp'] >= degradation_start) & \
               (sensor_df['timestamp'] < degradation_end)
        
        affected_rows = mask.sum()
        if affected_rows > 0:
            degradation_count += 1
            
            # Apply 20% increase to vibration_x
            sensor_df.loc[mask, 'vibration_x'] = \
                sensor_df.loc[mask, 'vibration_x'] * VIBRATION_INCREASE_FACTOR
            
            # Add micro-spikes to joint_1_torque
            # 5% of affected rows get spikes
            spike_mask = mask & (np.random.random(len(sensor_df)) < TORQUE_SPIKE_PROB)
            sensor_df.loc[spike_mask, 'joint_1_torque'] += \
                np.random.uniform(TORQUE_SPIKE_MAGNITUDE * 0.5, 
                                 TORQUE_SPIKE_MAGNITUDE * 1.5, 
                                 spike_mask.sum())
    
    return sensor_df, degradation_count

# =============================================================================
# MAIN GENERATION PIPELINE
# =============================================================================

def main():
    """Main execution pipeline."""
    print("=" * 70)
    print("SYNTHETIC DATA GENERATOR - PREDICTIVE MAINTENANCE MVP")
    print("Target: ABB IRB 6700 Robotic Arm")
    print("=" * 70)
    print()
    
    # Set random seed for reproducibility (optional - remove for true randomness)
    np.random.seed(42)
    
    # 1. Generate timestamps
    print("📅 Generating timestamps...")
    start_date = datetime.now() - timedelta(days=SIMULATION_DAYS)
    timestamps = generate_timestamps(start_date, SIMULATION_DAYS, SENSOR_FREQUENCY_HZ)
    print(f"   ✓ Generated {len(timestamps):,} timestamps ({SIMULATION_DAYS} days @ {SENSOR_FREQUENCY_HZ}Hz)")
    
    # 2. Generate sensor signals (Stream A)
    print()
    print("🔧 Generating sensor signals...")
    
    torque = generate_torque_signal(timestamps)
    print(f"   ✓ Joint torque (sine + noise)")
    
    temperature = generate_temperature_signal(timestamps)
    print(f"   ✓ Motor temperature (shift-aware drift)")
    
    vibration = generate_vibration_signal(timestamps)
    print(f"   ✓ Vibration (baseline + random spikes)")
    
    # 3. Create sensor DataFrame
    sensor_df = pd.DataFrame({
        'timestamp': timestamps,
        'asset_id': ASSET_ID,
        'joint_1_torque': torque,
        'vibration_x': vibration,
        'motor_temp_c': temperature
    })
    
    # 4. Inject "dirt": null values
    print()
    print(f"💉 Injecting data quality issues...")
    sensor_df = inject_null_values(sensor_df, NULL_INJECTION_RATE)
    null_count = sensor_df.isnull().sum().sum()
    print(f"   ✓ Injected {null_count:,} NaN values ({NULL_INJECTION_RATE*100}% of sensor readings)")
    
    # 5. Generate context events (Stream B)
    print()
    print("📋 Generating plant context events...")
    context_df = generate_context_events(start_date, SIMULATION_DAYS)
    print(f"   ✓ Generated {len(context_df)} context events")
    
    cleaning_events = (context_df['event_type'] == 'Cleaning_Crew_Zone_3').sum()
    print(f"   ✓ Cleaning Crew Zone 3 events: {cleaning_events}")
    
    # 6. Apply hidden degradation scenario
    print()
    print("⚡ Applying hidden degradation scenario...")
    sensor_df, degradation_count = apply_degradation_scenario(sensor_df, context_df)
    print(f"   ✓ Applied degradation to {degradation_count} time windows")
    print(f"   ✓ Effect: +20% vibration_x, torque micro-spikes")
    print(f"   ✓ Timing: 15min delay, 2hr duration per event")
    
    # 7. Save outputs
    print()
    print("💾 Saving outputs...")
    
    sensor_output = 'sensor_data_dirty.csv'
    context_output = 'context_logs.csv'
    
    sensor_df.to_csv(sensor_output, index=False)
    print(f"   ✓ Saved {sensor_output}")
    
    context_df.to_csv(context_output, index=False)
    print(f"   ✓ Saved {context_output}")
    
    # 8. Print summary
    print()
    print("=" * 70)
    print("GENERATION SUMMARY")
    print("=" * 70)
    print(f"Sensor Data Rows:          {len(sensor_df):,}")
    print(f"Context Event Rows:        {len(context_df)}")
    print(f"Simulation Period:         {SIMULATION_DAYS} days")
    print(f"Data Frequency:            {SENSOR_FREQUENCY_HZ} Hz (1 sample/second)")
    print(f"Total NaN Values:          {null_count:,} ({null_count/len(sensor_df)/3*100:.2f}% per column)")
    print(f"Cleaning Crew Events:      {cleaning_events}")
    print(f"Degradation Windows:       {degradation_count}")
    print()
    print("Sensor Statistics:")
    print(sensor_df[['joint_1_torque', 'vibration_x', 'motor_temp_c']].describe())
    print()
    print("✅ GENERATION COMPLETE!")
    print("=" * 70)

if __name__ == "__main__":
    main()
