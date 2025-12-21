
import pandas as pd
import json
import os
import numpy as np

CSV_PATH = "data/feature_store.csv"
TOPO_PATH = "data/plant_topology.json"

def main():
    print("🚀 Starting Proprietary Data Synthesis...")
    
    # 1. Load Data
    if not os.path.exists(CSV_PATH):
        print(f"❌ {CSV_PATH} not found.")
        return
        
    df = pd.read_csv(CSV_PATH)
    print(f"   Loaded {len(df)} records.")
    
    # 2. Extract Top 8 IDs for Topology
    unique_ids = df['machine_id'].unique()
    if len(unique_ids) < 8:
        print("⚠️ Not enough unique IDs for 8-robot topo. Using repetitions.")
        # repeat if needed (unlikely)
        
    robots = unique_ids[:8]
    print(f"   Selected Robots: {robots}")
    
    # 3. Define Topology (1->2->3->6, 4->5->6, 6->7->8)
    # Mapping indices 0..7 to 1..8 logic
    # 0->1->2->5<-4<-3
    #          |
    #          v
    #          6->7
    
    # Map:
    # R1: robots[0] (Source A)
    # R2: robots[1]
    # R3: robots[2] (Feeds R6)
    # R4: robots[3] (Source B)
    # R5: robots[4] (Feeds R6)
    # R6: robots[5] (Junction)
    # R7: robots[6]
    # R8: robots[7] (Final)
    
    topo_nodes = [
        {"id": "R1", "physical_id": robots[0], "label": "Robot 1 (Intake A)", "criticality": "C"},
        {"id": "R2", "physical_id": robots[1], "label": "Robot 2 (Weld A)",   "criticality": "C"},
        {"id": "R3", "physical_id": robots[2], "label": "Robot 3 (Inspect A)","criticality": "B"},
        {"id": "R4", "physical_id": robots[3], "label": "Robot 4 (Intake B)", "criticality": "C"},
        {"id": "R5", "physical_id": robots[4], "label": "Robot 5 (Weld B)",   "criticality": "B"},
        {"id": "R6", "physical_id": robots[5], "label": "Robot 6 (Assembly)", "criticality": "A"},
        {"id": "R7", "physical_id": robots[6], "label": "Robot 7 (Paint)",    "criticality": "A"},
        {"id": "R8", "physical_id": robots[7], "label": "Robot 8 (Packing)",  "criticality": "A"}
    ]
    
    topo_edges = [
        ["R1", "R2"],
        ["R2", "R3"],
        ["R3", "R6"],
        ["R4", "R5"],
        ["R5", "R6"],
        ["R6", "R7"],
        ["R7", "R8"]
    ]
    
    topo_data = {
        "nodes": topo_nodes,
        "edges": topo_edges
    }
    
    with open(TOPO_PATH, "w") as f:
        json.dump(topo_data, f, indent=2)
    print(f"   ✅ Saved {TOPO_PATH}")
    
    # 4. Feature Engineering
    print("🛠 Engineering Features...")
    
    # A. Criticality Rating
    # Map physical ID to criticality
    crit_map = {n['physical_id']: n['criticality'] for n in topo_nodes}
    
    # Apply map (default C if not in topo)
    df['criticality_rating'] = df['machine_id'].map(crit_map).fillna('C')
    
    # B. Dynamic Wear Pct
    # Use max tool_wear per machine as baseline? Or global max?
    # Global max in CSV seems to be around 246 based on head view.
    # Let's assume max is 300.
    MAX_WEAR = 300.0
    df['current_tool_wear_pct'] = df['tool_wear'] / MAX_WEAR
    df['current_tool_wear_pct'] = df['current_tool_wear_pct'].clip(upper=1.0)
    
    # Save
    df.to_csv(CSV_PATH, index=False)
    print(f"   ✅ Features added. Saved to {CSV_PATH}")
    
if __name__ == "__main__":
    main()
