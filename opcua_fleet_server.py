import asyncio
import logging
import random
from asyncua import Server, ua

# --- CONFIGURATION ---
ROBOT_IDS = ["ROBOT_1", "ROBOT_2", "ROBOT_3", "ROBOT_4"]

async def main():
    # Setup Server
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")
    server.set_server_name("Gaia Virtual Fleet")

    # Setup Namespace
    idx = await server.register_namespace("http://gaiapredictive.com/fleet")
    objects = server.nodes.objects

    # --- BUILD THE FLEET ---
    robot_nodes = {}
    
    # Create a Master Trigger for the Demo
    # Toggle this to True to start the "Cascade Failure"
    cascade_trigger = await objects.add_variable(idx, "Cascade_Failure_Active", False)
    await cascade_trigger.set_writable()
    
    print(f"🏗️ Building Virtual Fleet ({len(ROBOT_IDS)} Assets)...")
    
    for r_id in ROBOT_IDS:
        # Create Folder for each Robot
        obj = await objects.add_object(idx, r_id)
        
        # Add Sensors
        vib = await obj.add_variable(idx, "Vibration_X", 0.1)
        trq = await obj.add_variable(idx, "Torque_J1", 45.0)
        tmp = await obj.add_variable(idx, "Motor_Temp", 55.0)
        
        # Make them writable (so we can update them in loop)
        await vib.set_writable()
        await trq.set_writable()
        await tmp.set_writable()
        
        robot_nodes[r_id] = {'vib': vib, 'trq': trq, 'tmp': tmp}

    print("✅ Server Started at opc.tcp://localhost:4840")
    print("   -> Toggle 'Cascade_Failure_Active' to trigger fleet meltdown.")
    
    async with server:
        while True:
            # Check if Cascade Mode is Active
            is_cascade = await cascade_trigger.read_value()
            
            for r_id, sensors in robot_nodes.items():
                # Base Noise
                v_noise = random.uniform(-0.01, 0.01)
                
                # --- PHYSICS LOGIC ---
                if not is_cascade:
                    # NORMAL OPERATION
                    new_vib = 0.12 + v_noise
                    new_trq = 45.0 + random.uniform(-2, 2)
                    new_tmp = 55.0 + random.uniform(-0.5, 0.5)
                
                else:
                    # 🚨 CASCADE FAILURE MODE 🚨
                    if r_id == "ROBOT_1":
                        # The Cause: Conveyor Jam (High Torque, High Vib)
                        new_vib = 2.5 + random.uniform(-0.2, 0.2) # Critical
                        new_trq = 180.0 + random.uniform(-10, 10) # Stalled
                        new_tmp = 85.0 + random.uniform(0, 0.5)   # Overheating
                    
                    elif r_id in ["ROBOT_2", "ROBOT_3"]:
                        # The Effect: Starved (Zero Torque, Idle Vib)
                        new_vib = 0.01 + (v_noise * 0.1)          # Idle
                        new_trq = 0.0                             # No parts
                        new_tmp = 55.0 - 0.1                      # Cooling down
                        
                    elif r_id == "ROBOT_4":
                        # The Compensation: Speeding Up (High Vib, Normal Torque)
                        new_vib = 0.65 + v_noise                  # Warning level
                        new_trq = 50.0 + random.uniform(-5, 5)
                        new_tmp = 65.0 + random.uniform(0, 0.2)

                # Update Server Values
                await sensors['vib'].write_value(new_vib)
                await sensors['trq'].write_value(new_trq)
                await sensors['tmp'].write_value(new_tmp)

            await asyncio.sleep(0.5) # 2Hz Update Rate

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
