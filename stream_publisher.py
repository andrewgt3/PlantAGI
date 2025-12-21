import random

# ... imports ...

# ... inside loop ...
            # Inject simulated sensor noise (+/- 0.5 K)
            noise = random.uniform(-0.5, 0.5)
            payload = {
                "timestamp": datetime.now().isoformat(),
                "machine_id": str(row['Product ID']),
                "temperature_air": float(row['Air temperature [K]']) + noise,
                "rotational_speed": float(row['Rotational speed [rpm]']),
                "torque": float(row['Torque [Nm]']),
                "tool_wear": float(row['Tool wear [min]']),
                "machine_failure": int(row['Machine failure']) 
            }
            
            r.publish(CHANNEL, json.dumps(payload))
            
            count += 1
            if count % 500 == 0:
                print(f"📡 Progress: {count}/{total_records} ({count/total_records*100:.1f}%)", end='\r')
                
            time.sleep(sleep_delay)
            
    except KeyboardInterrupt:
        print("\n🛑 Stream stopped by user.")
    except Exception as e:
        print(f"\n❌ Error streaming data: {e}")
        
    print(f"\n✅ Stream finished. Published {count} total events.")

if __name__ == "__main__":
    stream_data()
