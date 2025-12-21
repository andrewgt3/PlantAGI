"""
Quick verification script to test the complete backend configuration
"""

import subprocess
import sys

def run_check(description, command):
    """Run a verification check"""
    print(f"\n{'='*60}")
    print(f"🔍 {description}")
    print(f"{'='*60}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"✅ PASS")
            if result.stdout:
                print(result.stdout[:500])  # First 500 chars
            return True
        else:
            print(f"❌ FAIL")
            if result.stderr:
                print(result.stderr[:500])
            return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def main():
    print("🚀 PDM Backend Configuration Verification")
    print("="*60)
    
    checks = [
        ("Check augmented tables exist", 
         "psql -U postgres -d pdm_timeseries -c '\\dt *augmented*' -h localhost"),
        
        ("Verify MongoDB is running",
         "docker ps | grep mongodb"),
        
        ("Check model file exists",
         "ls -lh models/failure_model.pkl"),
        
        ("Test MongoDB connection",
         "python3 test_mongodb_audit.py"),
    ]
    
    results = []
    for desc, cmd in checks:
        results.append(run_check(desc, cmd))
    
    print(f"\n{'='*60}")
    print(f"📊 VERIFICATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total Checks: {len(results)}")
    print(f"Passed: {sum(results)}")
    print(f"Failed: {len(results) - sum(results)}")
    
    if all(results):
        print(f"\n✅ ALL CHECKS PASSED - System Ready!")
    else:
        print(f"\n⚠️  Some checks failed - Review output above")
    
    return all(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
