import requests
import json

API_URL = "http://localhost:8001/api/v1"

def test_flow():
    # 1. Driver requests a location
    # (Assuming we ran the refinery and populated geohash 'dr5ruj4')
    target_address = "dr5ruj4" 
    
    print(f"Requesting Navigation for {target_address}...")
    try:
        resp = requests.get(f"{API_URL}/resolve/{target_address}")
        if resp.status_code == 200:
            data = resp.json()
            print("✅ Received Location:")
            print(f"   Park at: {data['navigation_point']}")
            print(f"   Walk to: {data['entry_point']}")
        else:
            print("⚠️ Address not refined yet.")
            return
    except:
        print("❌ API is down.")
        return

    # 2. Driver arrives and gives feedback
    print("\nSimulating Driver Feedback...")
    feedback = {
        "address_id": target_address,
        "driver_id": "D-999",
        "is_np_ok": True,
        "is_ep_ok": False, # Entry point was wrong
        "corrected_lat": 40.7135,
        "corrected_lon": -74.0065,
        "comment": "Door is actually around the corner"
    }
    
    resp = requests.post(f"{API_URL}/feedback", json=feedback)
    if resp.status_code == 200:
        print("✅ Feedback recorded (Ground Truth stored).")

if __name__ == "__main__":
    test_flow()