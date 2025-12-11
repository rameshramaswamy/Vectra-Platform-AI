import requests
import random
import uuid

API_URL = "http://localhost:8004" # Wi-Fi Service Port

def generate_mac():
    return ':'.join(['{:02x}'.format((uuid.uuid4().int >> ele) & 0xff) for ele in range(0,8*6,8)][::-1])

def run_simulation():
    # 1. Setup a "Building" with static routers
    routers = [generate_mac() for _ in range(5)]
    center_lat, center_lon = 40.7580, -73.9855
    
    print("📡 Training Radio Map...")
    # Simulate 50 scans at slight variations (Training Data)
    for _ in range(50):
        lat = center_lat + random.uniform(-0.0001, 0.0001)
        lon = center_lon + random.uniform(-0.0001, 0.0001)
        
        # Simulate signal degradation
        scan = {mac: random.randint(-60, -40) for mac in routers}
        
        requests.post(f"{API_URL}/ingest", json={
            "latitude": lat, "longitude": lon, "wifi_scan": scan
        })

    print("🔍 Testing Localization...")
    # Simulate a user with weak GPS but good Wi-Fi visibility
    test_scan = {mac: random.randint(-60, -40) for mac in routers}
    
    # User thinks they are 100m away (GPS Drift)
    drifted_lat = center_lat + 0.001 
    
    resp = requests.post(f"{API_URL}/locate", json={
        "latitude": drifted_lat, "longitude": center_lon, "wifi_scan": test_scan
    })
    
    result = resp.json()
    print("Result:", result)
    
    if result.get("source") == "wifi_knn":
        est_lat = result['location']['lat']
        error = abs(est_lat - center_lat)
        print(f"✅ GPS Error: ~110m | Wi-Fi Refined Error: ~{error*111000:.2f}m")

if __name__ == "__main__":
    run_simulation()