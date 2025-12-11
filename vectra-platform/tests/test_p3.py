import numpy as np
import requests
import time
import mlflow
from sklearn.mixture import GaussianMixture

# 1. Setup Mock Environment
MLFLOW_URI = "http://localhost:5000"
INFERENCE_API = "http://localhost:8002/api/v1/predict/entry-point"
GEOHASH = "test_loc_1"

def generate_and_train():
    print("🧪 Generative Synthetic Data (Bimodal Distribution)...")
    # Simulate Front Door points
    front_door = np.random.normal(loc=[40.75, -73.98], scale=0.0001, size=(50, 2))
    # Simulate Loading Dock points
    loading_dock = np.random.normal(loc=[40.751, -73.981], scale=0.0001, size=(30, 2))
    data = np.vstack([front_door, loading_dock])

    print("🧠 Training GMM...")
    mlflow.set_tracking_uri(MLFLOW_URI)
    
    with mlflow.start_run(run_name="synthetic_test"):
        model = GaussianMixture(n_components=2).fit(data)
        mlflow.sklearn.log_model(
            model, 
            "model", 
            registered_model_name=f"gmm_entry_{GEOHASH}"
        )
        
    # Transition to Production (Simulated)
    client = mlflow.MlflowClient()
    latest_version = client.get_latest_versions(f"gmm_entry_{GEOHASH}", stages=["None"])[0].version
    client.transition_model_version_stage(
        name=f"gmm_entry_{GEOHASH}",
        version=latest_version,
        stage="Production"
    )
    print("✅ Model Trained and Promoted to Production.")

def query_inference():
    print("🚀 Querying Inference Service...")
    # Wait for API to pick up new model
    time.sleep(2) 
    
    payload = {"geohash": GEOHASH}
    try:
        resp = requests.post(INFERENCE_API, json=payload)
        data = resp.json()
        print("\nPredicted Entry Points:")
        for ep in data['entry_points']:
            print(f"  - {ep['type']}: Lat={ep['lat']:.5f}, Lon={ep['lon']:.5f}, Prob={ep['probability']:.2f}")
    except Exception as e:
        print(f"❌ Inference Failed: {e}")

if __name__ == "__main__":
    generate_and_train()
    query_inference()