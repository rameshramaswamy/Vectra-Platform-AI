from feast import FeatureStore
from datetime import datetime, timedelta

def run_materialization():
    store = FeatureStore(repo_path=".")
    
    print("🚀 Materializing features to Online Store (Redis)...")
    # This loads data from the offline parquet into Redis for low-latency inference
    store.materialize(
        end_date=datetime.now(),
        start_date=datetime.now() - timedelta(days=7)
    )
    
    print("✅ Materialization Complete. Inference API will now see fresh data.")

if __name__ == "__main__":
    run_materialization()