import mlflow
import json
from cachetools import LRUCache
from threading import RLock

class ArtifactManager:
    def __init__(self, tracking_uri):
        mlflow.set_tracking_uri(tracking_uri)
        self.cache = LRUCache(maxsize=5000) # Cache 5000 locations
        self.lock = RLock()

    def get_entry_points(self, geohash: str):
        with self.lock:
            if geohash in self.cache:
                return self.cache[geohash]

            try:
                # Load JSON artifact directly (Very fast, no model init overhead)
                model_name = f"gmm_entry_{geohash}"
                uri = f"models:/{model_name}/Production"
                path = mlflow.artifacts.download_artifacts(artifact_uri=f"{uri}/metadata.json")
                
                with open(path, 'r') as f:
                    data = json.load(f)
                    self.cache[geohash] = data
                    return data
            except Exception:
                return None