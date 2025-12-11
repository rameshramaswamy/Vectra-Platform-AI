import mlflow.onnx
import onnxruntime as rt
import numpy as np
from cachetools import LRUCache, cachedmethod
from cachetools.keys import hashkey
from threading import RLock
import structlog

logger = structlog.get_logger()

class ONNXModelManager:
    def __init__(self, tracking_uri, cache_size=1000):
        mlflow.set_tracking_uri(tracking_uri)
        # LRU Cache: Keep 1000 most active geohashes in memory
        self.cache = LRUCache(maxsize=cache_size)
        self.lock = RLock()

    def _load_model_from_mlflow(self, geohash: str):
        model_name = f"gmm_entry_{geohash}"
        try:
            # Download ONNX artifact
            model_uri = f"models:/{model_name}/Production"
            local_path = mlflow.artifacts.download_artifacts(model_uri)
            
            # Initialize optimized runtime session
            # providers=['CPUExecutionProvider'] (or CUDA if GPU avail)
            sess = rt.InferenceSession(f"{local_path}/model.onnx", providers=['CPUExecutionProvider'])
            return sess
        except Exception as e:
            logger.debug(f"Model load failed for {geohash} (might utilize heuristic): {e}")
            return None

    def get_session(self, geohash: str):
        """Thread-safe, cached retrieval of inference session"""
        with self.lock:
            if geohash in self.cache:
                return self.cache[geohash]
            
            sess = self._load_model_from_mlflow(geohash)
            if sess:
                self.cache[geohash] = sess
            return sess

    def predict_gmm(self, geohash: str):
        sess = self.get_session(geohash)
        if not sess:
            return None

        # GMM ONNX output usually contains probabilities and labels
        # Note: skl2onnx output structure varies by conversion options.
        # For GMM, we often need to inspect the node outputs or rely on custom operators.
        # Here we simulate the extraction of Means (Centroids) which are encoded in the model parameters,
        # OR we run inference on dummy data to get probability density if the model supports it.
        
        # Enterprise Strategy: 
        # Instead of running 'predict' on input, we extract the stored means 
        # directly from the ONNX graph initializers to serve as Entry Points.
        
        try:
            # Extract Means/Weights directly from ONNX initializers (super fast)
            # This avoids running the compute engine entirely just to find the centers.
            means = None
            weights = None
            
            for node in sess.get_modelmeta().graph.initializer:
                if "means" in node.name:
                    means = np.frombuffer(node.raw_data, dtype=np.float32).reshape(-1, 2)
                if "weights" in node.name:
                    weights = np.frombuffer(node.raw_data, dtype=np.float32)

            # Fallback if specific extraction fails: heuristic or return None
            if means is not None:
                return means, weights
                
        except Exception:
            pass
            
        return None