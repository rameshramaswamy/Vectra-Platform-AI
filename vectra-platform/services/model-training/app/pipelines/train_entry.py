import ray
import numpy as np
import mlflow
import mlflow.onnx
from sklearn.mixture import GaussianMixture
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import structlog
from app.core.config import settings

logger = structlog.get_logger()

@ray.remote
def train_single_gmm(geohash: str, coords: np.ndarray):
    """
    Ray Task: Trains a single GMM and exports to ONNX.
    Running in parallel across the cluster.
    """
    if len(coords) < 5:
        return None

    try:
        # 1. Train Scikit-Learn Model
        model = GaussianMixture(n_components=2, covariance_type='full', random_state=42)
        model.fit(coords)
        
        # 2. Convert to ONNX (High Performance Runtime)
        # Define input shape: None (batch size) x 2 (Lat/Lon)
        initial_type = [('float_input', FloatTensorType([None, 2]))]
        onnx_model = convert_sklearn(model, initial_types=initial_type)
        
        # 3. Log to MLflow
        mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
        with mlflow.start_run(run_name=f"gmm_{geohash}"):
            mlflow.log_metric("aic", model.aic(coords))
            metadata = {
                "means": model.means_.tolist(),
                "weights": model.weights_.tolist(),
                "covariances": model.covariances_.tolist()
            }
            mlflow.log_dict(metadata, "metadata.json")      
                  
            # Save raw ONNX binary
            mlflow.onnx.log_model(
                onnx_model,
                artifact_path="model",
                registered_model_name=f"gmm_entry_{geohash}"
            )
            
            # Transition to Production (Auto-promote for this demo)
            client = mlflow.MlflowClient()
            latest = client.get_latest_versions(f"gmm_entry_{geohash}", stages=["None"])[0]
            client.transition_model_version_stage(
                name=f"gmm_entry_{geohash}",
                version=latest.version,
                stage="Production"
            )
        return geohash
    except Exception as e:
        logger.error(f"Training failed for {geohash}: {e}")
        return None

class DistributedTrainer:
    def __init__(self):
        # Connect to Ray Cluster
        ray.init(address="ray://ray-head:10001", ignore_reinit_error=True)

    def run_batch(self, data_dict: dict):
        """
        data_dict: { 'geohash': np.array([[lat, lon], ...]) }
        """
        logger.info(f"Distributing {len(data_dict)} training jobs...")
        
        # Launch async tasks
        futures = [train_single_gmm.remote(gh, data) for gh, data in data_dict.items()]
        
        # Wait for completion
        results = ray.get(futures)
        success_count = len([r for r in results if r is not None])
        
        logger.info(f"Batch Complete. Success: {success_count}/{len(data_dict)}")