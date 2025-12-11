import numpy as np
import mlflow
import mlflow.sklearn
from sklearn.mixture import GaussianMixture
from app.core.config import settings

class EntryPointGMM:
    def __init__(self, n_components=2):
        self.model = GaussianMixture(
            n_components=n_components, 
            covariance_type='full',
            random_state=42
        )

    def train(self, coords: np.ndarray, geohash: str):
        """
        coords: shape (N, 2) -> [[lat, lon], ...]
        """
        if len(coords) < 5:
            return None # Not enough data
            
        self.model.fit(coords)
        
        # Log to MLflow
        with mlflow.start_run(run_name=f"gmm_{geohash}"):
            mlflow.log_param("n_components", self.model.n_components)
            mlflow.log_metric("aic", self.model.aic(coords))
            
            # Save model with a unique tag for the geohash
            mlflow.sklearn.log_model(
                self.model, 
                artifact_path="model",
                registered_model_name=f"gmm_entry_{geohash}"
            )
            
        return self.model