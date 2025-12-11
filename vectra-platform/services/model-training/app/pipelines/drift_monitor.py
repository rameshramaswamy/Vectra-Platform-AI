import numpy as np
import pandas as pd
from scipy.spatial.distance import jensenshannon
from app.core.config import settings
import structlog

logger = structlog.get_logger()

def check_drift(geohash, old_means, new_data_points):
    """
    Simple drift check: Is the centroid of new data significantly far 
    from the trained model's means?
    """
    if len(new_data_points) < 10:
        return False
        
    # Simplified logic: Distance check
    new_centroid = new_data_points.mean(axis=0)
    
    # Distance to nearest existing cluster center
    distances = [np.linalg.norm(new_centroid - m) for m in old_means]
    min_dist = min(distances)
    
    # If new data center is > 20 meters from known entrance, trigger retrain
    is_drift = min_dist > 0.0002 # approx 20m
    
    if is_drift:
        logger.warning("Data Drift Detected", geohash=geohash, deviation=min_dist)
        
    return is_drift