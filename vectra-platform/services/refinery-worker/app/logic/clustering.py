import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from shapely.geometry import Point

class LocationHeuristics:
    def __init__(self, eps_meters=20, min_samples=3):
        # Earth Radius in meters
        self.kms_per_radian = 6371.0088
        # Convert eps from meters to radians for Haversine
        self.eps_radians = (eps_meters / 1000.0) / self.kms_per_radian
        self.min_samples = min_samples

    def calculate_confidence(self, cluster_df: pd.DataFrame, std_dev_meters: float) -> float:
        """
        Formula: 
        Base Score (Sample Size) * Penalty (Variance)
        - Needs at least 5 points for high confidence.
        - Variance > 30m reduces confidence drastically.
        """
        n_samples = len(cluster_df)
        
        # Sigmoid-like growth: 1 sample = 0.2, 5 samples = 0.8, 10+ = 1.0
        size_score = 1 - (1 / (0.2 * n_samples + 1))
        
        # Variance Penalty: if std_dev is 0m (perfect), penalty is 1.0 (no penalty).
        # If std_dev is 50m, penalty is 0.5.
        variance_penalty = 1 / (1 + (std_dev_meters / 30.0))
        
        return round(size_score * variance_penalty, 2)

    def find_entry_point(self, scan_events: pd.DataFrame) -> Point:
        """
        Optimized: Uses Haversine metric for true distance clustering.
        """
        if len(scan_events) == 0:
            return None
            
        # Convert Lat/Lon to Radians for Scikit-Learn
        coords_rad = np.radians(scan_events[['latitude', 'longitude']].values)
        
        # Optimization: metric='haversine' is O(n^2) but accurate for earth distances
        db = DBSCAN(
            eps=self.eps_radians, 
            min_samples=self.min_samples, 
            metric='haversine', 
            algorithm='ball_tree'
        ).fit(coords_rad)
        
        labels = db.labels_
        unique_labels, counts = np.unique(labels, return_counts=True)
        
        # Remove noise (-1)
        if -1 in unique_labels:
            noise_idx = np.where(unique_labels == -1)
            unique_labels = np.delete(unique_labels, noise_idx)
            counts = np.delete(counts, noise_idx)
            
        if len(unique_labels) == 0:
            # Fallback: Weighted Mean of all points based on accuracy
            return self._weighted_centroid(scan_events)
            
        dominant_label = unique_labels[np.argmax(counts)]
        
        # Get points in cluster (convert back to degrees later)
        cluster_mask = (labels == dominant_label)
        cluster_data = scan_events[cluster_mask]
        

        # Calculate Standard Deviation of the cluster in meters (approx)
        # 1 deg lat approx 111km -> 111,000m
        lat_std = np.std(cluster_data['latitude']) * 111000
        lon_std = np.std(cluster_data['longitude']) * 111000 * np.cos(np.radians(cluster_data['latitude'].mean()))
        geo_std_dev = np.sqrt(lat_std**2 + lon_std**2)
        
        final_pt = self._weighted_centroid(cluster_data)
        
        # Return Tuple: (Point, Confidence)
        conf = self.calculate_confidence(cluster_data, geo_std_dev)
        return final_pt, conf

    def _weighted_centroid(self, df: pd.DataFrame) -> Point:
        """
        Optimization: Trust points with better GPS accuracy (lower 'accuracy_m')
        and recent timestamps more.
        """
        # Inverse variance weighting (1 / accuracy^2)
        # Avoid division by zero with small epsilon
        weights = 1 / (df['accuracy_m']**2 + 1e-6)
        
        # Time Decay: Newer points get higher weight (e.g., last 30 days)
        # Simple linear decay factor could be added here
        
        lat = np.average(df['latitude'], weights=weights)
        lon = np.average(df['longitude'], weights=weights)
        
        return Point(lon, lat)

    def find_parking_candidate(self, trace_history: pd.DataFrame, entry_point: Point) -> Point:
        # (Keep existing logic but apply weighted centroid if multiple candidates exist)
        parking_candidates = trace_history[
            (trace_history['event_type'].isin(['STOP', 'ARRIVED'])) |
            (trace_history['speed'] < 1.0)
        ]
        
        if len(parking_candidates) == 0:
            return None

        # Optimization: Spatial Filter
        # Only consider parking spots within 100m of the computed Entry Point
        # (Simple box filter first for speed)
        ep_lat, ep_lon = entry_point.y, entry_point.x
        mask = (
            (parking_candidates['latitude'].between(ep_lat - 0.001, ep_lat + 0.001)) &
            (parking_candidates['longitude'].between(ep_lon - 0.001, ep_lon + 0.001))
        )
        nearby_parking = parking_candidates[mask]
        
        if len(nearby_parking) == 0:
            return None
            
        return self._weighted_centroid(nearby_parking)