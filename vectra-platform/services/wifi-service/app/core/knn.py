import numpy as np
import pandas as pd
import pygeohash as pgh
from typing import Dict, List
from cachetools import TTLCache, cachedmethod
from threading import RLock
from app.db.cassandra_client import CassandraManager
import structlog

logger = structlog.get_logger()

class WifiLocator:
    def __init__(self):
        self.session = CassandraManager.get_session()
        # Cache reference data for a geohash for 5 minutes
        self.cache = TTLCache(maxsize=1000, ttl=300)
        self.lock = RLock()

    def ingest(self, lat: float, lon: float, scan: Dict[str, int]):
        """
        Enterprise: Async Insert (Fire and Forget)
        """
        ghash = pgh.encode(lat, lon, precision=6)
        query = """
            INSERT INTO radio_map (geohash_p6, scan_id, latitude, longitude, bssids, created_at)
            VALUES (%s, uuid(), %s, %s, %s, toTimestamp(now()))
        """
        # Execute Async to not block the API thread
        future = self.session.execute_async(query, (ghash, lat, lon, scan))
        # Optional: Add callback for error logging
        future.add_errback(lambda e: logger.error("Cassandra Write Failed", error=str(e)))

    def _fetch_reference_data(self, ghash: str):
        """
        Fetches all scans in a geohash and converts to a DataFrame.
        """
        query = "SELECT latitude, longitude, bssids FROM radio_map WHERE geohash_p6 = %s"
        rows = self.session.execute(query, (ghash,))
        return list(rows)

    def _compact_fingerprints(self, raw_rows: List[dict]) -> List[dict]:
        """
        Optimization: Spatial Binning.
        Group raw scans by Fine Geohash (Precision 8 ~38m x 19m) or 
        super-fine Precision 9 (~4m x 4m) to create 'Synthetic Reference Points'.
        """
        grouped = {}
        
        for row in raw_rows:
            # Create a spatial key (Precision 8 is roughly room-sized/small shop)
            # You can also simply round Lat/Lon to 5 decimal places (~1.1m)
            spatial_key = pgh.encode(row['latitude'], row['longitude'], precision=8)
            
            if spatial_key not in grouped:
                grouped[spatial_key] = {'lat_sum': 0, 'lon_sum': 0, 'count': 0, 'signals': []}
            
            group = grouped[spatial_key]
            group['lat_sum'] += row['latitude']
            group['lon_sum'] += row['longitude']
            group['count'] += 1
            group['signals'].append(row['bssids'])

        compacted = []
        for key, data in grouped.items():
            # Average Coordinates
            avg_lat = data['lat_sum'] / data['count']
            avg_lon = data['lon_sum'] / data['count']
            
            # Average Signals (The tricky part)
            # We aggregate all RSSIs seen in this grid cell
            agg_signals = {}
            mac_counts = {}
            
            for scan in data['signals']:
                for mac, rssi in scan.items():
                    agg_signals[mac] = agg_signals.get(mac, 0) + rssi
                    mac_counts[mac] = mac_counts.get(mac, 0) + 1
            
            # Calculate mean RSSI per MAC
            mean_signals = {mac: int(val / mac_counts[mac]) for mac, val in agg_signals.items()}
            
            compacted.append({
                'latitude': avg_lat,
                'longitude': avg_lon,
                'bssids': mean_signals
            })
            
        return compacted
    
    @cachedmethod(lambda self: self.cache, lock=lambda self: self.lock)
    def locate(self, coarse_lat: float, coarse_lon: float, target_scan: Dict[str, int], k=5) -> Dict:
        """
        Vectorized KNN Lookup.
        """
        ghash = pgh.encode(coarse_lat, coarse_lon, precision=6)
        
        # 1. Get Reference Data (Cached)
        raw_references = self._fetch_reference_data(ghash)
        if not references:
            return None
        references = self._compact_fingerprints(raw_references)
        # 2. Vectorization Preparation
        # Create a list of reference RSSI dicts and coordinates
        ref_bssids_list = [r['bssids'] for r in references]
        ref_coords = np.array([[r['latitude'], r['longitude']] for r in references])
        
        # Identify common MAC addresses (Features)
        # We only care about MACs seen in the Target Scan to reduce dimensionality
        target_macs = list(target_scan.keys())
        target_vector = np.array([target_scan[mac] for mac in target_macs])
        
         # 3. Calculate Euclidean Distance
        diff = matrix - target_vector
        sq_dist = np.sum(diff ** 2, axis=1)
        euclidean_dist = np.sqrt(sq_dist)
        
        # OPTIMIZATION: Jaccard Penalty
        # We want to penalize references that don't share MACs with the target
        penalized_distances = []
        
        target_macs_set = set(target_macs)
        
        for i, ref_row in enumerate(references):
            ref_macs_set = set(ref_row['bssids'].keys())
            
            # Jaccard Index = Intersection / Union
            intersection = len(target_macs_set.intersection(ref_macs_set))
            union = len(target_macs_set.union(ref_macs_set))
            
            jaccard_index = intersection / union if union > 0 else 0
            
            # Invert index: 1.0 is perfect overlap, 0.0 is no overlap
            # Penalty Factor: Small overlap = Big Penalty
            penalty = 1.0 + (1.0 - jaccard_index) * 2.0 
            
            # Apply penalty to geometric distance
            penalized_distances.append(euclidean_dist[i] * penalty)

        final_distances = np.array(penalized_distances)

        
        # 4. Find Top K
        # argpartition is faster than sort for top-k
        if len(final_distances) < k:
            k = len(final_distances)
            
        idx = np.argpartition(final_distances, k)[:k]
        nearest_indices = idx[np.argsort(final_distances[idx])]
        
        # 5. Weighted Average
        weights = 1.0 / (final_distances[nearest_indices] + 1e-6)
        total_weight = np.sum(weights)
        
        weighted_lat = np.sum(ref_coords[nearest_indices, 0] * weights) / total_weight
        weighted_lon = np.sum(ref_coords[nearest_indices, 1] * weights) / total_weight
        
        uncertainty = final_distances[nearest_indices[0]] # Distance to nearest neighbor
        
        return {
            "lat": weighted_lat,
            "lon": weighted_lon,
            "uncertainty_m": float(uncertainty),
            "sample_size": len(references)
        }