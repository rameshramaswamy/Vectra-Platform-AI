import requests
from shapely.geometry import Point
import structlog

logger = structlog.get_logger()

class OSRMMatcher:
    def __init__(self, osrm_host: str = "http://osrm:5000"):
        self.base_url = osrm_host

    def snap_to_road(self, point: Point) -> Point:
        """
        Queries OSRM Nearest API to find the closest drivable edge.
        """
        try:
            # OSRM expects {lon},{lat}
            url = f"{self.base_url}/nearest/v1/driving/{point.x},{point.y}"
            response = requests.get(url, timeout=2)
            
            if response.status_code == 200:
                data = response.json()
                if data['code'] == 'Ok' and data['waypoints']:
                    # OSRM returns [lon, lat]
                    snapped = data['waypoints'][0]['location']
                    return Point(snapped[0], snapped[1])
        except Exception as e:
            logger.warning("OSRM Match Failed", error=str(e))
        
        # Fallback: Return original point if snapping fails
        return point