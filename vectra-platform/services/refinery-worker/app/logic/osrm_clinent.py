import requests
from shapely.geometry import Point
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import structlog

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = structlog.get_logger()

class OSRMMatcher:
    _session = None

    def __init__(self, base_url: str):
        self.base_url = base_url
        # Optimization: Singleton Session per Process
        if OSRMMatcher._session is None:
            OSRMMatcher._session = self._create_session()

    def _create_session(self):
        """
        Create a robust session with connection pooling and retries.
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # Keep 10 connections open
            pool_maxsize=10
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session


    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def snap_to_road(self, point: Point, bearing: float = None) -> Point:
        """
        Snaps point to nearest road. Retries on network failure.
        """
        session = OSRMMatcher._session
        try:
            # OSRM expects {lon},{lat}
            url = f"{self.base_url}/nearest/v1/driving/{point.x},{point.y}"

            params = {}
            if bearing is not None and not pd.isna(bearing):
                # OSRM format: "value,range". 
                # We allow a deviation of ±20 degrees.
                params['bearings'] = f"{int(bearing)},20"

            resp = session.get(url, params=params, timeout=1.0)
            
            # If strict bearing fails (e.g. GPS bearing noise), retry without it
            if resp.status_code != 200 and bearing is not None:
                    resp = session.get(url, timeout=1.0) # Fallback

            resp.raise_for_status()
            
            data = resp.json()
            if data['code'] == 'Ok' and data['waypoints']:
                snapped = data['waypoints'][0]['location']
                return Point(snapped[0], snapped[1])
        except Exception:
            pass # Fallback to original point handled by caller
        
        return point