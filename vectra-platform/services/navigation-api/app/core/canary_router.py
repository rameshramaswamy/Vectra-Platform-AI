import mmh3
import requests
import pybreaker
import structlog
from app.core.config import settings

logger = structlog.get_logger()

# Enterprise Circuit Breaker
# If AI fails 5 times in a row, stop calling it for 60 seconds.
ai_breaker = pybreaker.CircuitBreaker(
    fail_max=5, 
    reset_timeout=60,
    listeners=[pybreaker.CircuitBreakerListener()] # Log state changes in prod
)

class CanaryRouter:
    def __init__(self):
        self.ai_service_url = settings.AI_INFERENCE_URL
        self.rollout_percent = 5 # 5% of traffic

    def should_route_to_ai(self, identifier: str) -> bool:
        """
        Deterministic Sticky Routing.
        Hash(ID) % 100 < Threshold
        """
        # mmh3 is fast and distributes uniformly
        hash_val = mmh3.hash(identifier, seed=42)
        normalized = abs(hash_val) % 100
        return normalized < self.rollout_percent

    def resolve(self, geohash: str, db_result: dict) -> dict:
        # 1. Check Rollout Eligibility
        if not self.should_route_to_ai(geohash):
            return db_result

        # 2. Call AI Service (Protected by Circuit Breaker)
        try:
            return self._call_ai_service(geohash, db_result)
        except pybreaker.CircuitBreakerError:
            logger.warning("Canary: AI Circuit Open, skipping", geohash=geohash)
            return db_result
        except Exception as e:
            logger.error("Canary: AI Call Failed", error=str(e))
            return db_result

    @ai_breaker
    def _call_ai_service(self, geohash: str, db_result: dict):
        resp = requests.post(
            f"{self.ai_service_url}/api/v1/predict/entry-point",
            json={"geohash": geohash},
            timeout=0.3 # Strict timeout (300ms)
        )
        
        if resp.status_code == 200:
            ai_data = resp.json()
            # Validation: Ensure AI actually returned points
            if ai_data.get('entry_points'):
                best_ep = max(ai_data['entry_points'], key=lambda x: x['probability'])
                
                return {
                    "address_id": geohash,
                    "navigation_point": db_result['navigation_point'],
                    "entry_point": {"lat": best_ep['lat'], "lon": best_ep['lon']},
                    "source": f"canary_ai_{best_ep['type']}",
                    "confidence": best_ep['probability']
                }
        
        return db_result