import random
import requests
import structlog
from app.core.config import settings

logger = structlog.get_logger()

class CanaryRouter:
    """
    Decides whether to use Heuristic DB or AI Inference based on rollout percentage.
    """
    def __init__(self):
        self.ai_service_url = settings.AI_INFERENCE_URL # http://inference-service:8000
        self.rollout_percent = 5 # 5% traffic to AI

    def resolve(self, geohash: str, db_result: dict) -> dict:
        # 1. Deterministic hashing for sticky sessions (optional), or random
        # Here we use random for A/B testing distribution
        if random.randint(1, 100) <= self.rollout_percent:
            try:
                # 2. Call AI Service
                resp = requests.post(
                    f"{self.ai_service_url}/api/v1/predict/entry-point",
                    json={"geohash": geohash},
                    timeout=0.5
                )
                
                if resp.status_code == 200:
                    ai_data = resp.json()
                    # Check if AI returned valid data
                    if ai_data.get('entry_points'):
                        logger.info("Canary: Serving AI Result", geohash=geohash)
                        return self._format_ai_response(geohash, ai_data, db_result)
            except Exception as e:
                logger.error("Canary: AI Service Failed, falling back", error=str(e))
        
        # 3. Fallback / Standard: Heuristic DB Result
        return db_result

    def _format_ai_response(self, geohash, ai_data, db_result):
        # AI returns multiple EP probabilities. We pick the highest prob.
        # We assume NP comes from DB (Hybrid approach) or GNN if integrated.
        best_ep = max(ai_data['entry_points'], key=lambda x: x['probability'])
        
        return {
            "address_id": geohash,
            "navigation_point": db_result['navigation_point'], # Hybrid: NP from Heuristic
            "entry_point": {"lat": best_ep['lat'], "lon": best_ep['lon']}, # EP from AI
            "source": f"canary_ai_{best_ep['type']}"
        }