import redis
import json
import structlog
from sqlalchemy import create_engine, text
from app.core.config import settings

logger = structlog.get_logger()

class CacheWarmer:
    def __init__(self):
        self.redis = redis.from_url(settings.REDIS_URL)
        self.db_engine = create_engine(settings.DATABASE_URL)

    def run(self):
        logger.info("Starting Daily Cache Warmer...")
        
        # 1. Identify "Hot" Addresses (Top 80% visited in last 30 days)
        # Using Phase 2's refined_locations as source of truth for now
        sql = text("""
            SELECT id, ST_X(nav_point), ST_Y(nav_point), ST_X(entry_point), ST_Y(entry_point)
            FROM refined_locations
            WHERE updated_at > NOW() - INTERVAL '30 days'
        """)
        
        with self.db_engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
            
        logger.info(f"Pre-computing {len(rows)} locations...")
        
        # 2. Pipeline write to Redis
        pipe = self.redis.pipeline()
        count = 0
        
        for row in rows:
            # Format: Same as API response
            data = {
                "address_id": row[0],
                "navigation_point": {"lat": row[2], "lon": row[1]},
                "entry_point": {"lat": row[4], "lon": row[3]},
                "source": "cache_precomputed"
            }
            
            # Key: loc:{geohash}
            pipe.set(f"loc:{row[0]}", json.dumps(data), ex=86400 * 2) # 48hr TTL
            
            count += 1
            if count % 1000 == 0:
                pipe.execute()
                pipe = self.redis.pipeline()
                
        pipe.execute()
        logger.info("Cache Warming Complete.")

if __name__ == "__main__":
    warmer = CacheWarmer()
    warmer.run()