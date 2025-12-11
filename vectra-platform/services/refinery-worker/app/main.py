import time
import json
import pandas as pd
import structlog
from concurrent.futures import ProcessPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from app.core.config import settings
from app.logic.clustering import LocationHeuristics
from app.logic.osrm_client import OSRMMatcher
import redis
from contextlib import contextmanager
import pygeohash as pgh
# Structured Logging
structlog.configure(processors=[structlog.processors.JSONRenderer()])
logger = structlog.get_logger()

# Global engine for the main process
engine = create_engine(settings.DATABASE_URL, pool_size=10, max_overflow=20)


# Initialize Redis (Thread-safe connection pool)
redis_pool = redis.ConnectionPool.from_url(settings.REDIS_URL)


redis_client = redis.from_url(settings.REDIS_URL)
def update_hot_cache(geohash: str, np_point, ep_point):
    """
    Optimization: Push fresh data to Redis immediately.
    Eliminates the 'stale data' window of the nightly batch job.
    """
    try:
        data = {
            "address_id": geohash,
            "navigation_point": {"lat": np_point.y, "lon": np_point.x},
            "entry_point": {"lat": ep_point.y, "lon": ep_point.x},
            "source": "live_refinery"
        }
        # Set with 48h TTL
        redis_client.set(f"loc:{geohash}", json.dumps(data), ex=172800)
        logger.info("Cache Invalidated/Updated", geohash=geohash)
    except Exception as e:
        logger.error("Cache Update Failed", error=str(e))

@contextmanager
def redis_lock(lock_name, expire=60):
    r = redis.Redis(connection_pool=redis_pool)
    # NX=True (Only set if not exists), EX=expire (Auto-expire lock)
    have_lock = r.set(lock_name, "locked", nx=True, ex=expire)
    try:
        yield have_lock
    finally:
        if have_lock:
            r.delete(lock_name)

def process_single_geohash(ghash: str):
    """
    Isolated function for parallel execution.
    Re-initializes connections to be process-safe.
    """
    # Optimization 2: Distributed Lock
    lock_key = f"lock:refine:{ghash}"
    
    with redis_lock(lock_key, expire=30) as acquired:
        if not acquired:
            # Another worker is processing this. Skip.
            return None

        # Each process needs its own connection/matcher
        local_engine = create_engine(settings.DATABASE_URL)
        matcher = OSRMMatcher(settings.OSRM_HOST)
        heuristics = LocationHeuristics(
            eps_meters=settings.DBSCAN_EPS_METERS,
            min_samples=settings.MIN_SAMPLES_CLUSTER
        )
        neighbors = pgh.neighbors(ghash) # Returns list of 8 strings
        search_hashes = [ghash] + neighbors
        search_hashes_str = "'" + "','".join(search_hashes) + "'"
        result = None
        try:
            with local_engine.connect() as conn:
                # 1. Fetch SCANs (Only from CENTER geohash - the address is here)
                df_scans = pd.read_sql(
                    text(f"SELECT * FROM raw_gps_traces WHERE geohash = '{ghash}' AND event_type = 'SCAN'"), 
                    conn
                )
                
                ep_point, ep_conf = heuristics.find_entry_point(df_scans)
                if not ep_point: return None

                # 2. Fetch TRACES (From CENTER + NEIGHBORS - parking could be anywhere near)
                df_traces = pd.read_sql(
                    text(f"SELECT * FROM raw_gps_traces WHERE geohash IN ({search_hashes_str})"), 
                    conn
                )
                
                # Logic continues...
                raw_np, avg_bearing = heuristics.find_parking_candidate(df_traces, ep_point)
                
                # Pass bearing to OSRM
                final_np = matcher.snap_to_road(raw_np, bearing=avg_bearing) if raw_np else ep_point
                final_conf = ep_conf * 0.9 
                # 4. Prepare Result (Don't write in subprocess, return to main)
                result = {
                    "id": ghash,
                    "np_wkt": final_np.wkt,
                    "ep_wkt": ep_point.wkt,
                    "conf": final_conf
                }
        except Exception as e:
            logger.error("Processing Failed", geohash=ghash, error=str(e))
        finally:
            local_engine.dispose()
            
        return result

def run_batch_job():
    while True:
        with engine.connect() as conn:
            # 1. Find "Dirty" Geohashes (New scans received recently)
            # This logic finds geohashes where new raw data exists that hasn't been refined yet
            # (Simplified for Phase 2: Just take top N unprocessed/outdated)
            query = text(f"""
                SELECT t.geohash 
                FROM raw_gps_traces t
                LEFT JOIN refined_locations r ON t.geohash = r.id
                WHERE t.event_type = 'SCAN'
                AND (r.updated_at IS NULL OR t.timestamp > r.updated_at)
                GROUP BY t.geohash
                LIMIT {settings.BATCH_SIZE}
            """)
            candidates = [r[0] for r in conn.execute(query).fetchall()]

        if not candidates:
            logger.info("No dirty records found. Sleeping...")
            time.sleep(60)
            continue

        logger.info("Starting Batch", size=len(candidates))

        # 2. Parallel Execution
        updates = []
        with ProcessPoolExecutor(max_workers=settings.WORKER_THREADS) as executor:
            futures = {executor.submit(process_single_geohash, g): g for g in candidates}
            
            for future in as_completed(futures):
                res = future.result()
                if res:
                    updates.append(res)

        # 3. Bulk Write (Main Process)
        if updates:
            with engine.begin() as conn: # Transaction
                for up in updates:
                    sql = text("""
                        INSERT INTO refined_locations (id, nav_point, entry_point, confidence_score, updated_at)
                        VALUES (:id, ST_GeomFromText(:np_wkt), ST_GeomFromText(:ep_wkt), :conf, NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            nav_point = EXCLUDED.nav_point,
                            entry_point = EXCLUDED.entry_point,
                            updated_at = NOW()
                    """)
                    conn.execute(sql, up)

            for item in updates:
                # We need to parse WKT back to objects or store raw points in item dict
                # Assuming item has 'np_wkt' and 'ep_wkt'
                from shapely import wkt
                np_obj = wkt.loads(item['np_wkt'])
                ep_obj = wkt.loads(item['ep_wkt'])
                
                update_hot_cache(item['id'], np_obj, ep_obj)

            logger.info("Batch Complete", updated_count=len(updates))

if __name__ == "__main__":
    logger.info("Refinery Worker Started (Enterprise Mode)")
    run_batch_job()