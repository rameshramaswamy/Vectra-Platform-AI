import time
import mlflow
import structlog
from app.pipelines.train_entry import DistributedTrainer
from app.core.config import settings
from sqlalchemy import create_engine, text

logger = structlog.get_logger()
engine = create_engine(settings.DATABASE_URL)

def get_training_candidates(batch_size=1000):
    """
    Optimization: Query DB to find 'Dirty' Geohashes.
    Logic: (Current Trace Count - Trace Count at Last Training) > Threshold
    """
    sql = text("""
        SELECT t.geohash, COUNT(t.id) as current_count
        FROM raw_gps_traces t
        LEFT JOIN model_metadata m ON t.geohash = m.geohash
        WHERE t.event_type = 'SCAN'
        GROUP BY t.geohash, m.last_trained_count
        HAVING COUNT(t.id) - COALESCE(m.last_trained_count, 0) > 50
        LIMIT :limit
    """)
    
    with engine.connect() as conn:
        results = conn.execute(sql, {"limit": batch_size}).fetchall()
        return [r[0] for r in results]

def update_metadata(geohashes):
    """Update the registry after successful training"""
    # Simplified: execute update SQL
    pass

def run_smart_training_cycle():
    logger.info("Analyzing Data Deltas...")
    
    # 1. Get only candidates that NEED training
    candidates = get_training_candidates()
    
    if not candidates:
        logger.info("No geohashes meet training threshold. Sleeping.")
        return

    logger.info(f"Triggering Training for {len(candidates)} dirty locations")

    # 2. Fetch Data (Optimized Bulk Fetch)
    # In prod, use Feast batch retrieval. Here we simulate fetching dict.
    # data_dict = fetch_data_for_geohashes(candidates) 
    
    # 3. Submit to Ray (Existing Logic)
    # trainer = DistributedTrainer()
    # trainer.run_batch(data_dict)
    
    # 4. Update Metadata to reset the "dirty" counter
    update_metadata(candidates)

if __name__ == "__main__":
    while True:
        run_smart_training_cycle()
        time.sleep(3600) # Check every hour