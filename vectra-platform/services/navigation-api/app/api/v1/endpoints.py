import json
import redis
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.core.config import settings
from app.core.database import get_db
from app.schemas.io import LocationResponse, FeedbackRequest # Assuming schemas exist
from app.db.models import LocationFeedback
from app.core.canary_router import CanaryRouter
routerc = CanaryRouter()
router = APIRouter()
redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

@router.get("/resolve/{address_id}", response_model=LocationResponse)
def resolve_location(address_id: str, db: Session = Depends(get_db)):
    """
    Enterprise Resolver: Redis Cache -> DB -> 404
    """
    cache_key = f"loc:{address_id}"
    
    # 1. Check Cache
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # 2. Query DB
    sql = text("""
        SELECT 
            id, 
            ST_X(nav_point) as np_lon, ST_Y(nav_point) as np_lat,
            ST_X(entry_point) as ep_lon, ST_Y(entry_point) as ep_lat
        FROM refined_locations 
        WHERE id = :id
    """)
    result = db.execute(sql, {"id": address_id}).fetchone()
    
    if not result:
        raise HTTPException(status_code=404, detail="Location not resolved yet")

    response_data = {
        "address_id": result.id,
        "navigation_point": {"lat": result.np_lat, "lon": result.np_lon},
        "entry_point": {"lat": result.ep_lat, "lon": result.ep_lon},
        "source": "heuristic_v1_db"
    }

    # 3. Set Cache (Async via Redis pipeline if heavy, but simple set is fast)
    redis_client.setex(cache_key, settings.CACHE_TTL_SECONDS, json.dumps(response_data))
    final_result = routerc.resolve(address_id, response_data)   
    return final_result

@router.post("/feedback")
def submit_feedback(
    feedback: FeedbackRequest, 
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Async feedback submission to avoid blocking the driver app.
    """
    background_tasks.add_task(save_feedback_task, feedback, db)
    return {"status": "queued"}

def save_feedback_task(feedback, db_session):
    # Separate function for background task
    try:
        # Create new session if needed or use passed scoped session carefully
        # For BG tasks better to create fresh session factory logic usually
        # Simplified here:
        record = LocationFeedback(
            location_id=feedback.address_id,
            driver_id=feedback.driver_id,
            is_nav_point_accurate=feedback.is_np_ok,
            is_entry_point_accurate=feedback.is_ep_ok,
            corrected_lat=feedback.corrected_lat,
            corrected_lon=feedback.corrected_lon,
            comment=feedback.comment
        )
        db_session.add(record)
        db_session.commit()
    except Exception as e:
        print(f"Feedback Save Error: {e}")
    finally:
        db_session.close()