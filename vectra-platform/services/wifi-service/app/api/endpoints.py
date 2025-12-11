from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict
from app.core.knn import WifiLocator

router = APIRouter()
locator = WifiLocator()

class WifiPayload(BaseModel):
    # If training/ingesting, we need GPS. If locating, we might have rough GPS.
    latitude: float
    longitude: float
    wifi_scan: Dict[str, int] # {"aa:bb:cc:dd": -50}

@router.post("/ingest")
def ingest_signal(payload: WifiPayload):
    locator.ingest(payload.latitude, payload.longitude, payload.wifi_scan)
    return {"status": "indexed"}

@router.post("/locate")
def locate_device(payload: WifiPayload):
    result = locator.locate(payload.latitude, payload.longitude, payload.wifi_scan)
    if result:
        return {"source": "wifi_knn", "location": result}
    return {"source": "failure", "detail": "not_enough_data"}