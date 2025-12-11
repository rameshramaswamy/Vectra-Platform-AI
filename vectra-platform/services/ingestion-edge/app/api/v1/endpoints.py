from fastapi import APIRouter, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
from app.kafka.producer import send_trace
from app.core.config import settings
from fastapi import APIRouter, HTTPException, Request, BackgroundTasks, Header
from app.kafka.producer import send_trace
from services.common.python import telemetry_pb2 # Generated file
from google.protobuf.json_format import MessageToDict

router = APIRouter()

class TracePayload(BaseModel):
    driver_id: str
    vehicle_id: str
    latitude: float
    longitude: float
    speed_mps: float
    timestamp_ms: int
    event_type: str = "PING"
    accuracy_m: float = 0.0

@router.post("/telemetry")
async def ingest_telemetry(
    payload: TracePayload, 
    background_tasks: BackgroundTasks,
    x_api_key: Optional[str] = Header(None)
):
    if x_api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    
    # Push to Kafka in background to minimize latency for mobile client
    background_tasks.add_task(send_trace, payload.dict())
    
    return {"status": "accepted"}

@router.post("/telemetry/proto")
async def ingest_telemetry_proto(
    request: Request,
    background_tasks: BackgroundTasks,
    x_api_key: str = Header(None)
):
    """
    High-Performance Endpoint: Accepts Binary Protobuf
    """
    if x_api_key != settings.API_KEY:
        raise HTTPException(status_code=401)

    try:
        # Read raw bytes
        body = await request.body()
        
        # Parse Protobuf
        trace = telemetry_pb2.GpsTrace()
        trace.ParseFromString(body)
        
        # Convert to Dict for JSON serialization into Kafka
        # (Optimally Kafka should store Proto bytes, but for Phase 1 JSON is easier to debug)
        data = MessageToDict(trace, preserving_proto_field_name=True)
        
        background_tasks.add_task(send_trace, data)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid Protobuf")

    return {"status": "ok"}