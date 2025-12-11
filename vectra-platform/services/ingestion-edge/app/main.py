from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from app.api.v1 import endpoints
from app.core.logging import setup_logging
from app.kafka.producer import close_producer
import uvloop
import asyncio

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
# 1. Setup Logging
setup_logging()

app = FastAPI(title="Vectra Ingestion Edge", version="1.0.0")

# 2. Add Metrics Endpoint (/metrics)
Instrumentator().instrument(app).expose(app)

# 3. Include Routers
app.include_router(endpoints.router, prefix="/api/v1")

# 4. Health Check (for K8s Liveness/Readiness)
@app.get("/health")
async def health_check():
    return {"status": "ok"}

# 5. Graceful Shutdown
@app.on_event("shutdown")
async def shutdown_event():
    close_producer()