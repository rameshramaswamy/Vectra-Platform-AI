from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from app.api.v1 import endpoints
from app.core.config import settings

app = FastAPI(title="Vectra Navigation API", version="1.0.0")

# 1. Metrics
Instrumentator().instrument(app).expose(app)

# 2. Routes
app.include_router(endpoints.router, prefix="/api/v1")

@app.get("/health")
def health():
    return {"status": "ok", "env": settings.API_ENV}