from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://vectra:password@postgis:5432/vectra_core"
    OSRM_HOST: str = "http://osrm:5000"
    REDIS_URL: str = "redis://redis:6379/0"
    
    # Heuristic Tunables
    DBSCAN_EPS_METERS: float = 20.0
    MIN_SAMPLES_CLUSTER: int = 3
    CONFIDENCE_THRESHOLD: float = 0.75
    
    # Worker Settings
    WORKER_THREADS: int = 4
    BATCH_SIZE: int = 100

settings = Settings()