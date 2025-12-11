from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://vectra:password@postgis:5432/vectra_core"
    REDIS_URL: str = "redis://redis:6379/0"
    API_ENV: str = "production"
    CACHE_TTL_SECONDS: int = 3600 # 1 hour cache

settings = Settings()