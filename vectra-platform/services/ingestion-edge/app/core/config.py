from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_TRACES: str = "vectra-raw-gps"
    API_KEY: str = "secret-key-for-phase1"

    class Config:
        env_file = ".env"

settings = Settings()