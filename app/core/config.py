import os
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    APP_NAME: str = "Distributed Job Platform"
    APP_ENV: str = "development"
    DEBUG: bool = True
    PORT: int = 8000
    HOST: str = "0.0.0.0"

    JWT_SECRET_KEY: str = "supersecretchangeitinproduction"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/job_platform"
    REDIS_URL: str = "redis://localhost:6379/0"

    WORKER_CONCURRENCY: int = 5
    WORKER_HEARTBEAT_INTERVAL: int = 5
    WORKER_LEASE_DURATION: int = 15
    WORKER_REAPER_INTERVAL: int = 10

    RATE_LIMIT_LIMIT: int = 100
    RATE_LIMIT_WINDOW: int = 60

settings = Settings()
