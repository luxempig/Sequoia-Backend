from functools import lru_cache
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=True)

    APP_TITLE: str = "Sequoia API"

    # Postgres
    DB_HOST: str
    DB_PORT: int = 5432
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    # AWS / S3
    AWS_REGION: str = "us-east-2"
    # Optional override: if your media.s3_url is a bare key (not s3://...), we’ll use this bucket
    MEDIA_BUCKET: str = ""     
    PRESIGNED_TTL: int = 3600  # seconds

    # CORS
    CORS_ORIGINS: List[AnyHttpUrl] = [
        "http://localhost:3000",
    ]

@lru_cache
def get_settings() -> "Settings":
    return Settings()  # type: ignore
