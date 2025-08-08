
from functools import lru_cache
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=True)
    APP_TITLE: str = "Sequoia API"
    DB_HOST: str
    DB_PORT: int = 5432
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    AWS_REGION: str = "us-east-2"
    MEDIA_BUCKET: str = ""   # 'uss-sequoia-bucket' if you want presigned URLs
    PRESIGNED_TTL: int = 3600

    P_PATH: Optional[str] = None
    V_PATH: Optional[str] = None
    PV_PATH: Optional[str] = None
    S_PATH: Optional[str] = None
    SV_PATH: Optional[str] = None
    CORS_ORIGINS: List[AnyHttpUrl] = [
        "http://localhost:3000",
        "http://ec2-18-191-216-71.us-east-2.compute.amazonaws.com",
    ]
def get_settings() -> Settings:
    return Settings()  # type: ignore
