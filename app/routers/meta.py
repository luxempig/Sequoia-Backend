from fastapi import APIRouter
from datetime import datetime
from app.config import get_settings

router = APIRouter(prefix="/api", tags=["meta"])

@router.get("/health")
def health():
    s = get_settings()
    return {
        "status": "ok",
        "time": datetime.utcnow().isoformat() + "Z",
        "bucket_env": bool(s.MEDIA_BUCKET),
        "region": s.AWS_REGION,
        "presigned_ttl": s.PRESIGNED_TTL,
    }
