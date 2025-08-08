
from fastapi import APIRouter
from app.config import get_settings
from datetime import datetime

router = APIRouter(prefix="/api", tags=["meta"])

@router.get("/health")
def health():
    s = get_settings()
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z", "bucket": bool(s.MEDIA_BUCKET)}
