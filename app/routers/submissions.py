from fastapi import APIRouter, Depends, HTTPException
from app.schemas import SubmissionCreate
from app.email import send_submission

router = APIRouter()

@router.post("", status_code=201)
def create_submission(payload: SubmissionCreate):
    # Compose a plain-text body
    lines = [
        f"New research submission:",
        f"Name: {payload.name}",
        f"Email: {payload.email}",
        f"Subject: {payload.subject}",
        "",
        "Message:",
        payload.message,
    ]
    if payload.urls:
        lines += ["", "Links:"] + [f"- {u}" for u in payload.urls]
    try:
        send_submission(
            subj=f"[Sequoia] Research submission — {payload.subject}",
            body_text="\n".join(lines),
        )
        return {"ok": True}
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
