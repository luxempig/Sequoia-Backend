from typing import Optional
from pydantic import BaseModel

# ========= Core wire models (adjust if you prefer camelCase on the wire) =========

class Voyage(BaseModel):
    voyage_slug: str
    title: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    origin: Optional[str] = None
    destination: Optional[str] = None
    vessel_name: Optional[str] = None
    voyage_type: Optional[str] = None
    summary_markdown: Optional[str] = None
    notes_internal: Optional[str] = None  # excluded from FE by policy if needed
    source_urls: Optional[str] = None
    tags: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    president_slug_from_voyage: Optional[str] = None

class Media(BaseModel):
    media_slug: str
    title: Optional[str] = None
    media_type: Optional[str] = None   # image, pdf, audio, etc
    s3_url: Optional[str] = None       # may be full s3://bucket/key or bare key
    public_derivative_url: Optional[str] = None
    credit: Optional[str] = None
    date: Optional[str] = None
    description_markdown: Optional[str] = None
    tags: Optional[str] = None
    copyright_restrictions: Optional[str] = None
    google_drive_link: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    url: Optional[str] = None          # presigned/fallback for FE

class President(BaseModel):
    president_slug: str
    full_name: Optional[str] = None
    party: Optional[str] = None
    term_start: Optional[str] = None
    term_end: Optional[str] = None
    wikipedia_url: Optional[str] = None
    tags: Optional[str] = None

class Person(BaseModel):
    person_slug: str
    full_name: Optional[str] = None
    role_title: Optional[str] = None
    organization: Optional[str] = None
    birth_year: Optional[int] = None
    death_year: Optional[int] = None
    wikipedia_url: Optional[str] = None
    notes_internal: Optional[str] = None
    tags: Optional[str] = None
