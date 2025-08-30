
from typing import Optional, List
from pydantic import BaseModel, EmailStr, Field

class Passenger(BaseModel):
    passenger_id: int
    name: str
    bio_path: Optional[str] = None
    basic_info: Optional[str] = None

class Source(BaseModel):
    source_id: int
    source_type: str
    source_origin: Optional[str] = None
    source_description: Optional[str] = None
    source_path: str
    page_num: Optional[int] = None
    url: Optional[str] = None  # presigned URL

class Voyage(BaseModel):
    voyage_id: int
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]
    additional_info: Optional[str] = None
    notes: Optional[str] = None
    significant: Optional[int] = None
    royalty: Optional[int] = None
    president_id: Optional[int] = None
    president_name: Optional[str] = None

class SubmissionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    email: EmailStr
    subject: str = Field(min_length=1, max_length=200)
    message: str = Field(min_length=1, max_length=10000)
    urls: Optional[List[str]] = None
