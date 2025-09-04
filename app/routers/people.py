from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query
from psycopg2.extras import RealDictCursor
from app.db import get_connection

router = APIRouter(prefix="/api/people", tags=["people"])

@router.get("/", response_model=List[Dict[str, Any]])
def list_people(
    q: Optional[str] = Query(None, description="Search people by full_name ILIKE"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    if q:
        cur.execute("SELECT * FROM people WHERE full_name ILIKE %s ORDER BY full_name LIMIT %s OFFSET %s", (f"%{q}%", limit, offset))
    else:
        cur.execute("SELECT * FROM people ORDER BY full_name LIMIT %s OFFSET %s", (limit, offset))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@router.get("/by-voyage/{voyage_slug}", response_model=List[Dict[str, Any]])
def people_for_voyage(voyage_slug: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT p.*, vp.capacity_role, vp.notes AS voyage_notes
        FROM voyage_passengers vp
        JOIN people p ON p.person_slug = vp.person_slug
        WHERE vp.voyage_slug = %s
        ORDER BY p.full_name
        """,
        (voyage_slug,)
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows
