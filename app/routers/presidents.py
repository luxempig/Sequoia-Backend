from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor
from app.db import get_connection

router = APIRouter(prefix="/api/presidents", tags=["presidents"])

@router.get("/", response_model=List[Dict[str, Any]])
def list_presidents() -> List[Dict[str, Any]]:
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM presidents ORDER BY term_start")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@router.get("/{president_slug}/voyages", response_model=List[Dict[str, Any]])
def voyages_by_president(president_slug: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT v.*
        FROM voyage_presidents vp
        JOIN voyages v ON v.voyage_slug = vp.voyage_slug
        WHERE vp.president_slug = %s
        ORDER BY v.start_date
        """,
        (president_slug,)
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail="No voyages found for this president")
    return rows
