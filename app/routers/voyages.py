from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Request
from psycopg2.extras import RealDictCursor
from app.db import get_connection

router = APIRouter(tags=["voyages"])

@router.get("/", response_model=List[Dict[str, Any]])
def list_voyages(
    significant: Optional[int] = Query(None, description="1 to filter significant voyages, 0 otherwise"),
    royalty:    Optional[int] = Query(None, description="1 to filter voyages with royalty onboard, 0 otherwise"),
    date_from:  Optional[str] = Query(None, description="ISO date to filter voyages starting on or after this date"),
    date_to:    Optional[str] = Query(None, description="ISO date to filter voyages ending on or before this date")
) -> List[Dict[str, Any]]:
    """
    List voyages with optional filters on significance, royalty, and date range.
    Returns summary fields and flags.
    """
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    base_query = (
        "SELECT voyage_id, start_timestamp, end_timestamp, "
        "additional_info, notes, "
        "\"significant_voyage?\" AS significant, "
        "\"royalty?\" AS royalty FROM voyages"
    )
    conditions, params = [], []

    if significant is not None:
        conditions.append('"significant_voyage?" = %s')
        params.append(significant)
    if royalty is not None:
        conditions.append('"royalty?" = %s')
        params.append(royalty)
    if date_from:
        conditions.append('start_timestamp >= %s')
        params.append(date_from)
    if date_to:
        conditions.append('end_timestamp <= %s')
        params.append(date_to)

    query = base_query + (" WHERE " + " AND ".join(conditions) if conditions else "")
    cur.execute(query, params)
    voyages = cur.fetchall()
    cur.close()
    conn.close()
    return voyages

@router.get("/{voyage_id}", response_model=Dict[str, Any])
def get_voyage(voyage_id: int) -> Dict[str, Any]:
    """Retrieve full details for a single voyage."""
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM voyages WHERE voyage_id = %s", (voyage_id,))
    voyage = cur.fetchone()
    cur.close()
    conn.close()
    if not voyage:
        raise HTTPException(status_code=404, detail="Voyage not found")
    return voyage

@router.get("/{voyage_id}/media", response_model=List[Dict[str, Any]])
def get_voyage_media(voyage_id: int) -> List[Dict[str, Any]]:
    """List media sources associated with a voyage, including captions."""
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        SELECT s.source_id, s.source_type, s.source_origin, s.source_description, s.source_path, vs.page_num
        FROM voyage_sources vs
        JOIN sources s ON vs.source_id = s.source_id
        WHERE vs.voyage_id = %s
        ORDER BY vs.page_num NULLS LAST
        """,
        (voyage_id,)
    )
    media = cur.fetchall()
    cur.close()
    conn.close()
    return media

@router.get("/{voyage_id}/passengers", response_model=List[Dict[str, Any]])
def get_voyage_passengers(voyage_id: int) -> List[Dict[str, Any]]:
    """Retrieve all passengers for a specific voyage."""
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        SELECT p.passenger_id, p.name, p.bio_path, p.basic_info
        FROM passengers p
        JOIN voyage_passengers vp ON p.passenger_id = vp.passenger_id
        WHERE vp.voyage_id = %s
        """,
        (voyage_id,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail="No passengers found for this voyage")
    return rows
