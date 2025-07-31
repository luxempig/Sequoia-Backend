from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from psycopg2.extras import RealDictCursor
from app.db import get_connection

router = APIRouter(prefix="/api/voyages", tags=["voyages"])

@router.get("/", response_model=List[Dict[str, Any]])
def list_voyages(
    q: Optional[str] = Query(None, description="Keyword search in info, notes, or passenger names"),
    significant: Optional[int] = Query(None, description="1 to filter significant voyages"),
    royalty: Optional[int]    = Query(None, description="1 to filter voyages with royalty onboard"),
    date_from: Optional[str]   = Query(None, description="ISO date to filter voyages starting on or after this date"),
    date_to: Optional[str]     = Query(None, description="ISO date to filter voyages ending on or before this date")
) -> List[Dict[str, Any]]:
    """
    List voyages with optional filters on significance, royalty, date range,
    keyword search in voyage text or passenger names.
    """
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    base_query = (
        "SELECT DISTINCT v.voyage_id, v.start_timestamp, v.end_timestamp, "
        "v.additional_info, v.notes, "
        "v.\"significant_voyage?\" AS significant, "
        "v.\"royalty?\" AS royalty "
        "FROM voyages v"
    )
    joins: List[str]     = []
    conditions: List[str] = []
    params: List[Any]    = []

    # Text-search in voyages and passenger names
    if q:
        joins.append(" LEFT JOIN voyage_passengers vp ON v.voyage_id = vp.voyage_id")
        joins.append(" LEFT JOIN passengers p ON vp.passenger_id = p.passenger_id")
        conditions.append(
            "(v.additional_info ILIKE %s OR v.notes ILIKE %s OR p.name ILIKE %s)"
        )
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])

    # Other filters
    if significant is not None:
        conditions.append('v.\"significant_voyage?\" = %s')
        params.append(significant)
    if royalty is not None:
        conditions.append('v.\"royalty?\" = %s')
        params.append(royalty)
    if date_from:
        conditions.append('v.start_timestamp >= %s')
        params.append(date_from)
    if date_to:
        conditions.append('v.end_timestamp <= %s')
        params.append(date_to)

    # Assemble final query
    query = base_query + "".join(joins)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY v.start_timestamp"

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