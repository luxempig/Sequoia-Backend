# File: app/routers/voyages.py
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from psycopg2.extras import RealDictCursor
from app.db import get_connection

router = APIRouter(prefix="/api/voyages", tags=["voyages"])


@router.get("/", response_model=List[Dict[str, Any]])
def list_voyages(
    q: Optional[str] = Query(
        None, description="Keyword search in info, notes, or passenger names"
    ),
    significant: Optional[int] = Query(
        None, description="1 to filter significant voyages"
    ),
    royalty: Optional[int] = Query(
        None, description="1 to filter voyages with royalty onboard"
    ),
    president_id: Optional[int] = Query(
        None, description="Filter voyages by president_id"
    ),
    date_from: Optional[str] = Query(
        None, description="ISO date to filter voyages starting on or after this date"
    ),
    date_to: Optional[str] = Query(
        None, description="ISO date to filter voyages ending on or before this date"
    ),
) -> List[Dict[str, Any]]:
    """
    List voyages with optional filters.  **NOW BACKED BY THE VIEW
    `voyage_with_presidency`**, so we can filter on `president_id`
    without touching the original `voyages` table.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    base_query = (
        "SELECT DISTINCT vw.voyage_id, "
        "vw.start_timestamp, vw.end_timestamp, "
        "vw.additional_info, vw.notes, "
        "vw.\"significant_voyage?\" AS significant, "
        "vw.\"royalty?\"       AS royalty, "
        "vw.president_id, vw.president_name "
        "FROM voyage_with_presidency vw"
    )

    joins: List[str] = []
    conditions: List[str] = []
    params: List[Any] = []

    # Keyword search (voyage text OR passenger names)
    if q:
        joins.append(
            " LEFT JOIN voyage_passengers vp ON vw.voyage_id = vp.voyage_id"
        )
        joins.append(" LEFT JOIN passengers p ON vp.passenger_id = p.passenger_id")
        conditions.append(
            "(vw.additional_info ILIKE %s OR vw.notes ILIKE %s OR p.name ILIKE %s)"
        )
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])

    # Simple column filters
    if significant is not None:
        conditions.append('vw."significant_voyage?" = %s')
        params.append(significant)

    if royalty is not None:
        conditions.append('vw."royalty?" = %s')
        params.append(royalty)

    if president_id is not None:
        conditions.append("vw.president_id = %s")
        params.append(president_id)

    if date_from:
        conditions.append("vw.start_timestamp >= %s")
        params.append(date_from)

    if date_to:
        conditions.append("vw.end_timestamp <= %s")
        params.append(date_to)

    # Assemble final SQL
    query = base_query + "".join(joins)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY vw.start_timestamp"

    cur.execute(query, params)
    voyages = cur.fetchall()
    cur.close()
    conn.close()
    return voyages


@router.get("/{voyage_id}", response_model=Dict[str, Any])
def get_voyage(voyage_id: int) -> Dict[str, Any]:
    """
    Retrieve full details for a single voyage (plus `president_name`
    if applicable) from the view.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        "SELECT * FROM voyage_with_presidency WHERE voyage_id = %s",
        (voyage_id,),
    )
    voyage = cur.fetchone()
    cur.close()
    conn.close()

    if not voyage:
        raise HTTPException(status_code=404, detail="Voyage not found")
    return voyage
