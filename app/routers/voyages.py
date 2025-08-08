
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from app.db import get_connection
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/voyages", tags=["voyages"])

@router.get("/", response_model=List[Dict[str, Any]])
def list_voyages(
    q: Optional[str] = Query(None, description="Keyword search"),
    significant: Optional[int] = Query(None, description="1 = significant"),
    royalty: Optional[int] = Query(None, description="1 = royalty onboard"),
    president_id: Optional[int] = Query(None, description="Filter by president_id"),
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD from"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD to"),
    limit: int = Query(250, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    passenger: Optional[str] = Query(None, description="Filter by passenger name ILIKE"),
    has_media: Optional[bool] = Query(None, description="Only voyages with at least one linked source"),
    tag: Optional[str] = Query(None, description="Filter by tag name (exact)"),
    sort: str = Query("start_timestamp", regex="^(start_timestamp|end_timestamp)$"),
    order: str = Query("asc", regex="^(asc|desc)$"),
) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    base = (
        "SELECT DISTINCT vw.voyage_id, vw.start_timestamp, vw.end_timestamp, "
        "vw.additional_info, vw.notes, "
        "vw.\"significant_voyage?\" AS significant, "
        "vw.\"royalty?\"       AS royalty, "
        "vw.president_id, vw.president_name "
        "FROM voyage_with_presidency vw"
    )

    joins: List[str] = []
    conds: List[str] = []
    params: List[Any] = []

    if q:
        joins += [
            " LEFT JOIN voyage_passengers vp ON vw.voyage_id = vp.voyage_id",
            " LEFT JOIN passengers p ON vp.passenger_id = p.passenger_id",
        ]
        conds.append("(vw.additional_info ILIKE %s OR vw.notes ILIKE %s OR p.name ILIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]

    if passenger:
        joins += [
            " LEFT JOIN voyage_passengers vp2 ON vw.voyage_id = vp2.voyage_id",
            " LEFT JOIN passengers p2 ON vp2.passenger_id = p2.passenger_id",
        ]
        conds.append("p2.name ILIKE %s")
        params.append(f"%{passenger}%")

    if has_media is True:
        conds.append("EXISTS (SELECT 1 FROM voyage_sources vs WHERE vs.voyage_id = vw.voyage_id)")
    elif has_media is False:
        conds.append("NOT EXISTS (SELECT 1 FROM voyage_sources vs WHERE vs.voyage_id = vw.voyage_id)")

    if tag:
        joins += [
            " LEFT JOIN entity_tags et ON et.entity_type = 'voyage' AND et.entity_id = vw.voyage_id",
            " LEFT JOIN tags t ON t.tag_id = et.tag_id",
        ]
        conds.append("t.name = %s")
        params.append(tag)

    if significant is not None:
        conds.append('vw."significant_voyage?" = %s')
        params.append(significant)

    if royalty is not None:
        conds.append('vw."royalty?" = %s')
        params.append(royalty)

    if president_id is not None:
        conds.append("vw.president_id = %s")
        params.append(president_id)

    if date_from:
        conds.append("vw.start_timestamp >= %s")
        params.append(date_from)

    if date_to:
        conds.append("vw.end_timestamp <= %s")
        params.append(date_to)

    sql = base + "".join(joins) + (" WHERE " + " AND ".join(conds) if conds else "")
    sql += f" ORDER BY vw.{sort} {order.upper()} NULLS LAST LIMIT %s OFFSET %s"
    params += [limit, offset]

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@router.get("/{voyage_id}", response_model=Dict[str, Any])
def get_voyage(voyage_id: int) -> Dict[str, Any]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM voyage_with_presidency WHERE voyage_id = %s", (voyage_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Voyage not found")
    return row

@router.get("/{voyage_id}/media", response_model=List[Dict[str, Any]])
def get_voyage_media(voyage_id: int) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT s.source_id, s.source_type, s.source_origin,
               s.source_description, s.source_path, vs.page_num
        FROM voyage_sources vs
        LEFT JOIN sources s ON s.source_id = vs.source_id
        WHERE vs.voyage_id = %s
        ORDER BY vs.page_num NULLS LAST, s.source_id
        """,
        (voyage_id,),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@router.get("/{voyage_id}/passengers", response_model=List[Dict[str, Any]])
def get_voyage_passengers(voyage_id: int) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT p.passenger_id, p.name, p.bio_path, p.basic_info
        FROM voyage_passengers vp
        LEFT JOIN passengers p ON p.passenger_id = vp.passenger_id
        WHERE vp.voyage_id = %s
        """,
        (voyage_id,),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows
