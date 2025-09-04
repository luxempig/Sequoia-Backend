from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from psycopg2.extras import RealDictCursor
from app.db import get_connection

router = APIRouter(prefix="/api/voyages", tags=["voyages"])

@router.get("/", response_model=List[Dict[str, Any]])
def list_voyages(
    q: Optional[str] = Query(None, description="Keyword search in title/summary_markdown/notes_internal"),
    origin: Optional[str] = None,
    destination: Optional[str] = None,
    voyage_type: Optional[str] = None,
    date_from: Optional[str] = Query(None, description="start_date >= YYYY-MM-DD"),
    date_to: Optional[str]   = Query(None, description="end_date <= YYYY-MM-DD"),
    has_media: Optional[bool] = Query(None, description="Filter voyages that do/do not have media"),
    person: Optional[str] = Query(None, description="Filter by people.full_name ILIKE"),
    president_slug: Optional[str] = Query(None, description="Filter by exact president_slug via voyage_presidents join"),
    sort: str = Query("start_date", pattern="^(start_date|end_date|title)$"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    limit: int = Query(250, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    base = "SELECT DISTINCT v.* FROM voyages v"
    joins: List[str] = []
    conds: List[str] = []
    params: List[Any] = []

    if q:
        conds.append("(COALESCE(v.title,'') ILIKE %s OR COALESCE(v.summary_markdown,'') ILIKE %s OR COALESCE(v.notes_internal,'') ILIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]

    if origin:
        conds.append("v.origin = %s"); params.append(origin)
    if destination:
        conds.append("v.destination = %s"); params.append(destination)
    if voyage_type:
        conds.append("v.voyage_type = %s"); params.append(voyage_type)
    if date_from:
        conds.append("v.start_date >= %s"); params.append(date_from)
    if date_to:
        conds.append("v.end_date <= %s"); params.append(date_to)

    if has_media is True:
        joins.append("INNER JOIN voyage_media vm ON vm.voyage_slug = v.voyage_slug")
    elif has_media is False:
        conds.append("NOT EXISTS (SELECT 1 FROM voyage_media vm2 WHERE vm2.voyage_slug = v.voyage_slug)")

    if person:
        joins.append("LEFT JOIN voyage_passengers vp ON vp.voyage_slug = v.voyage_slug")
        joins.append("LEFT JOIN people p ON p.person_slug = vp.person_slug")
        conds.append("p.full_name ILIKE %s"); params.append(f"%{person}%")

    if president_slug:
        joins.append("INNER JOIN voyage_presidents vpr ON vpr.voyage_slug = v.voyage_slug")
        conds.append("vpr.president_slug = %s"); params.append(president_slug)

    sql = base + (" " + " ".join(joins) if joins else "")
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += f" ORDER BY v.{sort} {order.upper()} NULLS LAST LIMIT %s OFFSET %s"
    params += [limit, offset]

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@router.get("/{voyage_slug}", response_model=Dict[str, Any])
def get_voyage(voyage_slug: str) -> Dict[str, Any]:
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM voyages WHERE voyage_slug = %s", (voyage_slug,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Voyage not found")
    return row

@router.get("/{voyage_slug}/presidents", response_model=List[Dict[str, Any]])
def voyage_presidents(voyage_slug: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT pr.*
        FROM voyage_presidents vp
        JOIN presidents pr ON pr.president_slug = vp.president_slug
        WHERE vp.voyage_slug = %s
        ORDER BY pr.term_start
        """,
        (voyage_slug,)
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

@router.get("/{voyage_slug}/people", response_model=List[Dict[str, Any]])
def voyage_people(voyage_slug: str) -> List[Dict[str, Any]]:
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
