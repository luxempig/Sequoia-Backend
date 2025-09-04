from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query
from psycopg2.extras import RealDictCursor
from app.db import get_connection
from app.utils.s3 import presign_from_media_s3_url
from app.config import get_settings

router = APIRouter(prefix="/api/media", tags=["media"])

@router.get("/", response_model=List[Dict[str, Any]])
def list_media(
    q: Optional[str] = Query(None, description="Search title/description_markdown/credit/publication-like fields"),
    media_type: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None, description="media.date >= YYYY-MM-DD"),
    date_to: Optional[str]   = Query(None, description="media.date <= YYYY-MM-DD"),
    voyage_slug: Optional[str] = Query(None, description="Filter by a specific voyage via voyage_media"),
    presign: bool = Query(False),
    ttl: Optional[int] = Query(None, ge=60, le=86400),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    base = "SELECT m.* FROM media m"
    joins: List[str] = []
    conds: List[str] = []
    params: List[Any] = []

    if voyage_slug:
        joins.append("INNER JOIN voyage_media vm ON vm.media_slug = m.media_slug AND vm.voyage_slug = %s")
        params.append(voyage_slug)

    if q:
        conds.append(
            "(COALESCE(m.title,'') ILIKE %s OR COALESCE(m.description_markdown,'') ILIKE %s OR COALESCE(m.credit,'') ILIKE %s)"
        )
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if media_type:
        conds.append("m.media_type = %s"); params.append(media_type)
    if date_from:
        conds.append("m.date >= %s"); params.append(date_from)
    if date_to:
        conds.append("m.date <= %s"); params.append(date_to)

    sql = base + (" " + " ".join(joins) if joins else "")
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY m.date NULLS LAST, m.media_slug LIMIT %s OFFSET %s"
    params += [limit, offset]

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()

    if presign:
        ttl_eff = int(ttl) if ttl is not None else get_settings().PRESIGNED_TTL
        for r in rows:
            r["url"] = (
                presign_from_media_s3_url(r.get("s3_url") or "", expires=ttl_eff)
                or r.get("public_derivative_url")
                or r.get("google_drive_link")
                or r.get("s3_url")
            )
    else:
        for r in rows:
            r["url"] = r.get("public_derivative_url") or r.get("google_drive_link") or r.get("s3_url")

    return rows

@router.get("/by-voyage/{voyage_slug}", response_model=List[Dict[str, Any]])
def media_for_voyage(
    voyage_slug: str,
    presign: bool = Query(True),
    ttl: Optional[int] = Query(None, ge=60, le=86400),
) -> List[Dict[str, Any]]:
    """
    Returns media joined via voyage_media, preserving sort_order and notes.
    """
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT m.*, vm.sort_order, vm.notes AS voyage_media_notes
        FROM voyage_media vm
        JOIN media m ON m.media_slug = vm.media_slug
        WHERE vm.voyage_slug = %s
        ORDER BY vm.sort_order NULLS LAST, m.date NULLS LAST, m.media_slug
        """,
        (voyage_slug,)
    )
    rows = cur.fetchall()
    cur.close(); conn.close()

    if presign:
        ttl_eff = int(ttl) if ttl is not None else get_settings().PRESIGNED_TTL
        for r in rows:
            r["url"] = (
                presign_from_media_s3_url(r.get("s3_url") or "", expires=ttl_eff)
                or r.get("public_derivative_url")
                or r.get("google_drive_link")
                or r.get("s3_url")
            )
    else:
        for r in rows:
            r["url"] = r.get("public_derivative_url") or r.get("google_drive_link") or r.get("s3_url")

    return rows

@router.get("/{media_slug}", response_model=Dict[str, Any])
def get_media(media_slug: str, presign: bool = Query(True), ttl: Optional[int] = Query(None, ge=60, le=86400)) -> Dict[str, Any]:
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM media WHERE media_slug = %s", (media_slug,))
    row = cur.fetchone()
    cur.close(); conn.close()

    if not row:
        return {}

    if presign:
        ttl_eff = int(ttl) if ttl is not None else get_settings().PRESIGNED_TTL
        row["url"] = (
            presign_from_media_s3_url(row.get("s3_url") or "", expires=ttl_eff)
            or row.get("public_derivative_url")
            or row.get("google_drive_link")
            or row.get("s3_url")
        )
    else:
        row["url"] = row.get("public_derivative_url") or row.get("google_drive_link") or row.get("s3_url")
    return row
