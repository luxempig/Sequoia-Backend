
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query
from psycopg2.extras import RealDictCursor
from app.db import get_connection
from app.utils.s3 import presign_s3_key
import os

router = APIRouter(prefix="/api/sources", tags=["sources"])

@router.get("/by-voyage/{voyage_id}", response_model=List[Dict[str, Any]])
def sources_for_voyage(
    voyage_id: int,
    presign: bool = Query(True, description="Return presigned URLs if MEDIA_BUCKET is configured"),
    ttl: int = Query(3600, ge=60, le=86400)
) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT s.source_id, s.source_type, s.source_origin, s.source_description,
               s.source_path, s.permalink, vs.page_num
        FROM voyage_sources vs
        JOIN sources s ON s.source_id = vs.source_id
        WHERE vs.voyage_id = %s
        ORDER BY vs.page_num NULLS LAST, s.source_id
        """,
        (voyage_id,),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()

    if presign and os.getenv("MEDIA_BUCKET"):
        for r in rows:
            url = presign_s3_key(r.get("source_path") or "", expires=ttl)
            r["url"] = url or r.get("permalink")
    else:
        for r in rows:
            r["url"] = r.get("permalink") or r.get("source_path")

    return rows

@router.get("/", response_model=List[Dict[str, Any]])
def list_sources(
    q: Optional[str] = Query(None, description="Search in headline/publication/description"),
    type: Optional[str] = Query(None, description="Filter by source_type"),
    origin: Optional[str] = Query(None, description="Filter by source_origin"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    sql = "SELECT source_id, source_type, source_origin, headline, publication, publication_date, source_path, permalink FROM sources"
    conds = []
    params: list = []

    if q:
        conds.append("(COALESCE(headline,'') ILIKE %s OR COALESCE(publication,'') ILIKE %s OR COALESCE(source_description,'') ILIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if type:
        conds.append("source_type = %s"); params.append(type)
    if origin:
        conds.append("source_origin = %s"); params.append(origin)

    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY publication_date NULLS LAST, source_id DESC LIMIT %s OFFSET %s"
    params += [limit, offset]

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows
