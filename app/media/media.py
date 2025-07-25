# media.py

from fastapi import APIRouter, Request
from typing import Any, Dict, List
from psycopg2.extras import RealDictCursor
from database import get_connection

router = APIRouter(prefix="/voyages", tags=["voyages"])

@router.get(
    "/{voyage_id}/media",
    response_model=Dict[str, List[Dict[str, Any]]]
)
def get_voyage_media(voyage_id: int, request: Request) -> Dict[str, List[Dict[str, Any]]]:
    """List media sources associated with a voyage, grouped by type."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        SELECT
          s.source_id,
          s.source_type,
          s.source_origin,
          s.source_description,
          s.source_path,
          vs.page_num
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

    # Rewrite only non-video paths to full static URLs
    for m in media:
        if m["source_type"] != "Video":
            m["source_path"] = str(
                request.url_for("static", path=m["source_path"])
            )

    # Group media items by their type
    media_by_type: Dict[str, List[Dict[str, Any]]] = {}
    for m in media:
        media_by_type.setdefault(m["source_type"], []).append(m)

    return media_by_type
