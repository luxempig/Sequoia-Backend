# File: app/routers/presidents.py
from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException
from psycopg2.extras import RealDictCursor
from app.db import get_connection

router = APIRouter(prefix="/api/presidents", tags=["presidents"])


@router.get("/", response_model=List[Dict[str, Any]])
def list_presidents() -> List[Dict[str, Any]]:
    """
    Return every president in chronological order.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        SELECT president_id,
               full_name,
               term_start,
               term_end
        FROM presidents
        ORDER BY term_start
        """
    )
    presidents = cur.fetchall()
    cur.close()
    conn.close()
    return presidents


@router.get("/{president_id}/voyages", response_model=List[Dict[str, Any]])
def voyages_by_president(president_id: int) -> List[Dict[str, Any]]:
    """
    Convenience endpoint – all voyages that fall under a given president’s
    ownership period. Uses the `voyage_with_presidency` view.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        SELECT *
        FROM voyage_with_presidency
        WHERE president_id = %s
        ORDER BY start_timestamp
        """,
        (president_id,),
    )
    voyages = cur.fetchall()
    cur.close()
    conn.close()

    if not voyages:
        raise HTTPException(status_code=404, detail="No voyages found")
    return voyages
