from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from psycopg2.extras import RealDictCursor
from app.db import get_connection

router = APIRouter(tags=["passengers"])

@router.get("/", response_model=List[Dict[str, Any]])
def list_passengers(name: Optional[str] = Query(None, description="Filter by passenger name via ILIKE")) -> List[Dict[str, Any]]:
    """List all passengers, optionally filtering by name."""
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    if name:
        cur.execute(
            "SELECT passenger_id, name, bio_path, basic_info FROM passengers WHERE name ILIKE %s",
            (f"%{name}%",)
        )
    else:
        cur.execute("SELECT passenger_id, name, bio_path, basic_info FROM passengers")

    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

@router.get("/{passenger_id}", response_model=Dict[str, Any])
def get_passenger(passenger_id: int) -> Dict[str, Any]:
    """Retrieve details for a single passenger."""
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        "SELECT passenger_id, name, bio_path, basic_info FROM passengers WHERE passenger_id = %s",
        (passenger_id,)
    )
    passenger = cur.fetchone()
    cur.close()
    conn.close()

    if not passenger:
        raise HTTPException(status_code=404, detail="Passenger not found")
    return passenger