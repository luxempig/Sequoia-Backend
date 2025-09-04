from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

def get_connection():
    return psycopg2.connect(
        host     = os.getenv("DB_HOST"),
        port     = os.getenv("DB_PORT", "5432"),
        dbname   = os.getenv("DB_NAME"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        sslmode  = "require"
    )

@contextmanager
def db_cursor():
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cur
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()
