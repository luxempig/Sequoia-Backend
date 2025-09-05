# --- add/replace at top of file (imports) ---
import os
import re
import time
import random
import logging
from typing import Dict, Tuple, Optional

from psycopg2.extras import execute_values
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

LOG = logging.getLogger("voyage_ingest.db_updater")

# ---------- Retry config (env-tunable) ----------
_MAX_RETRIES = int(os.getenv("GAPI_MAX_RETRIES", "10"))          # ~ 6 tries
_BACKOFF_BASE = float(os.getenv("GAPI_BACKOFF_BASE", "0.8"))    # seconds
_BACKOFF_MAX = float(os.getenv("GAPI_BACKOFF_MAX", "30.0"))     # cap per sleep

_SHEETS_SVC = None
_SHEET_CACHE: dict[tuple[str, str], list[list[str]]] = {}
_PRES_UPSERTED: set[str] = set()   # spreadsheet_ids already upserted in this process


def _sheets_service():
    """Cached Google Sheets service."""
    global _SHEETS_SVC
    if _SHEETS_SVC:
        return _SHEETS_SVC
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path for DB pres upsert")
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    _SHEETS_SVC = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _SHEETS_SVC


def _gapi_with_retry(call_fn, what: str):
    """
    Execute a Google API call with exponential backoff + jitter.
    call_fn: zero-arg function that performs .execute() and returns the result.
    """
    for attempt in range(_MAX_RETRIES + 1):
        try:
            return call_fn()
        except HttpError as e:
            status = getattr(e, "status_code", None) or (e.resp.status if hasattr(e, "resp") else None)
            retryable = False
            if status in (429, 500, 502, 503, 504):
                retryable = True
            else:
                # Sometimes 'rateLimitExceeded' is in the error details
                msg = str(e).lower()
                if "ratelimit" in msg or "rate limit" in msg or "userlimit" in msg:
                    retryable = True
            if not retryable or attempt >= _MAX_RETRIES:
                LOG.error("Google API call failed (no more retries) for %s: %s", what, e)
                raise
            sleep_s = min(_BACKOFF_MAX, _BACKOFF_BASE * (2 ** attempt) * (0.5 + random.random()))
            LOG.warning("Google API throttled on %s (status=%s). Retry %d/%d in %.2fs",
                        what, status, attempt + 1, _MAX_RETRIES, sleep_s)
            time.sleep(sleep_s)
        except Exception as e:
            # Non-HttpError transient network hiccups -> retry
            if attempt >= _MAX_RETRIES:
                LOG.error("Google API call failed (non-HttpError) for %s: %s", what, e)
                raise
            sleep_s = min(_BACKOFF_MAX, _BACKOFF_BASE * (2 ** attempt) * (0.5 + random.random()))
            LOG.warning("Google API error on %s (%s). Retry %d/%d in %.2fs",
                        what, e.__class__.__name__, attempt + 1, _MAX_RETRIES, sleep_s)
            time.sleep(sleep_s)


def _read_sheet(spreadsheet_id: str, title: str) -> list[list[str]]:
    """
    Read a sheet tab with caching + retry. Returns list of rows (lists of strings).
    """
    key = (spreadsheet_id, title)
    if key in _SHEET_CACHE:
        return _SHEET_CACHE[key]

    svc = _sheets_service()
    def _do():
        return svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{title}!A:ZZ"
        ).execute()

    res = _gapi_with_retry(_do, what=f"Sheets.values.get({title})")
    rows = res.get("values", []) or []
    _SHEET_CACHE[key] = rows
    return rows


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
def _norm_str(x):
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def _norm_date(x):
    s = _norm_str(x)
    if not s:
        return None
    if not _DATE_RE.match(s):
        # soft-fail: allow odd dates to become NULL; the validator should catch earlier
        LOG.warning("Ignoring non-YYYY-MM-DD date in sheet: %r", s)
        return None
    return s


def _schema(cur):
    """Ensure schema is set; you already have this in your module."""
    schema = os.environ.get("DB_SCHEMA", "sequoia")
    cur.execute(f"SET search_path = {schema}, public;")


def _conn():
    """Your existing connection helper."""
    import psycopg2
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


# --------------- presidents upsert (rate-limit resilient + once-per-process) ---------------

def _upsert_presidents(cur) -> int:
    """
    Read 'presidents' Google Sheet and upsert into DB first.
    Columns (case-insensitive):
      REQUIRED: president_slug, full_name
      OPTIONAL: party, term_start, term_end, wikipedia_url, tags
    Robust to short/blank rows and Google API throttling.
    """
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        LOG.warning("SPREADSHEET_ID not set; skipping presidents upsert.")
        return 0
    if spreadsheet_id in _PRES_UPSERTED:
        LOG.debug("Presidents already upserted for this process; skipping.")
        return 0

    title = os.environ.get("PRESIDENTS_SHEET_TITLE", "presidents").strip() or "presidents"
    rows = _read_sheet(spreadsheet_id, title)
    if not rows:
        LOG.warning("Presidents sheet '%s' is empty; skipping presidents upsert.", title)
        return 0

    header = [(h or "").strip().lower() for h in rows[0]]
    def _hidx(name: str) -> Optional[int]:
        try:
            return header.index(name)
        except ValueError:
            return None

    col_ix = { name: _hidx(name) for name in
               ["president_slug","full_name","party","term_start","term_end","wikipedia_url","tags"] }

    if col_ix["president_slug"] is None or col_ix["full_name"] is None:
        LOG.error("Presidents sheet must include 'president_slug' and 'full_name' headers. Found: %s", header)
        return 0

    def cell(r, i):
        return r[i] if (i is not None and i < len(r)) else ""

    to_upsert = []
    for idx_row, r in enumerate(rows[1:], start=2):
        pres_slug = _norm_str(cell(r, col_ix["president_slug"]))
        full_name = _norm_str(cell(r, col_ix["full_name"]))
        if not pres_slug and not full_name:
            continue  # skip fully blank lines silently
        if not pres_slug or not full_name:
            LOG.warning("Skipping presidents row %d: missing required fields (slug=%r, name=%r)",
                        idx_row, pres_slug, full_name)
            continue

        party         = _norm_str(cell(r, col_ix["party"]))
        term_start    = _norm_date(cell(r, col_ix["term_start"]))
        term_end      = _norm_date(cell(r, col_ix["term_end"]))
        wikipedia_url = _norm_str(cell(r, col_ix["wikipedia_url"]))
        tags          = _norm_str(cell(r, col_ix["tags"]))

        to_upsert.append((pres_slug, full_name, party, term_start, term_end, wikipedia_url, tags))

    if not to_upsert:
        LOG.info("No presidents rows to upsert from '%s'.", title)
        _PRES_UPSERTED.add(spreadsheet_id)
        return 0

    _schema(cur)
    execute_values(cur, """
        INSERT INTO presidents (president_slug, full_name, party, term_start, term_end, wikipedia_url, tags)
        VALUES %s
        ON CONFLICT (president_slug) DO UPDATE SET
          full_name     = EXCLUDED.full_name,
          party         = EXCLUDED.party,
          term_start    = EXCLUDED.term_start,
          term_end      = EXCLUDED.term_end,
          wikipedia_url = EXCLUDED.wikipedia_url,
          tags          = EXCLUDED.tags;
    """, to_upsert)

    _PRES_UPSERTED.add(spreadsheet_id)
    LOG.info("Upserted %d president(s) from sheet '%s'", len(to_upsert), title)
    return len(to_upsert)


# --------------- UPDATED: upsert_all (unchanged logic; calls resilient presidents upsert) ---------------

def upsert_all(bundle: Dict, s3_links: Dict[str, Tuple[Optional[str], Optional[str]]]) -> None:
    """Upsert presidents (from sheet), then voyages, people, media, and join tables for one voyage bundle."""
    v = bundle["voyage"]; ppl = bundle.get("passengers", []) or []; med = bundle.get("media", []) or []
    vslug = v["voyage_slug"]

    conn = _conn(); conn.autocommit = False
    try:
        with conn.cursor() as cur:
            _schema(cur)

            # 1) Presidents first (idempotent + cached + rate-limit–resilient)
            _upsert_presidents(cur)

            # 2) Voyages / People / Media / Joins (same normalization as before)
            DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
            def _ns(x):
                if x is None: return None
                s = str(x).strip()
                return s if s else None
            def _nd(x):
                s = _ns(x)
                if not s: return None
                if not DATE_RE.match(s): raise ValueError(f"Bad date (YYYY-MM-DD): {s}")
                return s

            v_norm = {
                "voyage_slug": _ns(v.get("voyage_slug")),
                "title": _ns(v.get("title")),
                "start_date": _nd(v.get("start_date")),
                "end_date": _nd(v.get("end_date")),
                "origin": _ns(v.get("origin")),
                "destination": _ns(v.get("destination")),
                "vessel_name": _ns(v.get("vessel_name")),
                "voyage_type": _ns(v.get("voyage_type")),
                "summary_markdown": _ns(v.get("summary_markdown") or v.get("summary")),
                "source_urls": _ns(v.get("source_urls")),
                "tags": _ns(v.get("tags")),
            }

            # Voyages
            cur.execute("""
                INSERT INTO voyages (
                    voyage_slug, title, start_date, end_date, origin, destination,
                    vessel_name, voyage_type, summary_markdown, source_urls, tags
                )
                VALUES (%(voyage_slug)s, %(title)s, %(start_date)s, %(end_date)s, %(origin)s, %(destination)s,
                        %(vessel_name)s, %(voyage_type)s, %(summary_markdown)s, %(source_urls)s, %(tags)s)
                ON CONFLICT (voyage_slug) DO UPDATE SET
                    title = EXCLUDED.title,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    origin = EXCLUDED.origin,
                    destination = EXCLUDED.destination,
                    vessel_name = EXCLUDED.vessel_name,
                    voyage_type = EXCLUDED.voyage_type,
                    summary_markdown = EXCLUDED.summary_markdown,
                    source_urls = EXCLUDED.source_urls,
                    tags = EXCLUDED.tags;
            """, v_norm)

            # People
            if ppl:
                people_rows = []
                for p in ppl:
                    people_rows.append((
                        _ns(p.get("slug")),
                        _ns(p.get("full_name")),
                        _ns(p.get("role_title")),
                        _ns(p.get("organization")),
                        int(p["birth_year"]) if _ns(p.get("birth_year")) else None,
                        int(p["death_year"]) if _ns(p.get("death_year")) else None,
                        _ns(p.get("wikipedia_url")),
                        None,
                        _ns(p.get("tags")),
                    ))
                execute_values(cur, """
                    INSERT INTO people (person_slug, full_name, role_title, organization,
                                        birth_year, death_year, wikipedia_url, notes_internal, tags)
                    VALUES %s
                    ON CONFLICT (person_slug) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        role_title = EXCLUDED.role_title,
                        organization = EXCLUDED.organization,
                        birth_year = EXCLUDED.birth_year,
                        death_year = EXCLUDED.death_year,
                        wikipedia_url = EXCLUDED.wikipedia_url,
                        tags = EXCLUDED.tags;
                """, people_rows)

            # Media
            if med:
                media_rows = []
                for m in med:
                    mslug = _ns(m.get("slug"))
                    s3_orig, s3_pub = (s3_links.get(mslug, (None, None)) if mslug else (None, None))
                    media_rows.append((
                        mslug,
                        _ns(m.get("title")),
                        _ns(m.get("media_type")),
                        _ns(s3_orig),
                        _ns(s3_pub),
                        _ns(m.get("credit")),
                        _nd(m.get("date")),
                        _ns(m.get("description") or m.get("description_markdown")),
                        _ns(m.get("tags")),
                        _ns(m.get("google_drive_link")),
                    ))
                execute_values(cur, """
                    INSERT INTO media (
                        media_slug, title, media_type, s3_url, public_derivative_url,
                        credit, date, description_markdown, tags, google_drive_link
                    ) VALUES %s
                    ON CONFLICT (media_slug) DO UPDATE SET
                        title = EXCLUDED.title,
                        media_type = EXCLUDED.media_type,
                        s3_url = EXCLUDED.s3_url,
                        public_derivative_url = EXCLUDED.public_derivative_url,
                        credit = EXCLUDED.credit,
                        date = EXCLUDED.date,
                        description_markdown = EXCLUDED.description_markdown,
                        tags = EXCLUDED.tags,
                        google_drive_link = EXCLUDED.google_drive_link;
                """, media_rows)

            # voyage_passengers
            if ppl:
                vp_rows = []
                for p in ppl:
                    vp_rows.append((vslug, _ns(p.get("slug")), _ns(p.get("capacity_role")), None))
                execute_values(cur, """
                    INSERT INTO voyage_passengers (voyage_slug, person_slug, capacity_role, notes)
                    VALUES %s
                    ON CONFLICT (voyage_slug, person_slug) DO UPDATE SET
                        capacity_role = EXCLUDED.capacity_role,
                        notes = EXCLUDED.notes;
                """, vp_rows)

            # voyage_media
            if med:
                vm_rows = []
                for m in med:
                    mslug = _ns(m.get("slug")) or ""
                    sort = None
                    parts = mslug.rsplit("-", 1)
                    if len(parts) == 2 and parts[1].isdigit():
                        sort = int(parts[1])
                    vm_rows.append((vslug, mslug, sort, None))
                execute_values(cur, """
                    INSERT INTO voyage_media (voyage_slug, media_slug, sort_order, notes)
                    VALUES %s
                    ON CONFLICT (voyage_slug, media_slug) DO UPDATE SET
                        sort_order = COALESCE(EXCLUDED.sort_order, voyage_media.sort_order),
                        notes = EXCLUDED.notes;
                """, vm_rows)

        conn.commit()
        LOG.info("DB upsert complete (presidents first, resilient) for voyage %s", vslug)
    except Exception as e:
        conn.rollback()
        LOG.error("DB upsert failed for voyage %s: %s", vslug, e)
        raise
    finally:
        conn.close()
