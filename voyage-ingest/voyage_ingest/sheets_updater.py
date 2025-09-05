"""
sheets_updater.py
Update Google Sheets tabs based on parsed voyage data and S3 links.

Tabs handled (created if missing):
- voyages
- passengers
- media
- voyage_passengers (join)
- voyage_media (join)
- voyage_presidents (join; requires 'presidents' tab to exist with president slugs)

This module is idempotent: it upserts rows by their business key columns.

Optimizations added:
- Single Sheets service instance (no re-build per call)
- Exponential backoff (429/5xx) + optional local rate limit
- Batch creation of tabs + batch header writes
- Header caching with invalidation on update
- One-time per-sheet row index cache used by all upserts in a run
- Use includeValuesInResponse to avoid follow-up reads after header writes
"""

import os
import re
import time
import random
import logging
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

LOG = logging.getLogger("voyage_ingest.sheets_updater")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Optional local throttle (seconds between requests). Default off.
_RATE_LIMIT_SECONDS = float(os.getenv("SHEETS_RATE_LIMIT_SECONDS", "0.0"))

# ---------- Sheet schema (headers) ----------
INGEST_LOG_TITLE = "ingest_log"
INGEST_LOG_HEADERS = [
    "timestamp_iso",
    "doc_id",
    "voyage_slug",
    "status",              # OK | WITH_WARNINGS | ERROR
    "errors_count",
    "warnings_count",
    "media_declared",
    "media_uploaded",
    "thumbs_uploaded",
    "sync_mode",           # upsert | prune
    "dry_run",             # TRUE | FALSE
    "s3_deleted",          # objects hard-deleted (if no trash bucket)
    "s3_archived",         # objects copied to trash bucket then deleted
    "sheets_deleted_vm",   # rows removed from voyage_media
    "sheets_deleted_vp",   # rows removed from voyage_passengers
    "db_deleted_vm",       # DB rows removed from voyage_media
    "db_deleted_vp",       # DB rows removed from voyage_passengers
    "db_deleted_media",    # DB master media rows removed (only if prune_masters)
    "db_deleted_people",   # DB master people rows removed (only if prune_masters)
    "notes"
]

VOYAGES_HEADERS = [
    "voyage_slug",
    "title",
    "start_date",
    "end_date",
    "origin",
    "destination",
    "vessel_name",
    "voyage_type",
    "summary_markdown",
    "notes_internal",
    "source_urls",
    "tags",
]

PASSENGERS_HEADERS = [
    "person_slug",
    "full_name",
    "role_title",
    "organization",
    "birth_year",
    "death_year",
    "wikipedia_url",
    "notes_internal",
    "tags",
]

MEDIA_HEADERS = [
    "media_slug",
    "title",
    "media_type",
    "s3_url",
    "thumbnail_s3_url",
    "credit",
    "date",
    "description_markdown",
    "tags",
    "copyright_restrictions",
    "google_drive_link",   # keep original Drive link for curator traceability
]

VOYAGE_PASSENGERS_HEADERS = [
    "voyage_slug",
    "person_slug",
    "capacity_role",
    "notes",
]

VOYAGE_MEDIA_HEADERS = [
    "voyage_slug",
    "media_slug",
    "sort_order",
    "notes",
]

VOYAGE_PRESIDENTS_HEADERS = [
    "voyage_slug",
    "president_slug",
    "notes",
]

# President tab: we only need the slug column name to detect matches.
# We'll accept either 'president_slug' (preferred) or 'person_slug'/'slug' for flexibility.
PRESIDENT_SLUG_COL_CANDIDATES = ["president_slug", "person_slug", "slug"]


# =========================
# Google Sheets infra layer
# =========================

_svc_spreadsheets = None  # spreadsheets() resource
_svc_values = None        # spreadsheets().values() resource
_last_call_ts = 0.0       # for optional local rate limit


def _sheets_service():
    """Create (once) and return spreadsheets + values resources."""
    global _svc_spreadsheets, _svc_values
    if _svc_spreadsheets is not None and _svc_values is not None:
        return _svc_spreadsheets, _svc_values

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    root = build("sheets", "v4", credentials=creds)
    _svc_spreadsheets = root.spreadsheets()
    _svc_values = _svc_spreadsheets.values()
    return _svc_spreadsheets, _svc_values


def _rate_limit():
    """Simple local throttle, disabled by default."""
    global _last_call_ts
    if _RATE_LIMIT_SECONDS <= 0:
        return
    now = time.time()
    wait = _RATE_LIMIT_SECONDS - (now - _last_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.time()


def _execute_with_backoff(req, *, max_attempts: int = 10, base: float = 0.5, jitter: float = 0.25):
    """
    Execute a Google API request with exponential backoff on 429/5xx.
    Respects Retry-After header if provided.
    """
    attempt = 0
    while True:
        try:
            _rate_limit()
            # num_retries=0 so we can honor Retry-After ourselves
            return req.execute(num_retries=0)
        except HttpError as e:
            status = getattr(e, "status_code", None)
            if status is None and hasattr(e, "resp"):
                status = getattr(e.resp, "status", None)
            if status in (429, 500, 502, 503, 504):
                attempt += 1
                if attempt >= max_attempts:
                    raise
                retry_after = None
                if hasattr(e, "resp") and e.resp is not None:
                    retry_after = e.resp.get("retry-after") or e.resp.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except Exception:
                        sleep_s = base * (2 ** (attempt - 1)) + random.uniform(0, jitter)
                else:
                    sleep_s = base * (2 ** (attempt - 1)) + random.uniform(0, jitter)
                LOG.warning("Sheets API %s. Backing off %.2fs (attempt %d/%d).",
                            status, sleep_s, attempt, max_attempts)
                time.sleep(sleep_s)
                continue
            raise


# =========================
# Caches
# =========================

class HeadersCache:
    """
    In-memory header cache per (spreadsheet_id, title).
    Invalidate explicitly on header writes.
    """
    def __init__(self):
        self._cache: Dict[Tuple[str, str], List[str]] = {}

    def get(self, spreadsheet_id: str, title: str) -> Optional[List[str]]:
        return self._cache.get((spreadsheet_id, title))

    def set(self, spreadsheet_id: str, title: str, headers: List[str]) -> None:
        self._cache[(spreadsheet_id, title)] = list(headers)

    def invalidate(self, spreadsheet_id: str, title: str) -> None:
        self._cache.pop((spreadsheet_id, title), None)

HEADERS_CACHE = HeadersCache()


class IndexCache:
    """
    For a given sheet (spreadsheet_id, title), holds:
      - headers
      - all data rows (A2:ZZ)
      - mapping from a key column to row index (1-based; includes header as row 1)
    This lets multiple upserts share a single read of the data.
    """
    def __init__(self):
        # key: (spreadsheet_id, title)
        self._rows: Dict[Tuple[str, str], List[List[str]]] = {}
        self._headers: Dict[Tuple[str, str], List[str]] = {}
        self._keymaps: Dict[Tuple[str, str, str], Dict[str, int]] = {}

    def ensure_loaded(self, spreadsheet_id: str, title: str):
        key = (spreadsheet_id, title)
        if key in self._rows:
            return
        _, values = _sheets_service()
        # Load headers (from cache or API)
        headers = _get_headers(spreadsheet_id, title)
        self._headers[key] = headers
        # Load all rows once
        req = values.get(spreadsheetId=spreadsheet_id, range=f"{title}!A2:ZZ")
        resp = _execute_with_backoff(req)
        rows = resp.get("values", []) or []
        self._rows[key] = rows

    def headers(self, spreadsheet_id: str, title: str) -> List[str]:
        self.ensure_loaded(spreadsheet_id, title)
        return self._headers[(spreadsheet_id, title)]

    def get_row_index_by_key(self, spreadsheet_id: str, title: str, key_col: str, key_val: str) -> Optional[int]:
        """
        Returns 1-based row index including header row as 1, or None.
        """
        self.ensure_loaded(spreadsheet_id, title)
        rows = self._rows[(spreadsheet_id, title)]
        headers = self._headers[(spreadsheet_id, title)]

        try:
            kidx = headers.index(key_col)
        except ValueError:
            return None

        # Build mapping lazily and cache
        km_key = (spreadsheet_id, title, key_col)
        if km_key not in self._keymaps:
            mapping: Dict[str, int] = {}
            for i, row in enumerate(rows, start=2):  # sheet row numbers (header is 1)
                if kidx < len(row):
                    mapping.setdefault(row[kidx], i)
            self._keymaps[km_key] = mapping

        return self._keymaps[km_key].get(key_val)

    def update_row(self, spreadsheet_id: str, title: str, row_index: int, row_values: List[str]):
        """Update in-memory caches after a successful update."""
        key = (spreadsheet_id, title)
        if key not in self._rows:
            return
        rows = self._rows[key]
        # Convert to zero-based within data (A2 starts at row_index=2)
        data_idx = row_index - 2
        if 0 <= data_idx < len(rows):
            rows[data_idx] = row_values

        # Invalidate keymaps because multiple keys might have changed
        for km in list(self._keymaps.keys()):
            if km[0] == spreadsheet_id and km[1] == title:
                self._keymaps.pop(km, None)

    def append_row(self, spreadsheet_id: str, title: str, row_values: List[str]):
        """Append in-memory after a successful append."""
        key = (spreadsheet_id, title)
        if key in self._rows:
            self._rows[key].append(row_values)
        # Invalidate keymaps
        for km in list(self._keymaps.keys()):
            if km[0] == spreadsheet_id and km[1] == title:
                self._keymaps.pop(km, None)

    def invalidate_headers(self, spreadsheet_id: str, title: str, headers: List[str]):
        """Update header cache for this sheet and drop keymaps (column indices may shift)."""
        key = (spreadsheet_id, title)
        self._headers[key] = list(headers)
        for km in list(self._keymaps.keys()):
            if km[0] == spreadsheet_id and km[1] == title:
                self._keymaps.pop(km, None)

INDEX_CACHE = IndexCache()


# =========================
# Tab & header helpers
# =========================

def _get_sheet_metadata(spreadsheet_id: str):
    spreadsheets, _ = _sheets_service()
    req = spreadsheets.get(spreadsheetId=spreadsheet_id, includeGridData=False)
    return _execute_with_backoff(req)


def _find_sheet_id_by_title(meta: dict, title: str) -> Optional[int]:
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("title") == title:
            return s.get("properties", {}).get("sheetId")
    return None


def _ensure_tabs(spreadsheet_id: str, title_to_headers: Dict[str, List[str]]) -> None:
    """
    Ensure all tabs exist and have correct headers.
    - Creates missing tabs in one batchUpdate
    - Writes/updates all headers in a single values.batchUpdate
    - Updates header caches
    """
    spreadsheets, values = _sheets_service()
    meta = _get_sheet_metadata(spreadsheet_id)

    existing_titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    create_reqs = []
    for title in title_to_headers:
        if title not in existing_titles:
            create_reqs.append({
                "addSheet": {
                    "properties": {
                        "title": title,
                        "gridProperties": {"frozenRowCount": 1}
                    }
                }
            })

    if create_reqs:
        LOG.info("Creating %d missing tabs: %s", len(create_reqs), ", ".join(t for t in title_to_headers if t not in existing_titles))
        req = spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": create_reqs})
        _execute_with_backoff(req)

    # For all tabs, (re)write headers if missing or different.
    # To keep API reads minimal, try cache first; if cache miss, fetch via values.get (one per tab).
    data_updates = []
    updated_titles = []
    for title, headers in title_to_headers.items():
        cached = HEADERS_CACHE.get(spreadsheet_id, title)
        if cached is None:
            # fetch current headers once
            rng = f"{title}!1:1"
            req = values.get(spreadsheetId=spreadsheet_id, range=rng)
            resp = _execute_with_backoff(req)
            current = resp.get("values", [[]])[0] if resp.get("values") else []
        else:
            current = cached

        if current != headers:
            data_updates.append({
                "range": f"{title}!A1:{_col_letter(len(headers))}1",
                "majorDimension": "ROWS",
                "values": [headers],
            })
            updated_titles.append(title)

    if data_updates:
        # One batch write for all headers, and ask to include values in response for the last one.
        req = values.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "RAW",
                "data": data_updates,
                "includeValuesInResponse": True,
                "responseValueRenderOption": "FORMATTED_VALUE",
            }
        )
        resp = _execute_with_backoff(req)
        # Update caches with the values we just wrote
        for title in updated_titles:
            HEADERS_CACHE.set(spreadsheet_id, title, title_to_headers[title])
            INDEX_CACHE.invalidate_headers(spreadsheet_id, title, title_to_headers[title])


def _ensure_tab(spreadsheet_id: str, title: str, headers: List[str]) -> None:
    """Compat wrapper for code paths that ensure a single tab."""
    _ensure_tabs(spreadsheet_id, {title: headers})


def _get_headers(spreadsheet_id: str, title: str) -> List[str]:
    cached = HEADERS_CACHE.get(spreadsheet_id, title)
    if cached is not None:
        return cached
    _, values = _sheets_service()
    req = values.get(spreadsheetId=spreadsheet_id, range=f"{title}!1:1")
    resp = _execute_with_backoff(req)
    headers = resp.get("values", [[]])[0] if resp.get("values") else []
    HEADERS_CACHE.set(spreadsheet_id, title, headers)
    return headers


def _read_all(spreadsheet_id: str, title: str) -> List[List[str]]:
    """Direct read helper (rarely used now—IndexCache is preferred)."""
    _, values = _sheets_service()
    req = values.get(spreadsheetId=spreadsheet_id, range=f"{title}!A2:ZZ")
    resp = _execute_with_backoff(req)
    return resp.get("values", [])


def _col_letter(n: int) -> str:
    # 1 -> A, 2 -> B, ...
    res = ""
    while n:
        n, r = divmod(n - 1, 26)
        res = chr(65 + r) + res
    return res


# =========================
# Upsert helpers
# =========================

def _row_to_dict(row: List[str], headers: List[str]) -> Dict[str, str]:
    d = {}
    for i, h in enumerate(headers):
        d[h] = row[i] if i < len(row) else ""
    return d


def _dict_to_row(d: Dict[str, str], headers: List[str]) -> List[str]:
    return [str(d.get(h, "") or "") for h in headers]


def _find_row_index_by_key(spreadsheet_id: str, title: str, headers: List[str], key_col: str, key_val: str) -> Optional[int]:
    """
    Return 1-based row index (including header) for matching key, or None.
    Uses IndexCache for a single read per sheet per run.
    """
    return INDEX_CACHE.get_row_index_by_key(spreadsheet_id, title, key_col, key_val)


def _upsert(spreadsheet_id: str, title: str, headers: List[str], key_col: str, row_dict: Dict[str, str]) -> None:
    spreadsheets, values = _sheets_service()
    # Ensure tab & headers (cached)
    _ensure_tab(spreadsheet_id, title, headers)

    # Get canonical headers (from cache) and ensure IndexCache loaded
    headers = _get_headers(spreadsheet_id, title)
    INDEX_CACHE.ensure_loaded(spreadsheet_id, title)

    key_val = row_dict.get(key_col, "")
    if not key_val:
        raise ValueError(f"Upsert into '{title}' requires key column '{key_col}'")

    row_index = _find_row_index_by_key(spreadsheet_id, title, headers, key_col, key_val)
    row_values = _dict_to_row(row_dict, headers)

    if row_index:
        # Update existing row
        rng = f"{title}!A{row_index}:{_col_letter(len(headers))}{row_index}"
        req = values.update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": [row_values]},
        )
        _execute_with_backoff(req)
        INDEX_CACHE.update_row(spreadsheet_id, title, row_index, row_values)
    else:
        # Append new row
        req = values.append(
            spreadsheetId=spreadsheet_id,
            range=f"{title}!A:ZZ",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row_values]}
        )
        _execute_with_backoff(req)
        INDEX_CACHE.append_row(spreadsheet_id, title, row_values)


# =========================
# Inference helpers
# =========================

_TRAILING_NUM_RE = re.compile(r".*?(\d+)$")

def _infer_sort_order(media_slug: str) -> Optional[int]:
    """
    Examples:
      ...-photo-01 -> 1
      ...-image-12 -> 12
      ...-press-article -> None
    """
    m = _TRAILING_NUM_RE.match(media_slug)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _infer_capacity_role(passenger: Dict[str, str]) -> str:
    # Preference: explicit role_title if present
    role = (passenger.get("role_title") or "").strip()
    if role:
        return role
    tags = (passenger.get("tags") or "").lower()
    if "president" in tags:
        return "President"
    if "press" in tags:
        return "Press"
    if "crew" in tags:
        return "Crew"
    if "guest" in tags:
        return "Guest"
    return "Guest"


def _join_presidents_from_passengers(spreadsheet_id: str, passengers: List[Dict[str, str]]) -> List[str]:
    """
    Return list of passenger slugs that are present in the presidents tab.
    We accept one of several slug column names to be robust.
    Uses cached headers and a single read of the presidents sheet.
    """
    _, values = _sheets_service()

    # Try to read headers from 'presidents' tab; if it doesn't exist, return empty.
    try:
        headers = _get_headers(spreadsheet_id, "presidents")
        if not headers:
            return []
    except Exception:
        return []

    # Determine slug column in presidents tab
    slug_col = None
    for cand in PRESIDENT_SLUG_COL_CANDIDATES:
        if cand in headers:
            slug_col = cand
            break
    if not slug_col:
        return []

    # Single read for all rows
    req = values.get(spreadsheetId=spreadsheet_id, range="presidents!A2:ZZ")
    resp = _execute_with_backoff(req)
    values_rows = resp.get("values", []) or []

    slug_idx = headers.index(slug_col)
    president_slugs = set()
    for row in values_rows:
        if slug_idx < len(row) and row[slug_idx]:
            president_slugs.add(row[slug_idx])

    matched = []
    for p in passengers:
        pslug = p.get("slug") or p.get("person_slug")
        if pslug and pslug in president_slugs:
            matched.append(pslug)
    return matched


def append_ingest_log(spreadsheet_id: str, rows: List[List[str]]) -> None:
    """
    Append one or more rows to the ingest_log tab.
    Each row must match INGEST_LOG_HEADERS.
    """
    _ensure_tab(spreadsheet_id, INGEST_LOG_TITLE, INGEST_LOG_HEADERS)
    _, values = _sheets_service()
    req = values.append(
        spreadsheetId=spreadsheet_id,
        range=f"{INGEST_LOG_TITLE}!A:ZZ",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    )
    _execute_with_backoff(req)


def delete_rows(
    spreadsheet_id: str,
    sheet_title: str,
    key_columns: list[str],
    keys_to_delete: list[tuple[str, str]],
) -> int:
    """
    Delete rows from a sheet by matching composite keys (e.g., (voyage_slug, media_slug)).
    Returns the number of rows deleted.

    Assumptions:
      - Header row is the first row (row 1).
      - key_columns appear in the header with those exact names (case-insensitive match).
      - keys_to_delete is a list of tuples in the same order as key_columns.
    """
    spreadsheets, values = _sheets_service()

    # Get sheetId for DeleteDimension requests (single metadata read)
    req = spreadsheets.get(spreadsheetId=spreadsheet_id)
    meta = _execute_with_backoff(req)
    sheet_id = None
    for sh in meta.get("sheets", []):
        if sh.get("properties", {}).get("title") == sheet_title:
            sheet_id = sh.get("properties", {}).get("sheetId")
            break
    if sheet_id is None:
        return 0  # nothing to delete

    # Read all rows (one call)
    req = values.get(spreadsheetId=spreadsheet_id, range=f"{sheet_title}!A:ZZ")
    vals = _execute_with_backoff(req).get("values", []) or []
    if not vals:
        return 0

    header = [h.strip().lower() for h in vals[0]]
    col_idx = []
    for k in key_columns:
        try:
            col_idx.append(header.index(k.strip().lower()))
        except ValueError:
            # Column missing; nothing we can do safely
            return 0

    # Build a map of rowIndex -> key tuple (skip header)
    to_delete_row_indices = []
    key_set = set(keys_to_delete)

    for i, row in enumerate(vals[1:], start=1):  # row 1 is header; sheet row = i+1
        # construct tuple
        tup = tuple((row[j] if j < len(row) else "").strip() for j in col_idx)
        if tup in key_set:
            to_delete_row_indices.append(i)  # zero-based for DeleteDimension uses this index with header=0

    if not to_delete_row_indices:
        return 0

    # Build batchUpdate requests; delete bottom-up so indices don't shift
    requests = []
    for r in sorted(to_delete_row_indices, reverse=True):
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": r,        # delete row r (0-based, header is 0)
                    "endIndex": r + 1
                }
            }
        })

    req = spreadsheets.batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    )
    _execute_with_backoff(req)

    # Update caches if we have them
    if (spreadsheet_id, sheet_title) in INDEX_CACHE._rows:
        rows = INDEX_CACHE._rows[(spreadsheet_id, sheet_title)]
        # Convert zero-based indices in sheet to data indices (header=0)
        for r in sorted(to_delete_row_indices, reverse=True):
            data_idx = r - 1
            if 0 <= data_idx < len(rows):
                rows.pop(data_idx)
        for km in list(INDEX_CACHE._keymaps.keys()):
            if km[0] == spreadsheet_id and km[1] == sheet_title:
                INDEX_CACHE._keymaps.pop(km, None)

    return len(to_delete_row_indices)


# =========================
# Public API
# =========================

def update_all(spreadsheet_id: str, voyage_data: Dict, s3_links: Dict[str, Tuple[str, Optional[str]]]) -> None:
    """
    Upsert voyages, passengers, media; then populate join tables.
    :param spreadsheet_id: target Google Sheet
    :param voyage_data: dict with keys {voyage, passengers, media}
    :param s3_links: { media_slug: (s3_url, thumb_url) }
    """
    voyage = voyage_data.get("voyage", {})
    passengers = voyage_data.get("passengers", []) or []
    media = voyage_data.get("media", []) or []

    # Ensure all core tabs up-front, with one metadata read and batched writes.
    _ensure_tabs(
        spreadsheet_id,
        {
            "voyages": VOYAGES_HEADERS,
            "passengers": PASSENGERS_HEADERS,
            "media": MEDIA_HEADERS,
            "voyage_passengers": VOYAGE_PASSENGERS_HEADERS,
            "voyage_media": VOYAGE_MEDIA_HEADERS,
            # voyage_presidents ensured only if needed below
            INGEST_LOG_TITLE: INGEST_LOG_HEADERS,
        },
    )

    # ---------- Voyages ----------
    voyage_row = {
        "voyage_slug": voyage.get("voyage_slug", ""),
        "title": voyage.get("title", ""),
        "start_date": voyage.get("start_date", ""),
        "end_date": voyage.get("end_date", ""),
        "origin": voyage.get("origin", ""),
        "destination": voyage.get("destination", ""),
        "vessel_name": voyage.get("vessel_name", "USS Sequoia"),
        "voyage_type": voyage.get("voyage_type", ""),
        # Optional fields (parser might not have them):
        "summary_markdown": voyage.get("summary") or voyage.get("summary_markdown", ""),
        "notes_internal": voyage.get("notes_internal", ""),
        "source_urls": voyage.get("sources") or voyage.get("source_urls", ""),
        "tags": voyage.get("tags", ""),
    }
    if not voyage_row["voyage_slug"]:
        raise ValueError("voyage_slug is required to update sheets")

    _upsert(spreadsheet_id, "voyages", VOYAGES_HEADERS, "voyage_slug", voyage_row)

    # ---------- Passengers ----------
    for p in passengers:
        row = {
            "person_slug": p.get("slug") or p.get("person_slug", ""),
            "full_name": p.get("full_name", ""),
            "role_title": p.get("role_title", ""),
            "organization": p.get("organization", ""),
            "birth_year": p.get("birth_year", ""),
            "death_year": p.get("death_year", ""),
            "wikipedia_url": p.get("wikipedia_url", ""),
            "notes_internal": p.get("notes_internal", ""),
            "tags": p.get("tags", ""),
        }
        if not row["person_slug"] or not row["full_name"]:
            LOG.warning("Skipping passenger with missing slug/full_name: %s", row)
            continue
        _upsert(spreadsheet_id, "passengers", PASSENGERS_HEADERS, "person_slug", row)

    # ---------- Media ----------
    vslug = voyage_row["voyage_slug"]

    for m in media:
        mslug = m.get("slug", "")
        s3_url, thumb_url = s3_links.get(mslug, (m.get("s3_url", ""), m.get("thumbnail_s3_url", "")))

        row = {
            "media_slug": mslug,
            "title": m.get("title", ""),
            "media_type": m.get("media_type", ""),
            "s3_url": s3_url,
            "thumbnail_s3_url": (thumb_url or ""),
            "credit": m.get("credit", ""),
            "date": m.get("date", ""),
            "description_markdown": m.get("description") or m.get("description_markdown", ""),
            "tags": m.get("tags", ""),
            "copyright_restrictions": m.get("copyright_restrictions", ""),
            "google_drive_link": m.get("google_drive_link", ""),
        }
        if not row["media_slug"]:
            LOG.warning("Skipping media with missing slug: %s", row)
            continue
        _upsert(spreadsheet_id, "media", MEDIA_HEADERS, "media_slug", row)

    # ---------- Join: voyage_passengers ----------
    for p in passengers:
        person_slug = p.get("slug") or p.get("person_slug", "")
        if not person_slug:
            continue
        jr = {
            "voyage_slug": vslug,
            "person_slug": person_slug,
            "capacity_role": _infer_capacity_role(p),
            "notes": "",
        }
        # NOTE: This keys by person_slug only (as in the original), which may overwrite
        # duplicates across voyages. For a true composite PK (voyage_slug+person_slug),
        # add a synthetic key column or change the lookup to scan for both fields.
        _upsert(spreadsheet_id, "voyage_passengers", VOYAGE_PASSENGERS_HEADERS, "person_slug", jr)

    # ---------- Join: voyage_media ----------
    for m in media:
        mslug = m.get("slug", "")
        if not mslug:
            continue
        sort_order = _infer_sort_order(mslug)
        jr = {
            "voyage_slug": vslug,
            "media_slug": mslug,
            "sort_order": str(sort_order) if sort_order is not None else "",
            "notes": "",
        }
        _upsert(spreadsheet_id, "voyage_media", VOYAGE_MEDIA_HEADERS, "media_slug", jr)

    # ---------- Join: voyage_presidents (based on presidents tab) ----------
    pres_slugs = _join_presidents_from_passengers(spreadsheet_id, passengers)
    if pres_slugs:
        _ensure_tab(spreadsheet_id, "voyage_presidents", VOYAGE_PRESIDENTS_HEADERS)
        for pslug in pres_slugs:
            jr = {"voyage_slug": vslug, "president_slug": pslug, "notes": ""}
            _upsert(spreadsheet_id, "voyage_presidents", VOYAGE_PRESIDENTS_HEADERS, "president_slug", jr)

    LOG.info("Sheets updated for voyage '%s'", vslug)
