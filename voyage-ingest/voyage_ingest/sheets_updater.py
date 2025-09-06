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

Idempotent upserts with caching and backoff.
"""

import os
import re
import time
import random
import logging
from typing import Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

LOG = logging.getLogger("voyage_ingest.sheets_updater")

TITLE = os.environ.get("PRESIDENTS_SHEET_TITLE", "presidents").strip() or "presidents"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Gentle client-side throttle between calls (seconds)
_RATE_LIMIT_SECONDS = float(os.getenv("SHEETS_RATE_LIMIT_SECONDS", "0.5"))

# ---------- Sheet schema (headers) ----------
INGEST_LOG_TITLE = "ingest_log"
INGEST_LOG_HEADERS = [
    "timestamp_iso","doc_id","voyage_slug","status","errors_count","warnings_count",
    "media_declared","media_uploaded","thumbs_uploaded","sync_mode","dry_run",
    "s3_deleted","s3_archived","sheets_deleted_vm","sheets_deleted_vp",
    "db_deleted_vm","db_deleted_vp","db_deleted_media","db_deleted_people","notes"
]

VOYAGES_HEADERS = [
    "voyage_slug","title","start_date","end_date","start_time","end_time",
    "origin","destination","vessel_name","voyage_type","summary_markdown",
    "notes_internal","source_urls","tags",
]

PASSENGERS_HEADERS = [
    "person_slug","full_name","role_title","organization","birth_year",
    "death_year","wikipedia_url","notes_internal","tags",
]

MEDIA_HEADERS = [
    "media_slug","title","media_type","s3_url","thumbnail_s3_url","credit","date",
    "description_markdown","tags","copyright_restrictions","google_drive_link",
]

VOYAGE_PASSENGERS_HEADERS = ["voyage_slug","person_slug","capacity_role","notes"]
VOYAGE_MEDIA_HEADERS      = ["voyage_slug","media_slug","sort_order","notes"]
VOYAGE_PRESIDENTS_HEADERS = ["voyage_slug","president_slug","notes"]

PRESIDENTS_HEADERS = ["president_slug", "full_name", "party", "term_start", "term_end", "wikipedia_url", "tags"]


PRESIDENT_SLUG_COL_CANDIDATES = ["president_slug","person_slug","slug"]

_svc_spreadsheets = None
_svc_values = None
_last_call_ts = 0.0

# In-process caches to avoid extra reads
_SPREADSHEET_META_CACHE: Dict[str, dict] = {}       # spreadsheet_id -> metadata
_HEADERS_CACHE: Dict[Tuple[str,str], List[str]] = {}  # (sheet_id, title) -> headers

def _sheets_service():
    global _svc_spreadsheets, _svc_values
    if _svc_spreadsheets is not None and _svc_values is not None:
        return _svc_spreadsheets, _svc_values
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    root = build("sheets", "v4", credentials=creds, cache_discovery=False)
    _svc_spreadsheets = root.spreadsheets()
    _svc_values = _svc_spreadsheets.values()
    return _svc_spreadsheets, _svc_values

def _rate_limit():
    global _last_call_ts
    if _RATE_LIMIT_SECONDS <= 0: return
    now = time.time()
    wait = _RATE_LIMIT_SECONDS - (now - _last_call_ts)
    if wait > 0: time.sleep(wait)
    _last_call_ts = time.time()

def _execute_with_backoff(req, *, max_attempts: int = 12, base: float = 0.8, jitter: float = 0.5):
    attempt = 0
    while True:
        try:
            _rate_limit()
            return req.execute(num_retries=0)
        except HttpError as e:
            status = getattr(e, "status_code", None)
            if status is None and hasattr(e, "resp"):
                status = getattr(e.resp, "status", None)
            # retry on common quota / transient errors
            if status in (429,500,502,503,504):
                attempt += 1
                if attempt >= max_attempts:
                    raise
                retry_after = None
                if hasattr(e, "resp") and e.resp is not None:
                    retry_after = e.resp.headers.get("Retry-After") if hasattr(e.resp, "headers") else None
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except Exception:
                        sleep_s = base * (2 ** (attempt - 1)) + random.uniform(0, jitter)
                else:
                    sleep_s = base * (2 ** (attempt - 1)) + random.uniform(0, jitter)
                LOG.warning("Sheets API %s. Backoff %.2fs (attempt %d/%d).", status, sleep_s, attempt, max_attempts)
                time.sleep(sleep_s)
                continue
            raise

# --------- Simple caches ---------

class HeadersCache:
    def __init__(self): self._cache: Dict[Tuple[str,str], List[str]] = {}
    def get(self, sid, title): return self._cache.get((sid,title))
    def set(self, sid, title, headers): self._cache[(sid,title)] = list(headers)
    def invalidate(self, sid, title): self._cache.pop((sid,title), None)
HEADERS_CACHE = HeadersCache()

class IndexCache:
    def __init__(self):
        self._rows: Dict[Tuple[str,str], List[List[str]]] = {}
        self._headers: Dict[Tuple[str,str], List[str]] = {}
        self._keymaps: Dict[Tuple[str,str,str], Dict[str,int]] = {}
    def ensure_loaded(self, sid, title):
        key = (sid,title)
        if key in self._rows: return
        _, values = _sheets_service()
        headers = _get_headers(sid, title)
        self._headers[key] = headers
        req = values.get(spreadsheetId=sid, range=f"{title}!A2:ZZ")
        resp = _execute_with_backoff(req)
        self._rows[key] = resp.get("values", []) or []
    def headers(self, sid, title):
        self.ensure_loaded(sid,title)
        return self._headers[(sid,title)]
    def get_row_index_by_key(self, sid, title, key_col, key_val) -> Optional[int]:
        self.ensure_loaded(sid,title)
        rows = self._rows[(sid,title)]
        headers = self._headers[(sid,title)]
        try: kidx = headers.index(key_col)
        except ValueError: return None
        km_key = (sid,title,key_col)
        if km_key not in self._keymaps:
            mapping: Dict[str,int] = {}
            for i, row in enumerate(rows, start=2):
                if kidx < len(row):
                    mapping.setdefault(row[kidx], i)
            self._keymaps[km_key] = mapping
        return self._keymaps[km_key].get(key_val)
    def update_row(self, sid, title, row_index, row_values):
        key = (sid,title)
        if key not in self._rows: return
        rows = self._rows[key]
        data_idx = row_index - 2
        if 0 <= data_idx < len(rows): rows[data_idx] = row_values
        for km in list(self._keymaps.keys()):
            if km[0]==sid and km[1]==title: self._keymaps.pop(km, None)
    def append_row(self, sid, title, row_values):
        key = (sid,title)
        if key in self._rows: self._rows[key].append(row_values)
        for km in list(self._keymaps.keys()):
            if km[0]==sid and km[1]==title: self._keymaps.pop(km, None)
    def invalidate_headers(self, sid, title, headers):
        key = (sid,title)
        self._headers[key] = list(headers)
        for km in list(self._keymaps.keys()):
            if km[0]==sid and km[1]==title: self._keymaps.pop(km, None)

INDEX_CACHE = IndexCache()

# --------- Utilities ----------

def _get_spreadsheet_metadata(sid: str) -> dict:
    if sid in _SPREADSHEET_META_CACHE:
        return _SPREADSHEET_META_CACHE[sid]
    spreadsheets, _ = _sheets_service()
    req = spreadsheets.get(spreadsheetId=sid, includeGridData=False)
    meta = _execute_with_backoff(req)
    _SPREADSHEET_META_CACHE[sid] = meta
    return meta

def _find_sheet_id_by_title(meta: dict, title: str) -> Optional[int]:
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("title") == title:
            return s.get("properties", {}).get("sheetId")
    return None

def _col_letter(n: int) -> str:
    res = ""
    while n:
        n, r = divmod(n - 1, 26)
        res = chr(65 + r) + res
    return res

def _ensure_tabs(sid: str, title_to_headers: Dict[str, List[str]]) -> None:
    spreadsheets, values = _sheets_service()
    meta = _get_spreadsheet_metadata(sid)
    existing_titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    create_reqs = []
    for title in title_to_headers:
        if title not in existing_titles:
            create_reqs.append({"addSheet":{"properties":{"title":title,"gridProperties":{"frozenRowCount":1}}}})
    if create_reqs:
        req = spreadsheets.batchUpdate(spreadsheetId=sid, body={"requests": create_reqs})
        _execute_with_backoff(req)
        # refresh local metadata cache once after creation
        _SPREADSHEET_META_CACHE.pop(sid, None)
        meta = _get_spreadsheet_metadata(sid)

    # Batch read headers for all tabs we care about in one go
    ranges = [f"{title}!1:1" for title in title_to_headers.keys()]
    req = values.batchGet(spreadsheetId=sid, ranges=ranges)
    resp = _execute_with_backoff(req)
    by_title = {rng["range"].split("!")[0]: (rng.get("values", [[]])[0] if rng.get("values") else []) for rng in resp.get("valueRanges", [])}

    data_updates = []
    updated_titles = []
    for title, headers in title_to_headers.items():
        current = by_title.get(title, [])
        cached = HEADERS_CACHE.get(sid, title)
        if cached is not None:
            current = cached
        if current != headers:
            data_updates.append({
                "range": f"{title}!A1:{_col_letter(len(headers))}1",
                "majorDimension": "ROWS",
                "values": [headers],
            })
            updated_titles.append(title)

    if data_updates:
        req = values.batchUpdate(spreadsheetId=sid, body={
            "valueInputOption":"RAW","data":data_updates,"includeValuesInResponse":True,
            "responseValueRenderOption":"FORMATTED_VALUE"
        })
        _execute_with_backoff(req)
        for title in updated_titles:
            HEADERS_CACHE.set(sid, title, title_to_headers[title])
            INDEX_CACHE.invalidate_headers(sid, title, title_to_headers[title])

def _ensure_tab(sid: str, title: str, headers: List[str]) -> None:
    _ensure_tabs(sid, {title: headers})

def _get_headers(sid: str, title: str) -> List[str]:
    cached = HEADERS_CACHE.get(sid, title)
    if cached is not None: return cached
    _, values = _sheets_service()
    resp = _execute_with_backoff(values.get(spreadsheetId=sid, range=f"{title}!1:1"))
    headers = resp.get("values", [[]])[0] if resp.get("values") else []
    HEADERS_CACHE.set(sid, title, headers)
    return headers

def _dict_to_row(d: Dict[str, str], headers: List[str]) -> List[str]:
    return [str(d.get(h, "") or "") for h in headers]

def _find_row_index_by_key(sid: str, title: str, headers: List[str], key_col: str, key_val: str) -> Optional[int]:
    return INDEX_CACHE.get_row_index_by_key(sid, title, key_col, key_val)

def _upsert(sid: str, title: str, headers: List[str], key_col: str, row_dict: Dict[str, str]) -> None:
    spreadsheets, values = _sheets_service()
    _ensure_tab(sid, title, headers)
    headers = _get_headers(sid, title)
    INDEX_CACHE.ensure_loaded(sid, title)
    key_val = row_dict.get(key_col, "")
    if not key_val:
        raise ValueError(f"Upsert into '{title}' requires key column '{key_col}'")
    row_index = _find_row_index_by_key(sid, title, headers, key_col, key_val)
    row_values = _dict_to_row(row_dict, headers)
    if row_index:
        rng = f"{title}!A{row_index}:{_col_letter(len(headers))}{row_index}"
        _execute_with_backoff(values.update(
            spreadsheetId=sid, range=rng, valueInputOption="RAW", body={"values":[row_values]}
        ))
        INDEX_CACHE.update_row(sid, title, row_index, row_values)
    else:
        _execute_with_backoff(values.append(
            spreadsheetId=sid, range=f"{title}!A:ZZ", valueInputOption="RAW",
            insertDataOption="INSERT_ROWS", body={"values":[row_values]}
        ))
        INDEX_CACHE.append_row(sid, title, row_values)

# --------- Public API ----------

def append_ingest_log(spreadsheet_id: str, rows: List[List[str]]) -> None:
    _ensure_tab(spreadsheet_id, INGEST_LOG_TITLE, INGEST_LOG_HEADERS)
    _, values = _sheets_service()
    _execute_with_backoff(values.append(
        spreadsheetId=spreadsheet_id, range=f"{INGEST_LOG_TITLE}!A:ZZ",
        valueInputOption="RAW", insertDataOption="INSERT_ROWS", body={"values": rows}
    ))

def reset_presidents_sheet(spreadsheet_id: str, presidents: List[Dict[str, str]]) -> None:
    """
    Replace the entire presidents sheet with the provided presidents list.
    """
    
    _ensure_tab(spreadsheet_id, TITLE, PRESIDENTS_HEADERS)
    spreadsheets, values = _sheets_service()
    # Clear existing contents
    _execute_with_backoff(values.clear(
        spreadsheetId=spreadsheet_id, range=f"{TITLE}!A:ZZ"
    ))
    # Re-write headers
    _execute_with_backoff(values.update(
        spreadsheetId=spreadsheet_id,
        range=f"{TITLE}!A1:{_col_letter(len(PRESIDENTS_HEADERS))}1",
        valueInputOption="RAW",
        body={"values":[PRESIDENTS_HEADERS]},
    ))
    # Write rows
    if presidents:
        rows = [[p.get(h, "") for h in PRESIDENTS_HEADERS] for p in presidents]
        _execute_with_backoff(values.append(
            spreadsheetId=spreadsheet_id,
            range=f"{TITLE}!A:ZZ",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ))
    # caches
    HEADERS_CACHE.set(spreadsheet_id, TITLE, PRESIDENTS_HEADERS)
    INDEX_CACHE.invalidate_headers(spreadsheet_id, TITLE, PRESIDENTS_HEADERS)
    LOG.info("Presidents sheet reset with %d row(s).", len(presidents))

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

    _ensure_tabs(spreadsheet_id, {
        "voyages": VOYAGES_HEADERS,
        "passengers": PASSENGERS_HEADERS,
        "media": MEDIA_HEADERS,
        "voyage_passengers": VOYAGE_PASSENGERS_HEADERS,
        "voyage_media": VOYAGE_MEDIA_HEADERS,
        "voyage_presidents": VOYAGE_PRESIDENTS_HEADERS,
        INGEST_LOG_TITLE: INGEST_LOG_HEADERS,
    })

    # ---- Voyages
    vslug = (voyage.get("voyage_slug") or "").strip()
    voyage_row = {
        "voyage_slug": vslug,
        "title": voyage.get("title", ""),
        "start_date": voyage.get("start_date", ""),
        "end_date": voyage.get("end_date", ""),
        "start_time": voyage.get("start_time", ""),
        "end_time": voyage.get("end_time", ""),
        "origin": voyage.get("origin", ""),
        "destination": voyage.get("destination", ""),
        "vessel_name": voyage.get("vessel_name", "USS Sequoia"),
        "voyage_type": voyage.get("voyage_type", ""),
        "summary_markdown": voyage.get("summary") or voyage.get("summary_markdown", ""),
        "notes_internal": voyage.get("notes_internal", ""),
        "source_urls": voyage.get("sources") or voyage.get("source_urls", ""),
        "tags": voyage.get("tags", ""),
    }
    if not voyage_row["voyage_slug"]:
        raise ValueError("voyage_slug is required to update sheets")

    _upsert(spreadsheet_id, "voyages", VOYAGES_HEADERS, "voyage_slug", voyage_row)

    # ---- Passengers
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

    # ---- Media
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

    # ---- Join: voyage_passengers
    for p in passengers:
        person_slug = p.get("slug") or p.get("person_slug", "")
        if not person_slug: continue
        jr = {"voyage_slug": vslug, "person_slug": person_slug, "capacity_role": (p.get("role_title") or "Guest"), "notes": ""}
        _upsert(spreadsheet_id, "voyage_passengers", VOYAGE_PASSENGERS_HEADERS, "person_slug", jr)

    # ---- Join: voyage_media
    def _infer_sort_order(media_slug: str) -> Optional[int]:
        m = re.match(r".*?(\d+)$", media_slug or "")
        try: return int(m.group(1)) if m else None
        except ValueError: return None
    for m in media:
        mslug = m.get("slug", "")
        if not mslug: continue
        sort_order = _infer_sort_order(mslug)
        jr = {"voyage_slug": vslug, "media_slug": mslug, "sort_order": str(sort_order) if sort_order is not None else "", "notes": ""}
        _upsert(spreadsheet_id, "voyage_media", VOYAGE_MEDIA_HEADERS, "media_slug", jr)

    # ---- Join: voyage_presidents
    pres_slug = (voyage.get("president_slug") or "").strip()
    if pres_slug:
        jr = {"voyage_slug": vslug, "president_slug": pres_slug, "notes": ""}
        _upsert(spreadsheet_id, "voyage_presidents", VOYAGE_PRESIDENTS_HEADERS, "president_slug", jr)

    LOG.info("Sheets updated for voyage '%s'", vslug)

