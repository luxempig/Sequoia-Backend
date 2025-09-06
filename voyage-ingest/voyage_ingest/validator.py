from __future__ import annotations

import os
import re
import logging
from typing import Dict, List, Set, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

from voyage_ingest.slugger import slugify

LOG = logging.getLogger("voyage_ingest.validator")

# Accept YYYY or YYYY-MM or YYYY-MM-DD
DATE_RE_FLEX = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")
TIME_RE = re.compile(r"^\d{2}:\d{2}(:\d{2})?$")  # HH:MM or HH:MM:SS

PERSON_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)+(?:-[a-z0-9]+)?$")
MEDIA_SLUG_RE_TMPL = r"^{date}-[a-z0-9-]+-{vslug}-\d{{2}}$"

VALID_VOYAGE_TYPES = {"official", "private", "maintenance", "other"}

# --------- Presidents sheet helpers ---------

_SHEETS_SVC = None
_PRESIDENT_SLUG_CACHE: Optional[Set[str]] = None
_PRES_FULL_TO_SLUG: Optional[Dict[str, str]] = None

def _sheets_service():
    global _SHEETS_SVC
    if _SHEETS_SVC is not None:
        return _SHEETS_SVC
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path for validator")
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    _SHEETS_SVC = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _SHEETS_SVC

def _read_president_slugs() -> Set[str]:
    global _PRESIDENT_SLUG_CACHE
    if _PRESIDENT_SLUG_CACHE is not None:
        return _PRESIDENT_SLUG_CACHE
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        _PRESIDENT_SLUG_CACHE = set()
        return _PRESIDENT_SLUG_CACHE
    title = os.environ.get("PRESIDENTS_SHEET_TITLE", "presidents").strip() or "presidents"
    svc = _sheets_service()
    try:
        res = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{title}!A:ZZ"
        ).execute()
        values = res.get("values", []) or []
        if not values:
            _PRESIDENT_SLUG_CACHE = set()
            return _PRESIDENT_SLUG_CACHE
        header = [h.strip().lower() for h in values[0]]
        if "president_slug" not in header:
            _PRESIDENT_SLUG_CACHE = set()
            return _PRESIDENT_SLUG_CACHE
        idx = header.index("president_slug")
        slugs: Set[str] = set()
        for row in values[1:]:
            if idx < len(row):
                s = row[idx].strip().lower()
                if s:
                    slugs.add(s)
        _PRESIDENT_SLUG_CACHE = slugs
        return _PRESIDENT_SLUG_CACHE
    except Exception:
        _PRESIDENT_SLUG_CACHE = set()
        return _PRESIDENT_SLUG_CACHE

def _read_pres_fullname_to_slug() -> Dict[str, str]:
    global _PRES_FULL_TO_SLUG
    if _PRES_FULL_TO_SLUG is not None:
        return _PRES_FULL_TO_SLUG
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        _PRES_FULL_TO_SLUG = {}
        return _PRES_FULL_TO_SLUG
    title = os.environ.get("PRESIDENTS_SHEET_TITLE", "presidents").strip() or "presidents"
    svc = _sheets_service()
    try:
        res = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{title}!A:ZZ"
        ).execute()
        values = res.get("values", []) or []
        if not values:
            _PRES_FULL_TO_SLUG = {}
            return _PRES_FULL_TO_SLUG
        header = [h.strip().lower() for h in values[0]]
        if "president_slug" not in header or "full_name" not in header:
            _PRES_FULL_TO_SLUG = {}
            return _PRES_FULL_TO_SLUG
        i_full = header.index("full_name")
        i_slug = header.index("president_slug")
        m: Dict[str, str] = {}
        for row in values[1:]:
            full = (row[i_full] if i_full < len(row) else "").strip()
            slug = (row[i_slug] if i_slug < len(row) else "").strip()
            if full and slug:
                m[full.lower()] = slug
        _PRES_FULL_TO_SLUG = m
        return _PRES_FULL_TO_SLUG
    except Exception:
        _PRES_FULL_TO_SLUG = {}
        return _PRES_FULL_TO_SLUG

# --------- Field validators ---------

def _req(d: Dict, key: str, path: str, errs: List[str]):
    if not (d.get(key) or "").strip():
        errs.append(f"[{path}] missing required field: {key}")

def _date_flex(d: Dict, key: str, path: str, errs: List[str]):
    v = (d.get(key) or "").strip()
    if v and not DATE_RE_FLEX.match(v):
        errs.append(f"[{path}] invalid date for {key}: {v} (YYYY or YYYY-MM or YYYY-MM-DD)")

def _time_opt(d: Dict, key: str, path: str, errs: List[str]):
    v = (d.get(key) or "").strip()
    if v and not TIME_RE.match(v):
        errs.append(f"[{path}] invalid time for {key}: {v} (HH:MM or HH:MM:SS)")

def _enum(d: Dict, key: str, allowed: set, path: str, errs: List[str]):
    v = (d.get(key) or "").strip().lower()
    if v and v not in allowed:
        errs.append(f"[{path}] invalid value for {key}: {v} (allowed: {sorted(allowed)})")

def _is_supported_media_link(s: str) -> bool:
    s = (s or "").lower()
    return ("/file/d/" in s) or ("dropbox.com" in s)

# --------- Bundle validator ---------

def validate_bundle(bundle: Dict) -> List[str]:
    """
    Validate voyage + passengers + media.

    Notes:
    - Parser supplies president & president_slug (from the last ## President header).
    - voyage_slug is always parser-generated; we validate structure/prefix only.
    - Media 'title' is OPTIONAL.
    - Dates accept YYYY / YYYY-MM / YYYY-MM-DD.
    """
    errs: List[str] = []
    v = bundle.get("voyage") or {}
    ppl = bundle.get("passengers") or []
    med = bundle.get("media") or []

    # Voyage fields
    _req(v, "voyage_slug", "voyage", errs)
    _req(v, "title", "voyage", errs)
    _req(v, "start_date", "voyage", errs)

    _date_flex(v, "start_date", "voyage", errs)
    if v.get("end_date"):
        _date_flex(v, "end_date", "voyage", errs)
    _time_opt(v, "start_time", "voyage", errs)
    _time_opt(v, "end_time", "voyage", errs)
    if v.get("voyage_type"):
        _enum(v, "voyage_type", VALID_VOYAGE_TYPES, "voyage", errs)

    # voyage_slug sanity (prefix check)
    vslug = (v.get("voyage_slug") or "").strip()
    sd = (v.get("start_date") or "").strip()
    pres_full = (v.get("president") or "").strip().lower()
    full_to_slug = _read_pres_fullname_to_slug()
    expected_pres_slug = (v.get("president_slug") or "").strip().lower() or full_to_slug.get(pres_full, slugify(pres_full) if pres_full else "")
    if sd and expected_pres_slug:
        expected_prefix = f"{sd}-{expected_pres_slug}-"
        if not vslug.startswith(expected_prefix):
            errs.append(f"[voyage] voyage_slug should start with '{expected_prefix}' (got '{vslug}')")

    # Passengers
    for i, p in enumerate(ppl, start=1):
        path = f"passengers #{i}"
        ps = (p.get("slug") or p.get("person_slug") or "").strip()
        if ps and not PERSON_SLUG_RE.match(ps):
            errs.append(f"[{path}] invalid person slug: {ps}")
        for field in ("birth_year", "death_year"):
            val = (p.get(field) or "").strip()
            if val and not val.isdigit():
                errs.append(f"[{path}] {field} must be integer if provided")

    # Media (title optional; must have credit + date + link)
    for i, m in enumerate(med, start=1):
        path = f"media #{i}"
        for k in ("credit", "date", "google_drive_link"):
            if not (m.get(k) or "").strip():
                errs.append(f"[{path}] missing required field: {k}")
        if (m.get("date") or "").strip() and not DATE_RE_FLEX.match(m.get("date")):
            errs.append(f"[{path}] invalid date for date: {m.get('date')} (YYYY / YYYY-MM / YYYY-MM-DD)")
        link = (m.get("google_drive_link") or "").strip()
        if link and not _is_supported_media_link(link):
            errs.append(f"[{path}] media link must be a Google Drive '/file/d/<ID>/...' or a Dropbox shared link")

        mslug = (m.get("slug") or "").strip()
        if mslug and vslug and (m.get("date") or "").strip():
            tmpl = MEDIA_SLUG_RE_TMPL.format(date=re.escape((m.get("date") or "").strip()), vslug=re.escape(vslug))
            if not re.match(tmpl, mslug):
                errs.append(f"[{path}] media slug '{mslug}' does not match '<date>-<source>-{vslug}-NN'")

    return errs
