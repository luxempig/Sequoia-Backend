"""
Update Google Sheets tabs to exactly match the Doc content.
We replace (or create) rows for voyages, passengers, media, and join tables.
Also reset 'presidents' from the doc-provided list.

Includes naive rate limiting + exponential backoff for API calls.
"""

import os
import time
import random
import logging
from typing import Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

LOG = logging.getLogger("voyage_ingest.sheets_updater")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------- Headers ----------
PRESIDENTS_SHEET_TITLE = os.environ.get("PRESIDENTS_SHEET_TITLE", "presidents").strip() or "presidents"
PRESIDENTS_HEADERS = [
    "president_slug","full_name","party","term_start","term_end","wikipedia_url","tags",
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

# ---------- Rate limit ----------
REQS_PER_MIN_THRESHOLD = int(os.environ.get("SHEETS_REQS_THRESHOLD_PER_MIN", "290"))
_RATE_WINDOW = 60.0
_req_count = 0
_window_start = time.time()

def _rate_count():
    global _req_count, _window_start
    now = time.time()
    if now - _window_start >= _RATE_WINDOW:
        _window_start = now
        _req_count = 0
    _req_count += 1
    if _req_count >= REQS_PER_MIN_THRESHOLD:
        LOG.warning("Sheets API nearing per-minute threshold; sleeping 60s.")
        time.sleep(60)
        _window_start = time.time()
        _req_count = 0

def _execute_with_backoff(call):
    attempt = 0
    while True:
        try:
            _rate_count()
            return call.execute(num_retries=0)
        except HttpError as e:
            status = getattr(e, "status_code", None)
            if status is None and hasattr(e, "resp"):
                status = getattr(e.resp, "status", None)
            if status in (429, 500, 502, 503, 504):
                attempt += 1
                if attempt > 8: raise
                sleep_s = (0.6 * (2 ** (attempt-1))) + random.uniform(0, 0.35)
                LOG.warning("Sheets API %s. Backoff %.2fs (attempt %d).", status, sleep_s, attempt)
                time.sleep(sleep_s)
            else:
                raise

def _svc():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    root = build("sheets", "v4", credentials=creds)
    return root.spreadsheets(), root.spreadsheets().values()

def _ensure_tab(spreadsheets, spreadsheet_id: str, title: str, headers: List[str]):
    meta = _execute_with_backoff(spreadsheets.get(spreadsheetId=spreadsheet_id))
    existing_titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if title not in existing_titles:
        _execute_with_backoff(spreadsheets.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests":[{"addSheet":{"properties":{"title":title,"gridProperties":{"frozenRowCount":1}}}}]}
        ))
    # set headers
    values = spreadsheets.values()
    _execute_with_backoff(values.update(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A1:{chr(64+len(headers))}1",
        valueInputOption="RAW",
        body={"values":[headers]}
    ))
    # Clear all body rows (replace content approach)
    _execute_with_backoff(spreadsheets.values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A2:ZZ"
    ))

def reset_presidents_sheet(spreadsheet_id: str, presidents: List[Dict]) -> None:
    spreadsheets, values = _svc()
    _ensure_tab(spreadsheets, spreadsheet_id, PRESIDENTS_SHEET_TITLE, PRESIDENTS_HEADERS)
    # write rows
    rows = []
    for p in presidents or []:
        rows.append([
            p.get("president_slug",""), p.get("full_name",""), p.get("party",""),
            p.get("term_start",""), p.get("term_end",""), p.get("wikipedia_url",""),
            p.get("tags",""),
        ])
    if rows:
        _execute_with_backoff(values.append(
            spreadsheetId=spreadsheet_id, range=f"{PRESIDENTS_SHEET_TITLE}!A:ZZ",
            valueInputOption="RAW", insertDataOption="INSERT_ROWS", body={"values": rows}
        ))

def reset_and_fill_sheets(spreadsheet_id: str, bundles: List[Dict]) -> None:
    spreadsheets, values = _svc()

    # Ensure tabs & clear
    _ensure_tab(spreadsheets, spreadsheet_id, "voyages", VOYAGES_HEADERS)
    _ensure_tab(spreadsheets, spreadsheet_id, "passengers", PASSENGERS_HEADERS)
    _ensure_tab(spreadsheets, spreadsheet_id, "media", MEDIA_HEADERS)
    _ensure_tab(spreadsheets, spreadsheet_id, "voyage_passengers", VOYAGE_PASSENGERS_HEADERS)
    _ensure_tab(spreadsheets, spreadsheet_id, "voyage_media", VOYAGE_MEDIA_HEADERS)
    _ensure_tab(spreadsheets, spreadsheet_id, "voyage_presidents", VOYAGE_PRESIDENTS_HEADERS)

    # Batches to append
    voyages_rows, people_rows, media_rows = [], [], []
    vp_rows, vm_rows, vpr_rows = [], [], []

    for b in bundles:
        v = b.get("voyage") or {}
        ppl = b.get("passengers") or []
        med = b.get("media") or []

        vslug = (v.get("voyage_slug") or "").strip()
        voyages_rows.append([
            vslug, v.get("title",""), v.get("start_date",""), v.get("end_date",""),
            v.get("start_time",""), v.get("end_time",""),
            v.get("origin",""), v.get("destination",""), v.get("vessel_name","USS Sequoia"),
            v.get("voyage_type",""), v.get("summary_markdown") or v.get("summary",""),
            v.get("notes_internal",""), v.get("source_urls",""), v.get("tags",""),
        ])

        # passengers
        for p in ppl:
            people_rows.append([
                p.get("slug") or p.get("person_slug",""), p.get("full_name",""),
                p.get("role_title",""), p.get("organization",""),
                p.get("birth_year",""), p.get("death_year",""),
                p.get("wikipedia_url",""), p.get("notes_internal",""), p.get("tags",""),
            ])
            vp_rows.append([vslug, p.get("slug") or p.get("person_slug",""), p.get("role_title","") or "Guest", ""])

        # media
        for m in med:
            mslug = m.get("slug","")
            # sort by trailing -NN if present
            sort = ""
            if mslug:
                parts = mslug.rsplit("-",1)
                if len(parts)==2 and parts[1].isdigit():
                    sort = parts[1]
            media_rows.append([
                mslug, m.get("title",""), m.get("media_type",""),
                m.get("s3_url",""), m.get("thumbnail_s3_url",""),
                m.get("credit",""), m.get("date",""),
                m.get("description_markdown") or m.get("description",""),
                m.get("tags",""), m.get("copyright_restrictions",""),
                m.get("google_drive_link",""),
            ])
            vm_rows.append([vslug, mslug, sort, ""])

        # voyage_presidents
        pres_slug = (v.get("president_slug") or "").strip()
        if pres_slug:
            vpr_rows.append([vslug, pres_slug, ""])

    # Append in batches
    def _append(title: str, rows: List[List[str]]):
        if not rows: return
        _execute_with_backoff(values.append(
            spreadsheetId=spreadsheet_id, range=f"{title}!A:ZZ",
            valueInputOption="RAW", insertDataOption="INSERT_ROWS", body={"values": rows}
        ))

    _append("voyages", voyages_rows)
    _append("passengers", people_rows)
    _append("media", media_rows)
    _append("voyage_passengers", vp_rows)
    _append("voyage_media", vm_rows)
    _append("voyage_presidents", vpr_rows)
