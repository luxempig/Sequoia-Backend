from __future__ import annotations

import os
import re
import logging
from typing import Dict, List, Tuple, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

from voyage_ingest.slugger import slugify, generate_media_slugs

LOG = logging.getLogger("voyage_ingest.parser")

DOCS_SCOPES = ["https://www.googleapis.com/auth/documents.readonly"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# -------- Google APIs --------

def _docs_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=DOCS_SCOPES)
    return build("docs", "v1", credentials=creds, cache_discovery=False)

def _sheets_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def _read_doc_as_text(doc_id: str) -> str:
    docs = _docs_service()
    doc = docs.documents().get(documentId=doc_id).execute()
    content = doc.get("body", {}).get("content", [])
    chunks: List[str] = []
    for c in content:
        para = c.get("paragraph")
        if not para:
            continue
        for el in para.get("elements", []):
            t = el.get("textRun", {}).get("content")
            if t:
                chunks.append(t)
    return "".join(chunks)

# -------- Sheets helpers (for president slug map) --------

def _read_presidents_fullname_to_slug(spreadsheet_id: str) -> Dict[str, str]:
    """
    Reads the 'presidents' tab and maps lower(full_name) -> president_slug.
    If the sheet/headers are missing, returns {} and we'll fall back to slugify(name).
    """
    if not spreadsheet_id:
        return {}
    svc = _sheets_service()
    title = os.environ.get("PRESIDENTS_SHEET_TITLE", "presidents").strip() or "presidents"
    try:
        res = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{title}!A:ZZ"
        ).execute()
    except Exception:
        return {}
    vals = res.get("values", []) or []
    if not vals:
        return {}
    header = [h.strip().lower() for h in vals[0]]
    if "full_name" not in header or "president_slug" not in header:
        return {}
    i_full = header.index("full_name")
    i_slug = header.index("president_slug")
    out: Dict[str, str] = {}
    for row in vals[1:]:
        full = (row[i_full] if i_full < len(row) else "").strip()
        slug = (row[i_slug] if i_slug < len(row) else "").strip()
        if full and slug:
            out[full.lower()] = slug
    return out

# -------- Mini “YAML-ish” helpers --------

def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")

def _consume_kv_block(lines: List[str], start_idx: int) -> Tuple[Dict[str, str], int]:
    """
    Consumes a simple key: value block from lines[start_idx:], stopping at a line of '---'
    or at a new section header starting with '## '. Returns (dict, next_index).
    Supports 'key: |' multi-line values (indented or blank lines until a new header/divider).
    """
    out: Dict[str, str] = {}
    i = start_idx
    n = len(lines)
    while i < n:
        raw = lines[i]
        s = raw.rstrip("\n")
        if s.strip() == "---":
            i += 1
            break
        if s.strip().startswith("## "):
            break
        if ":" not in s:
            i += 1
            continue
        key, rest = s.split(":", 1)
        key = key.strip()
        val = rest.strip()
        if val == "|":
            i += 1
            buf: List[str] = []
            while i < n:
                nxt = lines[i]
                if nxt.strip() == "---" or nxt.strip().startswith("## "):
                    break
                if nxt.startswith("  ") or nxt.startswith("\t") or nxt.strip() == "":
                    buf.append(nxt.lstrip())
                    i += 1
                else:
                    break
            out[key] = "\n".join(buf).rstrip()
            continue
        else:
            out[key] = val
            i += 1
    return out, i

def _consume_list_block(lines: List[str], start_idx: int) -> Tuple[List[List[str]], int]:
    """
    Consumes an indented list block like:
      - key: val
        key2: val2
      - key: val
    Stops at '---' or '## ' header. Returns (list_of_raw_lines_per_entry, next_index).
    """
    i = start_idx
    n = len(lines)
    entries: List[List[str]] = []
    cur: List[str] = []
    def _flush():
        nonlocal cur, entries
        if cur:
            entries.append(cur)
            cur = []
    while i < n:
        s = lines[i].rstrip("\n")
        if s.strip() == "---" or s.strip().startswith("## "):
            _flush()
            if s.strip() == "---":
                i += 1
            break
        if s.strip().startswith("- "):
            _flush()
            cur = [s.strip()[2:]]
            i += 1
            continue
        if s.startswith("  ") or s.startswith("\t") or s.strip() == "":
            cur.append(s.strip())
            i += 1
            continue
        # other text -> end of section
        _flush()
        break
    _flush()
    return entries, i

def _kv_from_lines(entry_lines: List[str]) -> Dict[str, str]:
    # Reuse the kv parser on a tiny block
    d, _ = _consume_kv_block(entry_lines, 0)
    return d

# -------- Slug helpers for voyage --------

def _first_five_words_slug(title: str) -> str:
    words = (title or "").split()
    five = words[:5]
    return slugify(" ".join(five)) or "voyage"

# -------- Main parser --------

def parse_doc_multi(doc_id: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Reads a Google Doc in the agreed “ingestable” format, supporting:
      ## President
        president_slug: ...
        full_name: ...
        ...
      ---
      ## Voyage
        title: ...
        start_date: ...
        ...
      ---
      ## Passengers
      - slug: ...
        full_name: ...
        ...
      ---
      ## Media
      - credit: ...
        date: ...
        google_drive_link: ...
        ...
      ---

    Behavior:
    - The last seen ## President header is applied to all following voyages until the next ## President.
    - voyage_slug is ALWAYS auto-generated: {start_date}-{president_slug}-{first-5-words-of-title}, with a unique counter per (date,president).
    - Returns (presidents, bundles).
    """
    text = _strip_bom(_read_doc_as_text(doc_id))
    lines = text.splitlines()

    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()
    pres_map = _read_presidents_fullname_to_slug(spreadsheet_id)

    presidents: List[Dict] = []
    bundles: List[Dict] = []

    current_president: Optional[Dict] = None
    # uniqueness counter: (start_date, president_slug) -> count
    v_counters: Dict[Tuple[str, str], int] = {}

    i = 0
    n = len(lines)
    while i < n:
        s = lines[i].strip()
        if s == "## President":
            # consume president block
            i += 1
            pres, i = _consume_kv_block(lines, i)
            # normalize & fill missing bits if needed
            full_name = (pres.get("full_name") or "").strip()
            pslug = (pres.get("president_slug") or "").strip()
            if not pslug and full_name:
                pslug = pres_map.get(full_name.lower(), slugify(full_name))
                pres["president_slug"] = pslug
            current_president = pres
            presidents.append(pres)
            continue

        if s == "## Voyage":
            if not current_president:
                LOG.warning("Voyage encountered before any President header; using 'unknown-president'")
                current_president = {
                    "president_slug": "unknown-president",
                    "full_name": "Unknown President",
                }
                presidents.append(current_president)

            i += 1
            voyage, i = _consume_kv_block(lines, i)
            # attach president context
            voyage["president"] = current_president.get("full_name", "")
            voyage["president_slug"] = current_president.get("president_slug", "unknown-president")
            # defaults
            if not voyage.get("vessel_name"):
                voyage["vessel_name"] = "USS Sequoia"

            # Passengers?
            passengers: List[Dict] = []
            if i < n and lines[i].strip() == "## Passengers":
                i += 1
                raw_entries, i = _consume_list_block(lines, i)
                for ent in raw_entries:
                    d = _kv_from_lines(ent)
                    if d.get("slug") or d.get("full_name"):
                        passengers.append(d)

            # Media?
            media: List[Dict] = []
            if i < n and lines[i].strip() == "## Media":
                i += 1
                raw_entries, i = _consume_list_block(lines, i)
                for ent in raw_entries:
                    d = _kv_from_lines(ent)
                    # Keep only items with at least a link + credit/date (validator will enforce further)
                    if d.get("google_drive_link") or d.get("dropbox_link"):
                        media.append(d)

            # compute voyage_slug
            sd = (voyage.get("start_date") or "").strip()
            pres_slug = (voyage.get("president_slug") or "").strip() or "unknown-president"
            title = (voyage.get("title") or "").strip()
            descriptor = _first_five_words_slug(title)
            base = "-".join([p for p in [sd, pres_slug, descriptor] if p])
            if not base:
                base = "unknown-unknown-untitled"

            key = (sd or "unknown", pres_slug)
            v_counters[key] = v_counters.get(key, 0) + 1
            nnn = v_counters[key]
            voyage_slug = f"{base}-{nnn:02d}" if nnn > 1 else base
            voyage["voyage_slug"] = voyage_slug

            # auto-generate media slugs (requires date & credit; if missing, slugger will raise)
            if media:
                try:
                    generate_media_slugs(media, voyage_slug=voyage_slug)
                except Exception as e:
                    LOG.warning("Media slug generation issue for %s: %s", voyage_slug, e)

            bundles.append({"voyage": voyage, "passengers": passengers, "media": media})
            continue

        # otherwise just move on
        i += 1

    LOG.info("Parsed %d president(s), %d voyage(s) from doc %s", len(presidents), len(bundles), doc_id)
    return presidents, bundles
