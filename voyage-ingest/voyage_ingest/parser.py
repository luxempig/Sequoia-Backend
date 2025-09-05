from __future__ import annotations

import os
import re
import logging
from typing import Dict, List, Tuple, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

from voyage_ingest.slugger import generate_media_slugs, slugify

LOG = logging.getLogger("voyage_ingest.parser")

DOCS_SCOPES = ["https://www.googleapis.com/auth/documents.readonly"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# -------- Google Docs helpers --------

def _docs_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=DOCS_SCOPES)
    return build("docs", "v1", credentials=creds)

def _sheets_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds)

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

# -------- Minimal “YAML-ish” line parser used in our doc format --------

def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")

def _split_entries_block(lines: List[str]) -> List[List[str]]:
    entries: List[List[str]] = []
    cur: List[str] = []
    for ln in lines:
        if ln.strip().startswith("- "):
            if cur:
                entries.append(cur)
                cur = []
            cur.append(ln.strip()[2:])
        elif ln.startswith("  ") or ln.startswith("\t"):
            cur.append(ln.strip())
        elif ln.strip() == "":
            cur.append("")
        else:
            if cur:
                entries.append(cur)
                cur = []
    if cur:
        entries.append(cur)
    return entries

def _parse_kv_block(lines: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    i = 0
    while i < len(lines):
        raw = lines[i]
        s = raw.strip()
        if not s:
            i += 1
            continue
        if ":" not in s:
            i += 1
            continue
        key, rest = s.split(":", 1)
        key = key.strip()
        val = rest.strip()
        if val == "|":
            i += 1
            buf: List[str] = []
            while i < len(lines):
                nxt = lines[i]
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
    return out

# -------- Top-level doc parser --------

def _partition_into_voyages(doc_text: str) -> List[str]:
    """
    Split by each '## Voyage' section as one voyage block.
    """
    lines = doc_text.splitlines()
    starts = [i for i, ln in enumerate(lines) if ln.strip() == "## Voyage"]
    voyages: List[str] = []
    for idx, s in enumerate(starts):
        e = starts[idx + 1] if idx + 1 < len(starts) else len(lines)
        voyages.append("\n".join(lines[s:e]).strip())
    return voyages

def _extract_section(block: str, header: str) -> List[str]:
    lines = block.splitlines()
    out: List[str] = []
    in_sec = False
    for ln in lines:
        if ln.strip() == f"## {header}":
            in_sec = True
            continue
        if in_sec:
            if ln.strip() == "---" or ln.strip().startswith("## "):
                break
            out.append(ln)
    while out and out[0].strip() == "":
        out.pop(0)
    while out and out[-1].strip() == "":
        out.pop()
    return out

# -------- Presidents helpers (sheet) --------

def _read_presidents_fullname_to_slug(spreadsheet_id: str) -> Dict[str, str]:
    """
    Reads 'presidents' tab and maps lower(full_name) -> president_slug.
    Accepts headers: full_name, president_slug (case-insensitive).
    """
    svc = _sheets_service()
    title = os.environ.get("PRESIDENTS_SHEET_TITLE", "presidents").strip() or "presidents"
    res = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!A:ZZ"
    ).execute()
    vals = res.get("values", []) or []
    if not vals:
        return {}
    header = [h.strip().lower() for h in vals[0]]
    try:
        i_full = header.index("full_name")
        i_slug = header.index("president_slug")
    except ValueError:
        return {}
    out: Dict[str, str] = {}
    for row in vals[1:]:
        full = (row[i_full] if i_full < len(row) else "").strip()
        slug = (row[i_slug] if i_slug < len(row) else "").strip()
        if full and slug:
            out[full.lower()] = slug
    return out

# -------- Main parse --------

def parse_doc_multi(doc_id: str) -> List[Dict]:
    """
    Returns a list of voyage bundles:
      {
        "voyage": { ... },            # includes president + computed voyage_slug
        "passengers": [ ... ],
        "media": [ ... ]
      }
    Media 'slug' is auto-generated later by slugger.generate_media_slugs().
    """
    text = _strip_bom(_read_doc_as_text(doc_id))
    voyage_blocks = _partition_into_voyages(text)
    bundles: List[Dict] = []

    # We need presidents mapping to compute voyage_slug
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()
    pres_map = _read_presidents_fullname_to_slug(spreadsheet_id) if spreadsheet_id else {}

    # For uniqueness (same owner + same day)
    counters: Dict[Tuple[str, str], int] = {}

    for vb in voyage_blocks:
        v_lines = _extract_section(vb, "Voyage")
        p_lines = _extract_section(vb, "Passengers")
        m_lines = _extract_section(vb, "Media")

        voyage = _parse_kv_block(v_lines)

        passengers: List[Dict] = []
        if p_lines:
            entries = _split_entries_block(p_lines)
            for ent in entries:
                passengers.append(_parse_kv_block(ent))

        media: List[Dict] = []
        if m_lines:
            entries = _split_entries_block(m_lines)
            for ent in entries:
                media.append(_parse_kv_block(ent))

        # ---- Compute voyage_slug from start_date + president full name
        sd = (voyage.get("start_date") or "").strip()
        pres_full = (voyage.get("president") or "").strip()
        title = (voyage.get("title") or "").strip()
        descriptor = slugify(title) or "voyage"
        pres_slug = pres_map.get(pres_full.lower(), slugify(pres_full) if pres_full else "unknown-president")

        base_slug = f"{sd}-{pres_slug}-{descriptor}" if sd and pres_slug else (voyage.get("voyage_slug") or "")
        if sd and pres_slug:
            key = (sd, pres_slug)
            counters[key] = counters.get(key, 0) + 1
            n = counters[key]
            voyage_slug = f"{base_slug}-{n:02d}" if n > 1 else base_slug
            voyage["voyage_slug"] = voyage_slug
            voyage["president_slug"] = pres_slug  # pass through for downstream join
        else:
            # fallback to existing slug if provided (validator will complain otherwise)
            voyage["voyage_slug"] = voyage.get("voyage_slug", "")

        # Generate media slugs in-place (requires 'date' and 'credit')
        if media:
            generate_media_slugs(media, voyage_slug=voyage.get("voyage_slug", ""))

        bundles.append({"voyage": voyage, "passengers": passengers, "media": media})

    LOG.info("Parsed %d voyage bundle(s) from doc %s", len(bundles), doc_id)
    return bundles
