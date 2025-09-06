# ================================================================
# FILE: voyage-ingest/voyage_ingest/parser.py
# ================================================================
from __future__ import annotations

import os
import re
import logging
from typing import Dict, List, Tuple, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

from voyage_ingest.slugger import slugify

LOG = logging.getLogger("voyage_ingest.parser")

DOCS_SCOPES = ["https://www.googleapis.com/auth/documents.readonly"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# ---------------- Google APIs ----------------

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

# ---------------- Doc text helpers ----------------

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

def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")

# ---------------- Minimal YAML-ish parsers ----------------

def _split_entries_block(lines: List[str]) -> List[List[str]]:
    """
    For list-like blocks:
      - item starts with "- "
      - continued lines are indented
    """
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
    """
    Parse a simple key: value | key: | (multiline) block.
    """
    out: Dict[str, str] = {}
    i = 0
    while i < len(lines):
        raw = lines[i]
        s = raw.rstrip("\n")
        if not s.strip():
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
        else:
            out[key] = val
            i += 1
    return out

# ---------------- High-level structure parser ----------------

_HEADER_RE = re.compile(r"^\s*##\s+(President|Voyage|Passengers|Media)\s*$", re.IGNORECASE)

def _partition_sections(doc_text: str) -> List[Tuple[str, List[str]]]:
    """
    Returns a flat list of (section_name, section_lines) in the order they appear:
      ("President" | "Voyage" | "Passengers" | "Media", [lines...])
    """
    lines = doc_text.splitlines()
    sections: List[Tuple[str, List[str]]] = []
    current_name: Optional[str] = None
    current_lines: List[str] = []
    for ln in lines:
        m = _HEADER_RE.match(ln)
        if m:
            # flush previous
            if current_name is not None:
                sections.append((current_name, current_lines))
            current_name = m.group(1).title()  # normalize casing
            current_lines = []
        else:
            if current_name is not None:
                current_lines.append(ln)
    if current_name is not None:
        sections.append((current_name, current_lines))
    return sections

def _first_nonempty(s: str) -> str:
    return (s or "").strip()

def _ensure_pres_slug(p: Dict[str, str]) -> str:
    pres_slug = _first_nonempty(p.get("president_slug"))
    if pres_slug:
        return pres_slug
    # Derive from full_name if not present
    full = _first_nonempty(p.get("full_name"))
    if full:
        return slugify(full)
    return "unknown-president"

def _descriptor_from_title(title: str, max_words: int = 5) -> str:
    words = (title or "").strip().split()
    if not words:
        return "voyage"
    return slugify(" ".join(words[:max_words]))

def _read_presidents_fullname_to_slug(spreadsheet_id: str) -> Dict[str, str]:
    """
    If a presidents sheet exists, we can optionally map full_name->slug
    to keep slugs stable/identical to sheet. Best-effort only.
    """
    if not spreadsheet_id:
        return {}
    try:
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
        i_full = header.index("full_name") if "full_name" in header else -1
        i_slug = header.index("president_slug") if "president_slug" in header else -1
        if i_full < 0 or i_slug < 0:
            return {}
        out: Dict[str, str] = {}
        for r in vals[1:]:
            full = (r[i_full] if i_full < len(r) else "").strip()
            slug = (r[i_slug] if i_slug < len(r) else "").strip()
            if full and slug:
                out[full.lower()] = slug
        return out
    except Exception:
        return {}

# ---------------- Public parse ----------------

def parse_doc_multi(doc_id: str):
    """
    Returns (presidents, bundles)

    presidents: [
      { "president_slug","full_name","party","term_start","term_end","wikipedia_url","tags" },
      ...
    ]

    bundles: [
      {
        "voyage": {... includes 'president' and 'president_slug' and computed 'voyage_slug' ...},
        "passengers": [ ... ],
        "media": [ ... ],
      },
      ...
    ]
    """
    text = _strip_bom(_read_doc_as_text(doc_id))
    sections = _partition_sections(text)

    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()
    sheet_pres_map = _read_presidents_fullname_to_slug(spreadsheet_id)

    presidents: List[Dict[str, str]] = []
    bundles: List[Dict] = []

    current_pres: Optional[Dict[str, str]] = None
    current_voyage: Optional[Dict[str, str]] = None
    current_passengers: List[Dict] = []
    current_media: List[Dict] = []

    def _flush_voyage():
        nonlocal current_voyage, current_passengers, current_media
        if not current_voyage:
            return
        # attach president info
        if current_pres:
            current_voyage.setdefault("president", current_pres.get("full_name", ""))
            current_voyage.setdefault("president_slug", _ensure_pres_slug(current_pres))
        # compute voyage_slug if possible
        sd = _first_nonempty(current_voyage.get("start_date"))
        title = _first_nonempty(current_voyage.get("title"))
        pres_slug = _first_nonempty(current_voyage.get("president_slug"))
        if not pres_slug and current_voyage.get("president"):
            # try mapping from sheet
            pres_slug = sheet_pres_map.get(current_voyage["president"].lower(), slugify(current_voyage["president"]))
            current_voyage["president_slug"] = pres_slug

        if sd and pres_slug and title:
            descriptor = _descriptor_from_title(title, max_words=5)
            current_voyage["voyage_slug"] = f"{sd}-{pres_slug}-{descriptor}"
        # bundle
        bundles.append({
            "voyage": current_voyage,
            "passengers": current_passengers,
            "media": current_media,
        })
        # reset
        current_voyage = None
        current_passengers = []
        current_media = []

    for (name, lines) in sections:
        if name == "President":
            # starting a new president context flushes any open voyage
            _flush_voyage()

            pdata = _parse_kv_block(lines)
            # normalize keys that might appear differently
            # Expected in sheet: president_slug, full_name, party, term_start, term_end, wikipedia_url, tags
            full_name = _first_nonempty(pdata.get("full_name") or pdata.get("name") or pdata.get("president"))
            # if no explicit slug, try sheet mapping, else slugify
            pres_slug = _first_nonempty(pdata.get("president_slug"))
            if not pres_slug and full_name:
                pres_slug = sheet_pres_map.get(full_name.lower(), slugify(full_name))
            current_pres = {
                "president_slug": pres_slug or "unknown-president",
                "full_name": full_name or "",
                "party": _first_nonempty(pdata.get("party")),
                "term_start": _first_nonempty(pdata.get("term_start")),
                "term_end": _first_nonempty(pdata.get("term_end")),
                "wikipedia_url": _first_nonempty(pdata.get("wikipedia_url")),
                "tags": _first_nonempty(pdata.get("tags")),
            }
            # store/overwrite same pres_slug once (dedupe by slug)
            if current_pres["president_slug"] and not any(p.get("president_slug") == current_pres["president_slug"] for p in presidents):
                presidents.append(current_pres.copy())

        elif name == "Voyage":
            # flush any in-progress voyage
            _flush_voyage()
            v = _parse_kv_block(lines)
            # inject current president context immediately; _flush_voyage will ensure again
            if current_pres:
                v.setdefault("president", current_pres.get("full_name", ""))
                v.setdefault("president_slug", _ensure_pres_slug(current_pres))
            current_voyage = v

        elif name == "Passengers":
            # passengers block belongs to the current voyage
            # We accept either bullet-list entries or key:value lines; bullet entries can be simple "slug: ..., full_name: ..., role_title: ..."
            entries = _split_entries_block(lines)
            block: List[Dict] = []
            for ent in entries:
                block.append(_parse_kv_block(ent))
            if current_voyage is None:
                LOG.warning("Passengers block encountered with no active voyage; skipping.")
            else:
                current_passengers = block

        elif name == "Media":
            # media block belongs to the current voyage
            entries = _split_entries_block(lines)
            block: List[Dict] = []
            for ent in entries:
                block.append(_parse_kv_block(ent))
            if current_voyage is None:
                LOG.warning("Media block encountered with no active voyage; skipping.")
            else:
                current_media = block

    # flush tail
    _flush_voyage()

    LOG.info("Parsed %d president block(s), %d voyage bundle(s) from doc %s", len(presidents), len(bundles), doc_id)
    return presidents, bundles
