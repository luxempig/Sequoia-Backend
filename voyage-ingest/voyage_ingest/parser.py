from __future__ import annotations

import os
import re
import logging
from typing import Dict, List, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build

from voyage_ingest.slugger import generate_media_slugs

LOG = logging.getLogger("voyage_ingest.parser")

DOCS_SCOPES = ["https://www.googleapis.com/auth/documents.readonly"]

# -------- Google Docs helpers --------

def _docs_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=DOCS_SCOPES)
    return build("docs", "v1", credentials=creds)

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
    # Google Docs tends to add stray \n—keep them (the parser expects lines)
    return "".join(chunks)

# -------- Minimal “YAML-ish” line parser used in our doc format --------

def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")

def _split_entries_block(lines: List[str]) -> List[List[str]]:
    """
    Given a block like:
      - key: val
        sub: val
      - key: val
    return a list of list-of-lines for each entry (leading '- ' preserved only on first line).
    """
    entries: List[List[str]] = []
    cur: List[str] = []
    for ln in lines:
        if ln.strip().startswith("- "):
            if cur:
                entries.append(cur)
                cur = []
            cur.append(ln.strip()[2:])  # drop "- "
        elif ln.startswith("  ") or ln.startswith("\t"):
            cur.append(ln.strip())
        elif ln.strip() == "":
            # allow blank lines inside an entry
            cur.append("")
        else:
            # new section started: caller should stop earlier
            # but if it happens, finalize current and break
            if cur:
                entries.append(cur)
                cur = []
    if cur:
        entries.append(cur)
    return entries

def _parse_kv_block(lines: List[str]) -> Dict[str, str]:
    """
    Parse key: value pairs; supports multiline with `|` on the value line.
    """
    out: Dict[str, str] = {}
    i = 0
    while i < len(lines):
        raw = lines[i]
        s = raw.strip()
        if not s:
            i += 1
            continue
        if ":" not in s:
            # ignore non-kv lines here
            i += 1
            continue
        key, rest = s.split(":", 1)
        key = key.strip()
        val = rest.strip()
        if val == "|":
            # capture all indented lines until a non-indented KV or EOF
            i += 1
            buf: List[str] = []
            while i < len(lines):
                nxt = lines[i]
                if nxt.startswith("  ") or nxt.startswith("\t") or nxt.strip() == "":
                    # keep as-is (but strip single leading indent)
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

SECTION_SPLIT_RE = re.compile(r"^\s*---\s*$", re.MULTILINE)

def _partition_into_voyages(doc_text: str) -> List[str]:
    """
    The doc contains one or more voyages, each with sections separated by lines '---'.
    We split on lines with only '---' but keep internal '---' that separate the three sections.
    Strategy: find every '## Voyage' heading as a voyage start; slice until next '## Voyage' or EOF.
    """
    lines = doc_text.splitlines()
    starts = [i for i, ln in enumerate(lines) if ln.strip() == "## Voyage"]
    voyages: List[str] = []
    for idx, s in enumerate(starts):
        e = starts[idx + 1] if idx + 1 < len(starts) else len(lines)
        voyages.append("\n".join(lines[s:e]).strip())
    return voyages

def _extract_section(block: str, header: str) -> List[str]:
    """
    Extract the lines under a '## <header>' until the next '---' or next '## ' header.
    """
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
    # strip leading/trailing blank lines
    while out and out[0].strip() == "":
        out.pop(0)
    while out and out[-1].strip() == "":
        out.pop()
    return out

def parse_doc_multi(doc_id: str) -> List[Dict]:
    """
    Returns a list of voyage bundles:
      {
        "voyage": { ... },
        "passengers": [ { ... }, ... ],
        "media": [ { ... }, ... ]
      }
    Media 'slug' is auto-generated later by slugger.generate_media_slugs().
    """
    text = _strip_bom(_read_doc_as_text(doc_id))
    voyage_blocks = _partition_into_voyages(text)
    bundles: List[Dict] = []

    for vb in voyage_blocks:
        v_lines = _extract_section(vb, "Voyage")
        p_lines = _extract_section(vb, "Passengers")
        m_lines = _extract_section(vb, "Media")

        # Parse sections
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

        bundle = {"voyage": voyage, "passengers": passengers, "media": media}

        # Generate media slugs in-place (requires 'date' and 'credit')
        if media:
            generate_media_slugs(media, voyage_slug=voyage.get("voyage_slug", ""))

        bundles.append(bundle)

    LOG.info("Parsed %d voyage bundle(s) from doc %s", len(bundles), doc_id)
    return bundles
