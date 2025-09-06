#!/usr/bin/env python3
"""
Fix Media items in a Google Doc:
- Remove `title:` from each media item
- Ensure each item begins with `- credit: ...`
- Reorder remaining keys to: credit, date, google_drive_link, description, tags
- Preserve any extra keys by appending them after the known ones
- Write the corrected markdown back to the same Doc

Config (voyage-ingest/.env):
  GOOGLE_APPLICATION_CREDENTIALS=./keys/service_credentials.json
  FIX_DOC_ID=<your-google-doc-id>

Run:
  python -m tools.fix_media
"""

from __future__ import annotations

import os
import re
import sys
from typing import List, Dict, Tuple, Optional

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

DOCS_SCOPES = ["https://www.googleapis.com/auth/documents"]

def _docs_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=DOCS_SCOPES
    )
    return build("docs", "v1", credentials=creds, cache_discovery=False)

def _read_doc_as_text(doc_id: str) -> Tuple[str, dict]:
    svc = _docs_service()
    doc = svc.documents().get(documentId=doc_id).execute()
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
    return "".join(chunks), doc

def _doc_end_index(doc_meta: dict) -> int:
    """
    Compute the actual end index of the document body.
    """
    max_end = 1
    for seg in (doc_meta.get("body", {}) or {}).get("content", []) or []:
        ei = seg.get("endIndex")
        if isinstance(ei, int) and ei > max_end:
            max_end = ei
    return max_end

def _replace_entire_body(doc_id: str, new_text: str):
    svc = _docs_service()
    # Get real end index
    meta = svc.documents().get(documentId=doc_id).execute()
    end_idx = _doc_end_index(meta)
    # Delete then insert
    requests = [
        {"deleteContentRange": {"range": {"startIndex": 1, "endIndex": end_idx - 1}}},
        {"insertText": {"location": {"index": 1}, "text": new_text}},
    ]
    svc.documents().batchUpdate(documentId=doc_id, body={"requests": requests}).execute()

MEDIA_HEADER_RE = re.compile(r"^\s*##\s*Media\s*$", re.IGNORECASE)
SECTION_HEADER_RE = re.compile(r"^\s*##\s+[^\n]+$")
ITEM_START_RE = re.compile(r"^\s*-\s+(.+)$")
FIELD_RE = re.compile(r"^\s{2,}([a-zA-Z0-9_]+)\s*:\s*(.*)$")

ORDER = ["credit", "date", "google_drive_link", "description", "tags"]

def _split_sections(lines: List[str]) -> List[List[str]]:
    sections: List[List[str]] = []
    current: List[str] = []
    for ln in lines:
        if ln.strip().startswith("## "):
            if current:
                sections.append(current)
                current = []
        current.append(ln)
    if current:
        sections.append(current)
    return sections

def _process_media_block(block_lines: List[str]) -> List[str]:
    out: List[str] = []
    i = 0
    if i < len(block_lines) and MEDIA_HEADER_RE.match(block_lines[i]):
        out.append(block_lines[i])
        i += 1

    items: List[Dict[str, str]] = []
    cur_item: Optional[Dict[str, str]] = None

    def flush_item():
        if cur_item is not None:
            items.append(cur_item.copy())

    while i < len(block_lines):
        ln = block_lines[i]
        if SECTION_HEADER_RE.match(ln) and not MEDIA_HEADER_RE.match(ln):
            break

        m_item = ITEM_START_RE.match(ln)
        m_field = FIELD_RE.match(ln)

        if m_item:
            flush_item()
            cur_item = {}
            rem = m_item.group(1).strip()
            if ":" in rem:
                k, v = rem.split(":", 1)
                cur_item[k.strip().lower()] = v.strip()
        elif m_field and cur_item is not None:
            k = m_field.group(1).strip().lower()
            v = m_field.group(2).rstrip()
            cur_item[k] = v
        i += 1

    flush_item()

    for item in items:
        item.pop("title", None)

        credit_val = item.get("credit", "").strip()
        out.append(f"- credit: {credit_val}\n")

        for key in ORDER[1:]:
            if key == "date":
                out.append(f"  date: {item.get('date', '').strip()}\n")
            elif key == "google_drive_link":
                out.append(f"  google_drive_link: {item.get('google_drive_link', '').strip()}\n")
            elif key == "description":
                out.append(f"  description: {item.get('description', '').strip()}\n")
            elif key == "tags":
                out.append(f"  tags: {item.get('tags', '').strip()}\n")

        extras = [k for k in item.keys() if k not in ORDER and k != "title"]
        for k in sorted(extras):
            out.append(f"  {k}: {item.get(k, '').strip()}\n")

    return out

def _rewrite_media_sections(doc_text: str) -> str:
    lines = doc_text.splitlines(keepends=True)
    sections = _split_sections(lines)

    new_lines: List[str] = []
    for sec in sections:
        if sec and MEDIA_HEADER_RE.match(sec[0]):
            rewritten = _process_media_block(sec)
            new_lines.extend(rewritten)
        else:
            new_lines.extend(sec)

    return "".join(new_lines)

def main():
    # Load .env from repo root (voyage-ingest/.env)
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

    doc_id = os.environ.get("FIX_DOC_ID", "").strip()
    if not doc_id:
        print("ERROR: FIX_DOC_ID is not set in voyage-ingest/.env", file=sys.stderr)
        sys.exit(1)

    original, _doc_meta = _read_doc_as_text(doc_id)
    updated = _rewrite_media_sections(original)

    if updated == original:
        print("No changes detected; document left unchanged.")
        return

    _replace_entire_body(doc_id, updated)
    print("Media fields cleaned and reordered. Document updated successfully.")

if __name__ == "__main__":
    main()
