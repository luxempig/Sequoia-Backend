#!/usr/bin/env python3
"""
Fix passenger slugs in a Google Doc:
- In every ## Passengers block
- For each line like `- slug: SOMETHING`
- If SOMETHING does not contain a "-", append "-firstname"

Config (voyage-ingest/.env):
  GOOGLE_APPLICATION_CREDENTIALS=./keys/service_credentials.json
  FIX_DOC_ID=<your-google-doc-id>

Run:
  python -m tools.fix_passenger_slugs
"""

from __future__ import annotations
import os
import re
import sys
from typing import List, Tuple
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
    max_end = 1
    for seg in (doc_meta.get("body", {}) or {}).get("content", []) or []:
        ei = seg.get("endIndex")
        if isinstance(ei, int) and ei > max_end:
            max_end = ei
    return max_end

def _replace_entire_body(doc_id: str, new_text: str):
    svc = _docs_service()
    meta = svc.documents().get(documentId=doc_id).execute()
    end_idx = _doc_end_index(meta)
    requests = [
        {"deleteContentRange": {"range": {"startIndex": 1, "endIndex": end_idx - 1}}},
        {"insertText": {"location": {"index": 1}, "text": new_text}},
    ]
    svc.documents().batchUpdate(documentId=doc_id, body={"requests": requests}).execute()

# -------- Slug fixing --------

SLUG_LINE_RE = re.compile(r"^(\s*-\s+slug:\s*)([A-Za-z0-9_]+)(\s*)$")

def _fix_passenger_slugs(doc_text: str) -> str:
    new_lines: List[str] = []
    for ln in doc_text.splitlines(keepends=True):
        m = SLUG_LINE_RE.match(ln)
        if m:
            prefix, slug, suffix = m.groups()
            if "-" not in slug:
                slug = slug + "-firstname"
            ln = f"{prefix}{slug}{suffix}\n"
        new_lines.append(ln)
    return "".join(new_lines)

# -------- Main --------

def main():
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    doc_id = os.environ.get("FIX_DOC_ID", "").strip()
    if not doc_id:
        print("ERROR: FIX_DOC_ID is not set in voyage-ingest/.env", file=sys.stderr)
        sys.exit(1)

    original, _meta = _read_doc_as_text(doc_id)
    updated = _fix_passenger_slugs(original)

    if updated == original:
        print("No changes needed: all passenger slugs already valid.")
        return

    _replace_entire_body(doc_id, updated)
    print("Passenger slugs fixed and document updated successfully.")

if __name__ == "__main__":
    main()
