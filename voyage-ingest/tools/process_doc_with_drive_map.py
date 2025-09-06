# ============================================
# voyage-ingest/tools/process_doc_with_drive_map.py
# ============================================
"""
Use a prebuilt Drive dictionary (Drive URL -> [name, date]) to:
  - Normalize media items in a Google Doc written in the "voyage-ingestable" format
  - Move any Dropbox-linked media out of the Media list into a "Non-Drive links" dump
  - For Drive-linked media, fill/overwrite credit/date from the dictionary

Env (in voyage-ingest/.env):
  GOOGLE_APPLICATION_CREDENTIALS=./keys/service_credentials.json
  FIX_DOC_ID=<your-google-doc-id>      # preferred
  # or
  FIX-DOC-ID=<your-google-doc-id>      # supported as fallback
  DRIVE_DICT_JSON=voyage-ingest/tools/drive_dict.json  # default if unset

Output (no CLI args needed):
  voyage-ingest/tools/output_fixed.md

Run:
  python voyage-ingest/tools/process_doc_with_drive_map.py
"""

from __future__ import annotations
import os
import re
import json
from typing import List, Dict, Tuple, Optional

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ---------- Google Docs helpers ----------

def _docs_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid path")
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=["https://www.googleapis.com/auth/documents.readonly"]
    )
    return build("docs", "v1", credentials=creds, cache_discovery=False)

def read_doc_plaintext(doc_id: str) -> str:
    svc = _docs_service()
    doc = svc.documents().get(documentId=doc_id).execute()
    content = doc.get("body", {}).get("content", [])
    out: List[str] = []
    for c in content:
        p = c.get("paragraph")
        if not p:
            continue
        for el in p.get("elements", []):
            t = el.get("textRun", {}).get("content")
            if t:
                out.append(t)
    return "".join(out)


# ---------- Parsing helpers for our ingestable format ----------

HEADER_RE = re.compile(r"^##\s+Voyage\s*$", re.IGNORECASE)
SECTION_RE = re.compile(r"^##\s+(Voyage|Passengers|Media)\s*$", re.IGNORECASE)

def split_into_voyage_blocks(text: str) -> List[str]:
    """Split whole doc text into voyage blocks, each beginning with '## Voyage'."""
    lines = text.splitlines()
    idxs = [i for i, ln in enumerate(lines) if HEADER_RE.match(ln.strip())]
    blocks: List[str] = []
    for j, start in enumerate(idxs):
        end = idxs[j + 1] if j + 1 < len(idxs) else len(lines)
        blocks.append("\n".join(lines[start:end]).rstrip() + "\n")
    return blocks if blocks else [text]

def extract_section(lines: List[str], name: str) -> Tuple[int, int]:
    """
    Return (start_idx, end_idx) of a section named `name` within a voyage block lines.
    If missing, returns (-1, -1).
    """
    n = len(lines)
    start = -1
    for i, ln in enumerate(lines):
        if ln.strip().lower() == f"## {name}".lower():
            start = i
            break
    if start < 0:
        return -1, -1
    end = n
    for j in range(start + 1, n):
        if SECTION_RE.match(lines[j].strip()) or (lines[j].strip().startswith("## ") and lines[j].strip() != f"## {name}"):
            end = j
            break
    return start, end

def split_media_entries(section_lines: List[str]) -> List[List[str]]:
    """
    Given lines of the Media section, split into entries:
      - title: ...
        credit: ...
        date: ...
        google_drive_link: ...
        tags: ...
    Each entry begins with '- ' at the start of a line.
    Returns a list of list-of-lines (including their indentation).
    """
    entries: List[List[str]] = []
    current: List[str] = []
    for ln in section_lines:
        if ln.lstrip().startswith("- "):
            # new entry
            if current:
                entries.append(current)
                current = []
            current.append(ln)
        else:
            if current:
                current.append(ln)
    if current:
        entries.append(current)
    return entries

def join_media_entries(entries: List[List[str]]) -> List[str]:
    out: List[str] = []
    for ent in entries:
        if not ent:
            continue
        # ensure blank line between entries is not mandatory; just add lines
        out.extend(ent)
    return out

def get_field_line_index(entry_lines: List[str], field: str) -> Optional[int]:
    f = f"{field}:"
    for i, ln in enumerate(entry_lines):
        if f in ln:
            # ensure we match key at start (after optional spaces and bullet)
            stripped = ln.strip()
            if stripped.startswith(f"- {field}:") or stripped.startswith(f"{field}:"):
                return i
    return None

def get_field_value(entry_lines: List[str], field: str) -> str:
    idx = get_field_line_index(entry_lines, field)
    if idx is None:
        return ""
    ln = entry_lines[idx]
    after = ln.split(":", 1)[1]
    return after.strip()

def set_field_value(entry_lines: List[str], field: str, value: str) -> List[str]:
    idx = get_field_line_index(entry_lines, field)
    key = f"{field}:"
    if idx is None:
        # insert after the first line if needed
        indent = "  "
        if entry_lines:
            entry_lines.insert(1, f"{indent}{key} {value}".rstrip())
        else:
            entry_lines.append(f"- {key} {value}".rstrip())
        return entry_lines
    # replace
    prefix = entry_lines[idx].split(":", 1)[0] + ":"
    # keep original indentation
    leading_ws = re.match(r"^(\s*)", entry_lines[idx]).group(1)
    entry_lines[idx] = f"{leading_ws}{prefix} {value}".rstrip()
    return entry_lines


# ---------- Drive dict helpers ----------

def load_drive_dict(path: str) -> Dict[str, List[str]]:
    if not os.path.exists(path):
        raise RuntimeError(f"Drive dictionary JSON not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # expect: { url: [name, date] }
    return {k: (v if isinstance(v, list) else ["", ""]) for k, v in data.items()}

def normalize_drive_url(u: str) -> str:
    return u.strip()


# ---------- Core transform ----------

def transform_block(block_text: str, drive_map: Dict[str, List[str]]) -> str:
    lines = block_text.splitlines()
    # Find Media section bounds
    s, e = extract_section(lines, "Media")
    if s < 0:
        return block_text  # nothing to do

    media_sec_lines = lines[s+1:e]  # lines inside Media
    entries = split_media_entries(media_sec_lines)

    kept_entries: List[List[str]] = []
    non_drive_links: List[str] = []

    for entry in entries:
        link = get_field_value(entry, "google_drive_link")
        link_lc = link.lower()

        # DropBox handling
        if "dropbox.com" in link_lc:
            if link:
                non_drive_links.append(link)
            # skip this entry (remove from media)
            continue

        # Drive handling
        if "drive.google.com" in link_lc:
            norm = normalize_drive_url(link)
            name, date = drive_map.get(norm, ["", ""])
            # Fill credit/date if we have them from map
            if name:
                entry = set_field_value(entry, "credit", name)
            if date:
                entry = set_field_value(entry, "date", date)
            kept_entries.append(entry)
        else:
            # keep unknown links, do not modify
            kept_entries.append(entry)

    # Rebuild the Media section
    new_media_lines = join_media_entries(kept_entries)

    # Prepare Non-Drive dump (append at end of voyage block)
    extra_dump: List[str] = []
    if non_drive_links:
        extra_dump.append("")
        extra_dump.append("<!-- Non-Drive links (moved from Media items with Dropbox URLs) -->")
        extra_dump.append("### Non-Drive links")
        for u in non_drive_links:
            extra_dump.append(f"- {u}")
        extra_dump.append("")

    # Reassemble block
    new_lines = lines[:s+1] + new_media_lines + lines[e:] + extra_dump
    return "\n".join(new_lines).rstrip() + "\n"


def transform_document(md_text: str, drive_map: Dict[str, List[str]]) -> str:
    blocks = split_into_voyage_blocks(md_text)
    out_blocks: List[str] = []
    for b in blocks:
        out_blocks.append(transform_block(b, drive_map))
    return "\n".join(out_blocks).rstrip() + "\n"


# ---------- Orchestration ----------

def main():
    load_dotenv()  # load voyage-ingest/.env

    # Where's the doc?
    doc_id = os.environ.get("FIX_DOC_ID") or os.environ.get("FIX-DOC-ID") or ""
    doc_id = (doc_id or "").strip()
    if not doc_id:
        raise RuntimeError("Missing FIX_DOC_ID (or FIX-DOC-ID) in .env")

    # Where's the drive dict?
    drive_json = os.environ.get("DRIVE_DICT_JSON", "voyage-ingest/tools/drive_dict.json").strip()

    # Output path
    out_md = os.environ.get("OUT_MD_PATH", "voyage-ingest/tools/output_fixed.md").strip()

    # 1) Load dictionary
    drive_map = load_drive_dict(drive_json)

    # 2) Read Google Doc plaintext (already in ingestable markdown-like format)
    md_text = read_doc_plaintext(doc_id)

    # 3) Transform
    result = transform_document(md_text, drive_map)

    # 4) Save
    os.makedirs(os.path.dirname(out_md), exist_ok=True)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(result)

    print(f"✅ Wrote transformed markdown to: {out_md}")
    print(f"   Drive dictionary entries: {len(drive_map)}")


if __name__ == "__main__":
    main()
