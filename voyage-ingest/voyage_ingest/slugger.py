# FILE: voyage-ingest/voyage_ingest/slugger.py
from __future__ import annotations
import os
import re
from typing import Dict

_slug_re = re.compile(r"[^a-z0-9]+")
# lenient tokenizer for the "date" piece used in media slugs
def _tokenize_date(s: str) -> str:
    """
    Convert any free-text date into a safe token for slugs.
    Examples:
      ""            -> "undated"
      "1933"        -> "1933"
      "1933-04-23"  -> "1933-04-23"
      "April 1933"  -> "april-1933"
      "about 1933?" -> "about-1933"
    """
    s = (s or "").strip().lower()
    if not s:
        return "undated"
    s = _slug_re.sub("-", s).strip("-")
    return s or "undated"

def slugify(text: str) -> str:
    s = (text or "").lower()
    s = _slug_re.sub("-", s).strip("-")
    s = re.sub(r"-{2,}", "-", s)
    return s or "unknown"

def normalize_source(credit: str) -> str:
    raw = (credit or "").strip()
    if not raw:
        return "unknown-source"
    s = slugify(raw)
    aliases = {
        "white-house": "white-house",
        "white-house-photographer": "white-house",
        "national-archives": "national-archives",
        "natl-archives": "national-archives",
        "cbs-news": "cbs-news",
        "new-york-times": "new-york-times",
    }
    return aliases.get(s, s)

def generate_media_slugs(items: list[dict], voyage_slug: str) -> None:
    """
    In-place: for each media dict, fill m['slug'] if absent using:
      <date_token>-<source_slug>-<voyage_slug>-NN

    - date_token is a *lenient* token from any free-text date (or 'undated' if empty).
    - Ensures NN is sequential per (date_token, source_slug, voyage_slug) within this run.
    """
    counters: Dict[tuple[str, str, str], int] = {}
    for m in items or []:
        if m.get("slug"):
            continue  # keep existing
        date_token = _tokenize_date(m.get("date") or "")
        src = normalize_source(m.get("credit") or "")
        key = (date_token, src, voyage_slug)
        counters[key] = counters.get(key, 0) + 1
        nn = f"{counters[key]:02d}"
        m["source_slug"] = src
        m["slug"] = f"{date_token}-{src}-{voyage_slug}-{nn}"

# --- utils: president extraction remains the same but robust to longer president slugs
def _read_president_slugs_from_env_sheet() -> set[str]:
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except Exception:
        return set()
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        return set()
    title = os.environ.get("PRESIDENTS_SHEET_TITLE", "presidents").strip() or "presidents"
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path or not os.path.exists(creds_path):
        return set()
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    try:
        res = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{title}!A:ZZ"
        ).execute()
    except Exception:
        return set()
    values = res.get("values") or []
    if not values:
        return set()
    header = [h.strip().lower() for h in values[0]]
    if "president_slug" not in header:
        return set()
    i_slug = header.index("president_slug")
    out = set()
    for row in values[1:]:
        if i_slug < len(row):
            s = (row[i_slug] or "").strip().lower()
            if s:
                out.add(s)
    return out

def president_from_voyage_slug(voyage_slug: str) -> str:
    """
    Extract the president slug from 'YYYY-MM-DD-<president_slug>-<descriptor...>'.
    If presidents sheet is available, match the longest known slug following the date-.
    Otherwise, fall back to taking the token immediately after the date-.
    """
    s = (voyage_slug or "").strip().lower()
    if len(s) < 12 or s[4] != "-" or s[7] != "-" or s[10] != "-":
        return "unknown-president"

    # after 'YYYY-MM-DD-'
    rest = s[11:]
    known = _read_president_slugs_from_env_sheet()
    if known:
        best = None
        for pres in known:
            if rest.startswith(pres + "-") or rest == pres:
                if best is None or len(pres) > len(best):
                    best = pres
        if best:
            return best
    # fallback: take first token
    return rest.split("-", 1)[0] if "-" in rest else (rest or "unknown-president")
