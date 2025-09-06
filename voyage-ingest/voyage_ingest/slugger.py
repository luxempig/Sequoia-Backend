from __future__ import annotations
import os
import re
from typing import Dict, Iterable, List, Tuple, Set

_slug_re = re.compile(r"[^a-z0-9]+")
_DATE_PREFIX = re.compile(r"^(\d{4})(?:-(\d{2})-(\d{2}))?$")

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
        "sequoia-logbook-p": "sequoia-logbook",  # common cleanup when pX becomes part of slug
    }
    # also fold "sequoia-logbook-p5" => "sequoia-logbook"
    if s.startswith("sequoia-logbook-p"):
        return "sequoia-logbook"
    return aliases.get(s, s)

def _read_president_slugs_from_env_sheet() -> Set[str]:
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
    out: Set[str] = set()
    for row in values[1:]:
        if i_slug < len(row):
            s = (row[i_slug] or "").strip().lower()
            if s:
                out.add(s)
    return out

def generate_voyage_slug(start_date: str, president_slug: str, title: str) -> str:
    first5 = "-".join(slugify(title).split("-")[:5]) or "voyage"
    return f"{start_date}-{slugify(president_slug)}-{first5}"

def generate_media_slugs(items: List[dict], voyage_slug: str) -> None:
    """
    For each media dict, fill m['slug'] if missing:
      <date-or-year>-<source_slug>-<voyage_slug>-NN
    'date' may be 'YYYY' or 'YYYY-MM-DD'. Slugging is lenient.
    """
    counters: Dict[Tuple[str, str, str], int] = {}
    for m in items:
        if m.get("slug"):
            continue
        date = (m.get("date") or "").strip()
        credit = (m.get("credit") or "").strip()
        src = normalize_source(credit) or "unknown-source"
        # No date? use 'unknown' (try to avoid, but supported)
        dkey = date if date else "unknown"
        key = (dkey, src, voyage_slug)
        counters[key] = counters.get(key, 0) + 1
        nn = f"{counters[key]:02d}"
        m["source_slug"] = src
        m["slug"] = f"{dkey}-{src}-{voyage_slug}-{nn}"

# Parse the president slug segment from voyage_slug
def president_from_voyage_slug(voyage_slug: str) -> str:
    s = (voyage_slug or "").strip().lower()
    if not s or len(s) < 12 or s[4] != "-" or s[7] != "-" or s[10] != "-":
        return "unknown-president"
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
    # fallback: first token
    return rest.split("-", 1)[0] if "-" in rest else rest or "unknown-president"
