from __future__ import annotations
import re
from typing import Iterable

_slug_re = re.compile(r"[^a-z0-9]+")

def slugify(text: str) -> str:
    s = (text or "").lower()
    s = _slug_re.sub("-", s).strip("-")
    s = re.sub(r"-{2,}", "-", s)
    return s or "unknown"

def normalize_source(credit: str) -> str:
    """
    Map a human credit string to a stable source slug.
    Add custom mappings here if you have canonical sources.
    """
    raw = (credit or "").strip()
    if not raw:
        return "unknown-source"
    s = slugify(raw)
    # common normalizations
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
      <date>-<source_slug>-<voyage_slug>-NN
    Ensures NN is sequential per (date, source_slug, voyage_slug) within this run.
    """
    counters: dict[tuple[str, str, str], int] = {}
    for m in items:
        if m.get("slug"):
            continue  # already set (backward compatibility)
        date = (m.get("date") or "").strip()
        credit = (m.get("credit") or "").strip()
        if not date:
            raise ValueError("media item missing required 'date' for slug generation")
        if not credit:
            raise ValueError("media item missing required 'credit' for slug generation")
        src = normalize_source(credit)
        key = (date, src, voyage_slug)
        counters[key] = counters.get(key, 0) + 1
        nn = f"{counters[key]:02d}"
        m["source_slug"] = src  # persist on item for downstream use
        m["slug"] = f"{date}-{src}-{voyage_slug}-{nn}"

# voyage_ingest/utils_slug.py
import re

_VOY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}-([a-z0-9-]+)-[a-z0-9-]+$")

def president_from_voyage_slug(voyage_slug: str) -> str:
    """
    Extract 'president-lastname' from 'YYYY-MM-DD-<president>-<descriptor>'.
    Returns 'unknown-president' if it can't parse.
    """
    m = _VOY_RE.match((voyage_slug or "").strip())
    if not m:
        return "unknown-president"
    return m.group(1)
