# ============================================
# voyage-ingest/tools/extract_drive_dictionary.py
# ============================================
"""
Extract mapping of Google Drive links -> (date, credit) from an ingestable markdown file.

Usage:
    python extract_drive_dictionary.py input.md output.json

- input.md : ingestable markdown (the same format used for voyage-ingest)
- output.json : file where dictionary will be written
"""

import re
import sys
import json
from typing import Dict, Tuple

# Regex for markdown link: [label](url)
LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

def normalize_date(y: str, m: str = None, d: str = None) -> str:
    if not y:
        return ""
    if m and d:
        return f"{y}-{int(m):02d}-{int(d):02d}"
    if m:
        return f"{y}-{int(m):02d}"
    return y

def parse_label(label: str) -> Tuple[str, str]:
    """
    Parse the label inside [...] to return (date, credit).
    """
    label = label.strip()

    # Case 1: Sequoia Logbook YEAR (page N)
    m = re.match(r"^Sequoia\s+Logbook\s+(\d{4})\s*\(page\s*([0-9]+)\)", label, re.I)
    if m:
        year, page = m.groups()
        return year, f"Sequoia Logbook page {page}"

    # Case 2: YYYY.MM.DD something
    m = re.match(r"^(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})\s+(.*)", label)
    if m:
        y, mo, d, rest = m.groups()
        return normalize_date(y, mo, d), rest.strip().replace("/", " ")

    # Case 3: YYYY.MM something
    m = re.match(r"^(\d{4})[.\-/](\d{1,2})\s+(.*)", label)
    if m:
        y, mo, rest = m.groups()
        return normalize_date(y, mo), rest.strip().replace("/", " ")

    # Case 4: Year only
    m = re.match(r"^(\d{4})\s+(.*)", label)
    if m:
        y, rest = m.groups()
        return normalize_date(y), rest.strip().replace("/", " ")

    # Fallback: no parse
    return "", label.replace("/", " ")

def build_drive_dict(md_text: str) -> Dict[str, Tuple[str, str]]:
    out: Dict[str, Tuple[str, str]] = {}
    for m in LINK_RE.finditer(md_text):
        label, url = m.group(1).strip(), m.group(2).strip()
        if "drive.google.com" in url:
            date, credit = parse_label(label)
            out[url] = (date, credit)
    return out

def main():
    print("doing main")
    if len(sys.argv) != 3:
        print("Usage: python extract_drive_dictionary.py input.md output.json", file=sys.stderr)
        sys.exit(1)

    input_path, output_path = sys.argv[1], sys.argv[2]
    with open(input_path, "r", encoding="utf-8") as f:
        md_text = f.read()

    drive_dict = build_drive_dict(md_text)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(drive_dict, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(drive_dict)} drive links to {output_path}")

if __name__ == "__main__":
    main()
