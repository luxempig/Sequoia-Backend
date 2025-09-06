# ============================================
# voyage-ingest/tools/extract_drive_dictionary.py
# ============================================
"""
Build a dictionary mapping:
    Google Drive URL -> [name, date]

Rules:
- If label looks like: 1933.04.24_Wilmington_Daily_Press_Journal_pg1.jpg
    => name = "Wilmington_Daily_Press_Journal_pg1"
       date = "1933.04.24"
- If label looks like: Sequoia Logbook 1933 (p 9)
    => name = "Sequoia Logbook p9"
       date = "1933"
- If label looks like: YYYY.MM.DD Some Name
    => name = "Some Name" (slashes removed)
       date = "YYYY.MM.DD"
- If label looks like: YYYY.MM Some Name
    => name = "Some Name"
       date = "YYYY.MM"
- If label looks like: YYYY Some Name
    => name = "Some Name"
       date = "YYYY"
- Otherwise:
    => name = label (extension removed if looks like filename)
       date = ""

Usage:
    python extract_drive_dictionary.py input.md output.json
"""

import re
import sys
import os
import json
from typing import Dict, List, Tuple

# Basic markdown link: [label](url)
LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

# Helpers
def strip_ext(s: str) -> str:
    """Remove a single trailing file extension like '.jpg' or '.pdf'."""
    base = s.rsplit("/", 1)[-1]  # drop any path-ish bits in label
    if "." in base:
        # only strip the last dot segment (e.g., .jpg)
        return ".".join(base.split(".")[:-1]) if base.count(".") >= 1 else base
    return base

def remove_slashes(s: str) -> str:
    no_forward = s.replace("/", "_").strip()
    no_slashes = no_forward.replace("\\", "_").strip()
    no_periods = no_slashes.replace(".", "_").strip()
    no_colons = no_periods.replace(":", "_").strip()
    no_stars = no_colons.replace("*", "_").strip()
    no_question = no_stars.replace("?", "_").strip()
    no_quotes = no_question.replace('"', "_").strip()
    no_angle = no_quotes.replace("<", "_").replace(">", "_").strip()
    no_pipe = no_angle.replace("|", "_").strip()
    no_double = re.sub(r"__+", "_", no_pipe)
    no_space = no_double.replace(" ", "_").strip()
    return no_space

def parse_label_to_name_date(label: str) -> Tuple[str, str]:
    """
    Return (name, date) per the new rules.
    """
    lab = label.strip()

    # --- Case A: "Sequoia Logbook 1933 (p 9)" or "Sequoia Logbook 1933 (pg 9)" or "(page 9)"
    m = re.match(r"^(Sequoia\s+Logbook)\s+(\d{4})\s*\(\s*p(?:age|g)?\s*([0-9]+)\s*\)?", lab, re.I)
    if m:
        title, year, page = m.groups()
        name = f"{title} p{page}"
        date = year.replace(".", "-").strip()

        name = name if name else "unknown"
        date = date if date else ""

        return name, date

    # --- Case B: Filename-style: "YYYY.MM.DD_Some_Name.ext"
    m = re.match(r"^(\d{4})[.\-_/](\d{1,2})[.\-_/](\d{1,2})[_\-\. ](.+)$", lab)
    if m:
        y, mo, d, rest = m.groups()
        date = f"{y}.{int(mo):02d}.{int(d):02d}"
        date = date.replace(".", "-").strip()
        rest = strip_ext(rest).strip()
        return remove_slashes(rest), date

    # --- Case C: "YYYY.MM Some Name"
    m = re.match(r"^(\d{4})[.\-_/](\d{1,2})\s+(.+)$", lab)
    if m:
        y, mo, rest = m.groups()
        date = f"{y}.{int(mo):02d}"
        date = date.replace(".", "-").strip()
        rest = strip_ext(rest).strip()
        return remove_slashes(rest), date

    # --- Case D: "YYYY Some Name"
    m = re.match(r"^(\d{4})\s+(.+)$", lab)
    if m:
        y, rest = m.groups()
        date = y
        date = date.replace(".", "-").strip()
        rest = strip_ext(rest).strip()
        return remove_slashes(rest), date

    # --- Case E: Pure filename where a leading date is stuck with underscore, e.g.
    # "1933.04.24_Wilmington_Daily_Press_Journal_pg1.jpg" (already covered in B),
    # but catch variants like "1933-04-24 Wilmington Daily....pdf"
    m = re.match(r"^(\d{4})[.\-_/](\d{1,2})[.\-_/](\d{1,2})\s+(.+)$", lab)
    if m:
        y, mo, d, rest = m.groups()
        date = f"{y}.{int(mo):02d}.{int(d):02d}"
        date = date.replace(".", "-").strip()
        rest = strip_ext(rest).strip()
        return remove_slashes(rest), date

    # Fallback: just return label (sans extension) as name; date empty
    return remove_slashes(strip_ext(lab)), ""

def build_drive_dict(md_text: str) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for m in LINK_RE.finditer(md_text):
        label = m.group(1).strip()
        url = m.group(2).strip()
        if "drive.google.com" in url:
            name, date = parse_label_to_name_date(label)
            date = date.replace(".", "-").strip()
            out[url] = [name, date]
    return out

def main():
    if len(sys.argv) != 3:
        print("Usage: python extract_drive_dictionary.py input.md output.json", file=sys.stderr)
        sys.exit(1)

    input_path, output_path = sys.argv[1], sys.argv[2]
    if not os.path.isfile(input_path):
        print(f"Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    with open(input_path, "r", encoding="utf-8") as f:
        md_text = f.read()

    mapping = build_drive_dict(md_text)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(mapping)} Drive links to {output_path}")

if __name__ == "__main__":
    main()
