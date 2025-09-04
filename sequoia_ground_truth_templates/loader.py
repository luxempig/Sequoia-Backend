#!/usr/bin/env python3
"""
Idempotent loader for Sequoia Voyage Ground Truth CSVs -> Postgres.

- Validates required headers and enums
- Parses dates
- Upserts in FK-safe order inside a single transaction
- Provides DRY-RUN mode and a summary report

ENV VARS (fill in or use a .env loader):
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

USAGE:
  python loader.py --root ./templates --apply      # actually write to DB
  python loader.py --root ./templates --dry-run    # validate only
"""

import os, csv, sys, argparse, datetime
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

import psycopg2
import psycopg2.extras

REQUIRED = object()

# ---- Configuration: map CSVs -> target tables and columns ----
# Adjust these to match your actual Postgres schema if names differ.
CFG = {
    "voyages": {
        "csv": "voyages.csv",
        "table": "voyages",
        "pk": ("voyage_slug",),  # business key for upsert
        "cols": [
            ("voyage_slug", REQUIRED),
            ("title", REQUIRED),
            ("start_date", REQUIRED),
            ("end_date", None),
            ("origin", None),
            ("destination", None),
            ("vessel_name", None),
            ("voyage_type", None),
            ("summary_markdown", None),
            ("notes_internal", None),
            ("source_urls", None),
            ("tags", None),
        ],
        "enums": {
            "voyage_type": {"official", "private", "maintenance", "other", ""},
        },
        "defaults": {
            "vessel_name": "USS Sequoia",
        },
    },
    "passengers": {
        "csv": "passengers.csv",
        "table": "passengers",
        "pk": ("person_slug",),
        "cols": [
            ("person_slug", REQUIRED),
            ("full_name", REQUIRED),
            ("role_title", None),
            ("organization", None),
            ("birth_year", None),
            ("death_year", None),
            ("wikipedia_url", None),
            ("notes_internal", None),
            ("tags", None),
        ],
    },
    "presidents": {
        "csv": "presidents.csv",
        "table": "presidents",
        "pk": ("president_slug",),
        "cols": [
            ("president_slug", REQUIRED),
            ("full_name", REQUIRED),
            ("party", REQUIRED),
            ("term_start", REQUIRED),
            ("term_end", REQUIRED),
            ("wikipedia_url", None),
            ("notes_internal", None),
            ("tags", None),
        ],
    },
    "media": {
        "csv": "media.csv",
        "table": "media",
        "pk": ("media_slug",),
        "cols": [
            ("media_slug", REQUIRED),
            ("title", None),
            ("media_type", None),
            ("s3_url", REQUIRED),
            ("thumbnail_s3_url", None),
            ("credit", None),
            ("date", None),
            ("description_markdown", None),
            ("tags", None),
            ("copyright_restrictions", None),
        ],
        "enums": {
            "media_type": {"image", "pdf", "audio", "video", "other", ""},
        },
    },
    "voyage_passengers": {
        "csv": "voyage_passengers.csv",
        "table": "voyage_passengers",
        "pk": ("voyage_slug", "person_slug"),  # composite key okay for upsert
        "cols": [
            ("voyage_slug", REQUIRED),
            ("person_slug", REQUIRED),
            ("capacity_role", None),
            ("notes", None),
        ],
        "fk": [
            ("voyage_slug", "voyages", "voyage_slug"),
            ("person_slug", "passengers", "person_slug"),
        ],
    },
    "voyage_presidents": {
        "csv": "voyage_presidents.csv",
        "table": "voyage_presidents",
        "pk": ("voyage_slug", "president_slug"),
        "cols": [
            ("voyage_slug", REQUIRED),
            ("president_slug", REQUIRED),
            ("notes", None),
        ],
        "fk": [
            ("voyage_slug", "voyages", "voyage_slug"),
            ("president_slug", "presidents", "president_slug"),
        ],
    },
    "voyage_media": {
        "csv": "voyage_media.csv",
        "table": "voyage_media",
        "pk": ("voyage_slug", "media_slug"),
        "cols": [
            ("voyage_slug", REQUIRED),
            ("media_slug", REQUIRED),
            ("sort_order", None),
            ("notes", None),
        ],
        "fk": [
            ("voyage_slug", "voyages", "voyage_slug"),
            ("media_slug", "media", "media_slug"),
        ],
    },
}

DATE_FIELDS = {"start_date", "end_date", "term_start", "term_end", "date"}

def parse_date(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            return datetime.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Invalid date '{s}' — expected YYYY-MM-DD")

def parse_int(s: str) -> Optional[int]:
    s = (s or "").strip()
    if not s:
        return None
    return int(s)

def validate_enum(field: str, value: str, allowed: set):
    if value not in allowed:
        raise ValueError(f"Invalid value for {field}: '{value}' (allowed: {sorted(list(allowed))})")

def read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [row for row in reader]

def ensure_headers(rows: List[Dict[str, str]], expected_cols: List[str], csv_name: str):
    if not rows:
        # Still ensure header presence by checking reader.fieldnames from a re-open if needed
        pass
    # DictReader ensures headers exist; validate presence
    if rows:
        present = set(rows[0].keys())
        missing = [c for c in expected_cols if c not in present]
        if missing:
            raise ValueError(f"{csv_name}: missing headers: {missing}")

def to_python(row: Dict[str, str]) -> Dict[str, object]:
    out = {}
    for k, v in row.items():
        if k in DATE_FIELDS:
            out[k] = parse_date(v)
        elif k in ("birth_year", "death_year", "sort_order"):
            out[k] = parse_int(v)
        else:
            out[k] = (v or "").strip()
    return out

def build_upsert_sql(table: str, pk_cols: Tuple[str, ...], cols: List[str]) -> str:
    # All columns included in the insert; on conflict on PK -> update non-PK cols
    insert_cols = cols
    placeholders = [f"%({c})s" for c in insert_cols]
    non_pk = [c for c in insert_cols if c not in pk_cols]
    set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in non_pk]) or "/* no update */"
    pk_list = ", ".join(pk_cols)
    col_list = ", ".join(insert_cols)
    ph_list = ", ".join(placeholders)
    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({ph_list})
        ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause};
    """
    return sql

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Folder containing the CSV templates")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--apply", action="store_true", help="Write changes to Postgres")
    mode.add_argument("--dry-run", action="store_true", help="Validate only; no DB writes")
    args = ap.parse_args()

    root = args.root

    # Load all CSVs
    loaded = {}
    for key, spec in CFG.items():
        path = os.path.join(root, spec["csv"])
        rows = read_csv(path)
        expected = [c for c, _ in spec["cols"]]
        ensure_headers(rows, expected, spec["csv"])
        # Type coercion / defaults / required checks
        clean = []
        seen_pks = set()
        for i, row in enumerate(rows, start=2):  # start=2 accounts for header line #1
            # apply defaults
            row = {**{k: v for k, v in row.items()}}
            if "defaults" in spec:
                for dk, dv in spec["defaults"].items():
                    if not (row.get(dk) or "").strip():
                        row[dk] = dv

            # coerce types
            try:
                prow = to_python(row)
            except Exception as e:
                raise ValueError(f"{spec['csv']} line {i}: {e}") from e

            # required checks
            for col, req in spec["cols"]:
                if req is REQUIRED:
                    if prow.get(col) in (None, ""):
                        raise ValueError(f"{spec['csv']} line {i}: missing required '{col}'")

            # enum checks
            for col, allowed in spec.get("enums", {}).items():
                val = (prow.get(col) or "")
                validate_enum(col, val, allowed)

            # dedupe within file on business key
            pk_tuple = tuple((prow.get(col) or "") for col in spec["pk"])
            if pk_tuple in seen_pks:
                raise ValueError(f"{spec['csv']}: duplicate primary key {pk_tuple}")
            seen_pks.add(pk_tuple)

            clean.append(prow)
        loaded[key] = clean

    # FK checks (in-memory against what will be upserted)
    index_by = {}
    for key, spec in CFG.items():
        if spec["pk"]:
            idx = set()
            for r in loaded[key]:
                pk_tuple = tuple(r.get(col) for col in spec["pk"])
                idx.add(pk_tuple)
            index_by[key] = idx

    def check_fk(child_key: str, fk_spec: List[Tuple[str, str, str]]):
        for row in loaded[child_key]:
            for child_col, parent_key, parent_col in fk_spec:
                val = row.get(child_col)
                if val in (None, ""):
                    continue
                parent_pk = (val,) if len(CFG[parent_key]["pk"]) == 1 else None
                if parent_pk is None:
                    # Composite parent not expected here; extend if needed
                    raise ValueError(f"FK check for composite key not implemented for {parent_key}")
                if parent_pk not in index_by.get(parent_key, set()):
                    raise ValueError(
                        f"{child_key}: '{child_col}' references missing {parent_key}.{parent_col}='{val}'"
                    )

    for key, spec in CFG.items():
        if "fk" in spec:
            check_fk(key, spec["fk"])

    # If dry run, just print summary and exit
    summary = {k: len(v) for k, v in loaded.items()}
    print("Validation OK. Row counts:", summary)
    if args.dry_run:
        return

    # Apply to DB
    dsn = " ".join([
        f"host={os.environ.get('PGHOST', '')}",
        f"port={os.environ.get('PGPORT', '5432')}",
        f"dbname={os.environ.get('PGDATABASE', '')}",
        f"user={os.environ.get('PGUSER', '')}",
        f"password={os.environ.get('PGPASSWORD', '')}",
    ])
    if "dbname=" not in dsn or os.environ.get("PGDATABASE") in (None, ""):
        print("ERROR: Missing PG* environment variables. Set PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.", file=sys.stderr)
        sys.exit(2)

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Order matters: parents first, then M2M joins
            order = ["voyages", "passengers", "presidents", "media",
                     "voyage_passengers", "voyage_presidents", "voyage_media"]
            for key in order:
                spec = CFG[key]
                table = spec["table"]
                pk = spec["pk"]
                cols = [c for c, _ in spec["cols"]]
                sql = build_upsert_sql(table, pk, cols)
                for row in loaded[key]:
                    cur.execute(sql, row)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("Upsert complete.", summary)

if __name__ == "__main__":
    main()
