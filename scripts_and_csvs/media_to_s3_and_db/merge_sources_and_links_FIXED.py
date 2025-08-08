#!/usr/bin/env python3
# Merge S3-upload results + timeline-derived metadata into Postgres.
# - Upserts into `sources` (by sha256).
# - Ensures voyages exist from `stage_voyages_full.csv` (by dates + additional_info).
# - Creates `voyage_sources` links from `stage_voyage_source_links.csv`.
# Requires .env with DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD.

import argparse, csv, os, sys, re
csv.field_size_limit(10**8)  # allow very large cells

from typing import Dict, Optional
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

def load_csv(path: str):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))

def to_date(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*$", s)
    if m:
        y, mn, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y:04d}-{mn:02d}-{d:02d}"
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--success-web", default="uploaded_success_web.csv")
    ap.add_argument("--success-drive", default="uploaded_success_drive.csv")
    ap.add_argument("--stage-sources", default="stage_sources.csv", help="timeline-parsed URL metadata")
    ap.add_argument("--stage-links", default="stage_voyage_source_links.csv", help="stg_voyage_id,url,page_num")
    ap.add_argument("--stage-voyages", default="stage_voyages_full.csv", help="stg_voyage_id + dates + info")
    args = ap.parse_args()

    load_dotenv()

    # Read inputs
    successes = []
    for p in [args.success_web, args.success_drive]:
        if os.path.exists(p):
            successes.extend(load_csv(p))
    if not successes:
        print("No success files found or both empty. Nothing to merge.")
        sys.exit(0)
    stage_sources = load_csv(args.stage_sources)
    stage_links = load_csv(args.stage_links)
    stage_voyages = load_csv(args.stage_voyages)

    # Normalize by URL
    meta_by_url: Dict[str, Dict[str, str]] = {}
    for r in stage_sources:
        url = (r.get("url") or "").strip()
        if not url:
            continue
        meta_by_url[url] = r

    succ_by_url: Dict[str, Dict[str, str]] = {}
    for s in successes:
        url = (s.get("url") or "").strip()
        if not url:
            continue
        if url in succ_by_url and not succ_by_url[url].get("skipped"):
            continue
        succ_by_url[url] = s

    # Build voyage staging map: stg_voyage_id -> (start_date, end_date, additional_info)
    voyage_meta = {}
    for v in stage_voyages:
        stg_id = int(v["stg_voyage_id"])
        start_d = to_date(v.get("start_date"))
        end_d = to_date(v.get("end_date") or v.get("start_date"))
        addl = (v.get("additional_info") or "").strip()
        voyage_meta[stg_id] = (start_d, end_d, addl)

    # Connect DB
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require",
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Ensure columns exist in sources
    cur.execute("""
    ALTER TABLE sources
      ADD COLUMN IF NOT EXISTS sha256 TEXT UNIQUE,
      ADD COLUMN IF NOT EXISTS source_path TEXT,
      ADD COLUMN IF NOT EXISTS source_type TEXT,
      ADD COLUMN IF NOT EXISTS source_origin TEXT,
      ADD COLUMN IF NOT EXISTS publication_date DATE,
      ADD COLUMN IF NOT EXISTS publication TEXT,
      ADD COLUMN IF NOT EXISTS headline TEXT,
      ADD COLUMN IF NOT EXISTS page TEXT,
      ADD COLUMN IF NOT EXISTS permalink TEXT,
      ADD COLUMN IF NOT EXISTS archive_ref TEXT;
    """)
    conn.commit()

    # Upsert into sources
    upserts = 0
    for url, s in succ_by_url.items():
        m = meta_by_url.get(url, {})
        params = {
            "sha256": (s.get("sha256") or "").strip(),
            "source_path": (s.get("s3_key") or "").strip(),
            "source_type": (m.get("source_type") or "").strip() or None,
            "source_origin": (m.get("source_origin") or "").strip() or None,
            "publication_date": (m.get("publication_date") or "").strip() or None,
            "publication": (m.get("publication") or "").strip() or None,
            "headline": (m.get("headline") or "").strip() or None,
            "page": (m.get("page") or "").strip() or None,
            "permalink": url,
            "archive_ref": None,
        }
        if not params["sha256"] or not params["source_path"]:
            continue
        cur.execute(
            """
            INSERT INTO sources (sha256, source_path, source_type, source_origin, publication_date, publication, headline, page, permalink, archive_ref)
            VALUES (%(sha256)s, %(source_path)s, %(source_type)s, %(source_origin)s, NULLIF(%(publication_date)s,'')::date, %(publication)s, %(headline)s, %(page)s, %(permalink)s, %(archive_ref)s)
            ON CONFLICT (sha256) DO UPDATE SET
              source_path = EXCLUDED.source_path,
              source_type = COALESCE(EXCLUDED.source_type, sources.source_type),
              source_origin = COALESCE(EXCLUDED.source_origin, sources.source_origin),
              publication_date = COALESCE(EXCLUDED.publication_date, sources.publication_date),
              publication = COALESCE(EXCLUDED.publication, sources.publication),
              headline = COALESCE(EXCLUDED.headline, sources.headline),
              page = COALESCE(EXCLUDED.page, sources.page),
              permalink = COALESCE(EXCLUDED.permalink, sources.permalink);
            """, params,
        )
        upserts += 1
    conn.commit()
    print(f"Upserted/updated {upserts} sources.")

    # Build sha256 -> source_id map
    cur.execute("SELECT source_id, sha256 FROM sources WHERE sha256 IS NOT NULL")
    sid_by_sha = {row["sha256"]: row["source_id"] for row in cur.fetchall()}
    sid_by_url = {}
    for url, s in succ_by_url.items():
        sha = (s.get("sha256") or "").strip()
        if sha and sha in sid_by_sha:
            sid_by_url[url] = sid_by_sha[sha]

    # Ensure voyages exist and build mapping stg_voyage_id -> voyage_id
    map_stg_to_vid = {}
    to_insert = []
    for stg_id, (sd, ed, addl) in voyage_meta.items():
        if not sd:
            continue
        cur.execute(
            """
            SELECT voyage_id FROM voyages
            WHERE start_timestamp::date = %s
              AND COALESCE(end_timestamp::date, %s) = %s
            ORDER BY voyage_id LIMIT 1
            """,
            (sd, sd if ed is None else ed, ed if ed else sd),
        )
        row = cur.fetchone()
        if row:
            map_stg_to_vid[stg_id] = row["voyage_id"]
        else:
            to_insert.append((stg_id, sd, ed or sd, addl))

    if to_insert:
        cur.executemany(
            """
            INSERT INTO voyages (start_timestamp, end_timestamp, additional_info)
            VALUES (%s::date, %s::date, %s)
            """,
            [(sd, ed, addl) for (_, sd, ed, addl) in to_insert],
        )
        conn.commit()
        # Look up IDs after insert
        for stg_id, sd, ed, addl in to_insert:
            cur.execute(
                """
                SELECT voyage_id FROM voyages
                WHERE start_timestamp::date = %s
                  AND COALESCE(end_timestamp::date, %s) = %s
                  AND COALESCE(additional_info,'') = COALESCE(%s,'')
                ORDER BY voyage_id LIMIT 1
                """,
                (sd, ed, ed, addl),
            )
            row = cur.fetchone()
            if row:
                map_stg_to_vid[stg_id] = row["voyage_id"]

    print(f"Mapped {len(map_stg_to_vid)} staging voyages to voyage_id.")

    # Ensure a unique index for (voyage_id, source_id) to avoid dup links
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes WHERE tablename = 'voyage_sources' AND indexname = 'voyage_sources_unique'
            ) THEN
                CREATE UNIQUE INDEX voyage_sources_unique ON voyage_sources (voyage_id, source_id);
            END IF;
        END $$;
        """
    )
    conn.commit()

    # Insert voyage_sources links (dedup per voyage+source; choose min page_num)
    link_map = {}
    bad_links = 0
    for r in stage_links:
        try:
            stg_id = int(r["stg_voyage_id"])
        except Exception:
            continue
        url = (r.get("url") or "").strip()
        page_raw = (r.get("page_num") or "").strip()
        page_num = None
        if page_raw:
            try:
                page_num = int(re.search(r"\\d+", page_raw).group(0))
            except Exception:
                page_num = None

        vid = map_stg_to_vid.get(stg_id)
        sid = sid_by_url.get(url)
        if not vid or not sid:
            bad_links += 1
            continue
        key = (vid, sid)
        if key not in link_map:
            link_map[key] = page_num
        else:
            if page_num is not None and (link_map[key] is None or page_num < link_map[key]):
                link_map[key] = page_num

    # Insert links safely (idempotent)
    for (vid, sid), pn in link_map.items():
        cur.execute(
            """
            INSERT INTO voyage_sources (voyage_id, source_id, page_num)
            VALUES (%s, %s, %s)
            ON CONFLICT (voyage_id, source_id) DO UPDATE
              SET page_num = COALESCE(voyage_sources.page_num, EXCLUDED.page_num);
            """ ,
            (vid, sid, pn),
        )
    conn.commit()
    print(f"Linked {len(link_map)} source(s) to voyages. Skipped {bad_links} that couldn't be matched.")

    cur.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
