import os
import re
import time
import random
import logging
from typing import Dict, Tuple, Optional, List

from psycopg2.extras import execute_values

LOG = logging.getLogger("voyage_ingest.db_updater")

def _conn():
    import psycopg2
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )

def _schema(cur):
    schema = os.environ.get("DB_SCHEMA", "sequoia")
    cur.execute(f"SET search_path = {schema}, public;")

def reset_presidents_table_from_list(presidents: List[Dict]) -> None:
    """
    Replace entire presidents table content with the given list (from Doc headers).
    This will fail if voyages table has a foreign key to presidents on rows pointing
    to slugs not present in 'presidents'. Ensure you upsert presidents BEFORE voyages
    or temporarily disable FK checks if needed.
    """
    conn = _conn(); conn.autocommit = False
    try:
        with conn.cursor() as cur:
            _schema(cur)
            # safest: upsert instead of truncate; then delete any extra not in given list
            slugs = [p.get("president_slug","") for p in presidents or [] if p.get("president_slug")]
            # Upsert rows
            rows = []
            for p in presidents or []:
                rows.append((
                    p.get("president_slug",""), p.get("full_name",""), p.get("party",""),
                    p.get("term_start",""), p.get("term_end",""),
                    p.get("wikipedia_url",""), p.get("tags",""),
                ))
            if rows:
                execute_values(cur, """
                    INSERT INTO presidents (president_slug, full_name, party, term_start, term_end, wikipedia_url, tags)
                    VALUES %s
                    ON CONFLICT (president_slug) DO UPDATE SET
                      full_name=EXCLUDED.full_name, party=EXCLUDED.party,
                      term_start=EXCLUDED.term_start, term_end=EXCLUDED.term_end,
                      wikipedia_url=EXCLUDED.wikipedia_url, tags=EXCLUDED.tags;
                """, rows)

            # delete presidents not in slugs IF they are not referenced
            if slugs:
                cur.execute("""
                    DELETE FROM presidents p
                    WHERE p.president_slug NOT IN %s
                    AND NOT EXISTS (
                      SELECT 1 FROM voyages v WHERE v.president_slug_from_voyage = p.president_slug
                    );
                """, (tuple(slugs),))
        conn.commit()
        LOG.info("Presidents reset/upsert complete: %d", len(presidents or []))
    except Exception as e:
        conn.rollback()
        LOG.error("Failed to reset presidents table: %s", e)
        raise
    finally:
        conn.close()

def upsert_all(bundle: Dict, s3_links: Dict[str, Tuple[Optional[str], Optional[str]]]) -> None:
    """
    Upsert voyages, people, media, and joins for one voyage bundle.
    Assumes presidents table has already been populated from Doc headers.
    """
    v = bundle["voyage"]; ppl = bundle.get("passengers", []) or []; med = bundle.get("media", []) or []
    vslug = v["voyage_slug"]
    pres_slug = (v.get("president_slug") or "").strip()

    def _ns(x): 
        if x is None: return None
        s = str(x).strip()
        return s if s else None

    conn = _conn(); conn.autocommit = False
    try:
        with conn.cursor() as cur:
            _schema(cur)

            # voyages
            cur.execute("""
                INSERT INTO voyages (
                    voyage_slug, title, start_date, end_date, start_time, end_time,
                    origin, destination, vessel_name, voyage_type,
                    summary_markdown, source_urls, tags, president_slug_from_voyage
                ) VALUES (%(voyage_slug)s, %(title)s, %(start_date)s, %(end_date)s, %(start_time)s, %(end_time)s,
                         %(origin)s, %(destination)s, %(vessel_name)s, %(voyage_type)s,
                         %(summary_markdown)s, %(source_urls)s, %(tags)s, %(president_slug)s)
                ON CONFLICT (voyage_slug) DO UPDATE SET
                    title=EXCLUDED.title, start_date=EXCLUDED.start_date, end_date=EXCLUDED.end_date,
                    start_time=EXCLUDED.start_time, end_time=EXCLUDED.end_time, origin=EXCLUDED.origin,
                    destination=EXCLUDED.destination, vessel_name=EXCLUDED.vessel_name, voyage_type=EXCLUDED.voyage_type,
                    summary_markdown=EXCLUDED.summary_markdown, source_urls=EXCLUDED.source_urls,
                    tags=EXCLUDED.tags, president_slug_from_voyage=EXCLUDED.president_slug_from_voyage;
            """, {
                "voyage_slug": _ns(v.get("voyage_slug")),
                "title": _ns(v.get("title")),
                "start_date": _ns(v.get("start_date")),
                "end_date": _ns(v.get("end_date")),
                "start_time": _ns(v.get("start_time")),
                "end_time": _ns(v.get("end_time")),
                "origin": _ns(v.get("origin")),
                "destination": _ns(v.get("destination")),
                "vessel_name": _ns(v.get("vessel_name") or "USS Sequoia"),
                "voyage_type": _ns(v.get("voyage_type")),
                "summary_markdown": _ns(v.get("summary_markdown") or v.get("summary")),
                "source_urls": _ns(v.get("source_urls")),
                "tags": _ns(v.get("tags")),
                "president_slug": _ns(pres_slug),
            })

            # people
            if ppl:
                rows = []
                for p in ppl:
                    rows.append((
                        _ns(p.get("slug") or p.get("person_slug")),
                        _ns(p.get("full_name")),
                        _ns(p.get("role_title")),
                        _ns(p.get("organization")),
                        int(p["birth_year"]) if _ns(p.get("birth_year")) else None,
                        int(p["death_year"]) if _ns(p.get("death_year")) else None,
                        _ns(p.get("wikipedia_url")),
                        None,
                        _ns(p.get("tags")),
                    ))
                execute_values(cur, """
                    INSERT INTO people (person_slug, full_name, role_title, organization,
                                        birth_year, death_year, wikipedia_url, notes_internal, tags)
                    VALUES %s
                    ON CONFLICT (person_slug) DO UPDATE SET
                      full_name=EXCLUDED.full_name, role_title=EXCLUDED.role_title, organization=EXCLUDED.organization,
                      birth_year=EXCLUDED.birth_year, death_year=EXCLUDED.death_year, wikipedia_url=EXCLUDED.wikipedia_url,
                      tags=EXCLUDED.tags;
                """, rows)

            # media
            if med:
                rows = []
                for m in med:
                    mslug = _ns(m.get("slug"))
                    s3_orig, s3_pub = (s3_links.get(mslug, (None, None)) if mslug else (None, None))
                    rows.append((
                        mslug, _ns(m.get("title")),
                        _ns(m.get("media_type")), _ns(s3_orig), _ns(s3_pub),
                        _ns(m.get("credit")), _ns(m.get("date")),  # date is left as TEXT-friendly
                        _ns(m.get("description_markdown") or m.get("description")),
                        _ns(m.get("tags")), _ns(m.get("google_drive_link")),
                    ))
                execute_values(cur, """
                    INSERT INTO media (media_slug, title, media_type, s3_url, public_derivative_url,
                                       credit, date, description_markdown, tags, google_drive_link)
                    VALUES %s
                    ON CONFLICT (media_slug) DO UPDATE SET
                      title=EXCLUDED.title, media_type=EXCLUDED.media_type, s3_url=EXCLUDED.s3_url,
                      public_derivative_url=EXCLUDED.public_derivative_url, credit=EXCLUDED.credit, date=EXCLUDED.date,
                      description_markdown=EXCLUDED.description_markdown, tags=EXCLUDED.tags,
                      google_drive_link=EXCLUDED.google_drive_link;
                """, rows)

            # joins
            if ppl:
                rows = []
                for p in ppl:
                    rows.append((vslug, _ns(p.get("slug") or p.get("person_slug")), _ns(p.get("role_title")) or "Guest", None))
                execute_values(cur, """
                    INSERT INTO voyage_passengers (voyage_slug, person_slug, capacity_role, notes)
                    VALUES %s
                    ON CONFLICT (voyage_slug, person_slug) DO UPDATE SET
                      capacity_role=EXCLUDED.capacity_role, notes=EXCLUDED.notes;
                """, rows)
            if med:
                rows = []
                for m in med:
                    mslug = _ns(m.get("slug"))
                    sort = None
                    if mslug:
                        parts = mslug.rsplit("-",1)
                        if len(parts)==2 and parts[1].isdigit():
                            sort = int(parts[1])
                    rows.append((vslug, mslug, sort, None))
                execute_values(cur, """
                    INSERT INTO voyage_media (voyage_slug, media_slug, sort_order, notes)
                    VALUES %s
                    ON CONFLICT (voyage_slug, media_slug) DO UPDATE SET
                      sort_order=COALESCE(EXCLUDED.sort_order, voyage_media.sort_order), notes=EXCLUDED.notes;
                """, rows)

        conn.commit()
        LOG.info("DB upsert complete for voyage %s", vslug)
    except Exception as e:
        conn.rollback()
        LOG.error("DB upsert failed for voyage %s: %s", vslug, e)
        raise
    finally:
        conn.close()
