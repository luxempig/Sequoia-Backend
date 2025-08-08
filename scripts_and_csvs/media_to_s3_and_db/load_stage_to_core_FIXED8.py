#!/usr/bin/env python3
"""
Load stage_*.csv files into the core Sequoia DB tables (idempotent).

Improvements vs FIXED7:
- Handles voyage_stops primary key (voyage_id, stop_order):
  * If stop_order is missing/<=0, it assigns the next available integer for that voyage.
  * Uses ON CONFLICT (voyage_id, stop_order) DO UPDATE to upsert safely.
- Still uses arrived_at/departed_at/location_name; role_on_voyage for voyage_passengers.
- Boolean-safe for voyages.significant_voyage / voyages.royalty.
"""
import argparse, csv, os, re
csv.field_size_limit(10**8)
from typing import Dict, Optional, Set

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

def to_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    s = s.replace("—", "-").replace("–", "-").replace("/", "-")
    s = re.sub(r"\?\?", "01", s)
    m = re.match(r"^\s*(\d{4})(?:-(\d{1,2})(?:-(\d{1,2}))?)?\s*$", s)
    if not m:
        return None
    y = int(m.group(1)); mth = int(m.group(2)) if m.group(2) else 1; d = int(m.group(3)) if m.group(3) else 1
    mth = max(1, min(12, mth)); d = max(1, min(28, d))
    return f"{y:04d}-{mth:02d}-{d:02d}"

def to_bool_flag(v) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"1","true","t","yes","y"}: return True
    if s in {"0","false","f","no","n"}: return False
    if s == "": return None
    try: return bool(int(s))
    except Exception: return None

def table_exists(cur, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"public.{table}",))
    return bool(cur.fetchone()["ok"])

def col_exists(cur, table: str, col: str) -> bool:
    cur.execute("""
        SELECT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema='public' AND table_name=%s AND column_name=%s
        ) AS ok
    """, (table, col))
    return bool(cur.fetchone()["ok"])

def load_csv(path: Optional[str]):
    if not path: return []
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require",
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--voyages", required=True)
    ap.add_argument("--stops", required=False)
    ap.add_argument("--voyage-passengers", dest="voyage_passengers", required=True)
    ap.add_argument("--passengers", required=True)
    args = ap.parse_args()

    load_dotenv()
    stg_voyages = load_csv(args.voyages)
    stg_stops = load_csv(args.stops) if args.stops else []
    stg_vp = load_csv(args.voyage_passengers)
    stg_pass = load_csv(args.passengers)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # ------------ Voyages upsert ------------
    stg_to_vid: Dict[int, int] = {}
    inserted_voyages = 0; matched_voyages = 0

    for r in stg_voyages:
        try:
            stg_id = int((r.get("stg_voyage_id") or r.get("voyage_id") or "").strip())
        except Exception:
            continue

        sd = to_date(r.get("start_date") or r.get("start_timestamp"))
        ed = to_date(r.get("end_date") or r.get("end_timestamp") or r.get("start_date"))
        additional_info = (r.get("additional_info") or "").strip() or None
        notes = (r.get("notes") or "").strip() or None

        significant = to_bool_flag(r.get("significant") or r.get("significant_voyage") or r.get("significant_voyage?"))
        royalty = to_bool_flag(r.get("royalty") or r.get("royalty?"))

        if not sd:
            continue

        cur.execute("""
            SELECT voyage_id FROM voyages
            WHERE start_timestamp::date = %s
              AND COALESCE(end_timestamp::date, %s) = %s
              AND COALESCE(additional_info,'') = COALESCE(%s,'')
            ORDER BY voyage_id LIMIT 1
        """, (sd, ed or sd, ed or sd, additional_info))
        row = cur.fetchone()
        if row:
            vid = row["voyage_id"]
            stg_to_vid[stg_id] = vid
            matched_voyages += 1
            cur.execute("""
                UPDATE voyages SET
                    notes = COALESCE(NULLIF(%s,''), notes),
                    significant_voyage = COALESCE(%s, significant_voyage),
                    royalty = COALESCE(%s, royalty)
                WHERE voyage_id = %s
            """, (notes or "", significant, royalty, vid))
        else:
            cur.execute("""
                INSERT INTO voyages (start_timestamp, end_timestamp, additional_info, notes, significant_voyage, royalty)
                VALUES (%s::date, %s::date, %s, %s,
                        COALESCE(%s, FALSE),
                        COALESCE(%s, FALSE))
                RETURNING voyage_id
            """, (sd, ed or sd, additional_info, notes, significant, royalty))
            vid = cur.fetchone()["voyage_id"]
            stg_to_vid[stg_id] = vid
            inserted_voyages += 1

    conn.commit()

    # ------------ Passengers upsert ------------
    cur.execute("SELECT passenger_id, name FROM passengers")
    name_to_pid = {row["name"].strip(): row["passenger_id"] for row in cur.fetchall()}

    upserted_pass = 0
    for r in stg_pass:
        name = (r.get("name") or r.get("passenger_name") or "").strip()
        if not name: continue
        bio_path = (r.get("bio_path") or "").strip() or None
        basic_info = (r.get("basic_info") or "").strip() or None

        pid = name_to_pid.get(name)
        if pid:
            cur.execute("""
                UPDATE passengers
                SET bio_path = COALESCE(%s, bio_path),
                    basic_info = COALESCE(%s, basic_info)
                WHERE passenger_id = %s
            """, (bio_path, basic_info, pid))
        else:
            cur.execute("""
                INSERT INTO passengers (name, bio_path, basic_info)
                VALUES (%s, %s, %s)
                RETURNING passenger_id
            """, (name, bio_path, basic_info))
            pid = cur.fetchone()["passenger_id"]
            name_to_pid[name] = pid
        upserted_pass += 1

    conn.commit()

    # ------------ Voyage-passengers link ------------
    has_role = col_exists(cur, "voyage_passengers", "role_on_voyage")
    linked_vp = 0; skipped_vp = 0
    for r in stg_vp:
        try:
            stg_id = int((r.get("stg_voyage_id") or r.get("voyage_id") or "").strip())
        except Exception:
            skipped_vp += 1; continue
        name = (r.get("name") or r.get("passenger_name") or "").strip()
        role = (r.get("role") or r.get("note") or "").strip() or None

        vid = stg_to_vid.get(stg_id)
        pid = name_to_pid.get(name)
        if not vid or not pid:
            skipped_vp += 1; continue

        cur.execute("SELECT 1 FROM voyage_passengers WHERE voyage_id=%s AND passenger_id=%s LIMIT 1", (vid, pid))
        exists = cur.fetchone() is not None
        if not exists:
            if has_role:
                cur.execute("INSERT INTO voyage_passengers (voyage_id, passenger_id, role_on_voyage) VALUES (%s, %s, %s)", (vid, pid, role))
            else:
                cur.execute("INSERT INTO voyage_passengers (voyage_id, passenger_id) VALUES (%s, %s)", (vid, pid))
            linked_vp += 1

    conn.commit()

    # ------------ Stops (assign safe stop_order; upsert on PK) ------------
    stops_done = 0; stops_skipped = 0
    has_ports = table_exists(cur, "ports")
    has_vstops = table_exists(cur, "voyage_stops")

    if stg_stops and has_ports and has_vstops:
        # cache ports by name and current max stop_order per voyage
        cur.execute("SELECT port_id, name FROM ports")
        port_cache = {row["name"].strip(): row["port_id"] for row in cur.fetchall()}

        cur.execute("SELECT voyage_id, MAX(stop_order) AS maxord FROM voyage_stops GROUP BY voyage_id")
        next_order: Dict[int, int] = {row["voyage_id"]: (row["maxord"] or 0) for row in cur.fetchall()}
        used_orders: Dict[int, Set[int]] = {}
        for vid, maxord in next_order.items():
            used_orders[vid] = set()
            cur.execute("SELECT stop_order FROM voyage_stops WHERE voyage_id=%s", (vid,))
            used_orders[vid].update([r["stop_order"] for r in cur.fetchall()])

        def assign_order(vid: int, proposed: Optional[int]) -> int:
            if vid not in used_orders:
                used_orders[vid] = set()
                next_order[vid] = 0
            if proposed is None or proposed <= 0:
                # assign next available
                o = next_order[vid] + 1
                while o in used_orders[vid]:
                    o += 1
                used_orders[vid].add(o)
                next_order[vid] = o
                return o
            # ensure proposed isn't already used; if it is, bump
            o = proposed
            while o in used_orders[vid]:
                o += 1
            used_orders[vid].add(o)
            if o > next_order[vid]:
                next_order[vid] = o
            return o

        for r in stg_stops:
            try:
                stg_id = int((r.get("stg_voyage_id") or r.get("voyage_id") or "").strip())
            except Exception:
                stops_skipped += 1; continue
            vid = stg_to_vid.get(stg_id)
            if not vid:
                stops_skipped += 1; continue

            name = (r.get("location") or r.get("port") or r.get("stop") or "").strip()
            if not name:
                stops_skipped += 1; continue

            try:
                proposed = int((r.get("stop_order") or r.get("order") or "0").strip())
            except Exception:
                proposed = None
            order = assign_order(vid, proposed)

            lat = (r.get("lat") or r.get("latitude") or "").strip() or None
            lon = (r.get("lon") or r.get("longitude") or "").strip() or None
            arr = to_date(r.get("arrival_date") or r.get("arrival"))
            dep = to_date(r.get("departure_date") or r.get("departure"))
            notes = (r.get("notes") or "").strip() or None

            # resolve/create port_id
            port_id = port_cache.get(name)
            if port_id is None:
                cur.execute("SELECT port_id FROM ports WHERE name=%s LIMIT 1", (name,))
                row = cur.fetchone()
                if row:
                    port_id = row["port_id"]
                    port_cache[name] = port_id
                else:
                    cur.execute("""
                        INSERT INTO ports (name, latitude, longitude)
                        VALUES (%s, NULLIF(%s,'')::double precision, NULLIF(%s,'')::double precision)
                        RETURNING port_id
                    """, (name, lat, lon))
                    port_id = cur.fetchone()["port_id"]
                    port_cache[name] = port_id

            # upsert stop on PK (voyage_id, stop_order)
            cur.execute("""
                INSERT INTO voyage_stops (voyage_id, port_id, stop_order, arrived_at, departed_at, location_name, notes)
                VALUES (%s, %s, %s, %s::date, %s::date, %s, %s)
                ON CONFLICT (voyage_id, stop_order) DO UPDATE
                  SET arrived_at    = COALESCE(voyage_stops.arrived_at, EXCLUDED.arrived_at),
                      departed_at   = COALESCE(voyage_stops.departed_at, EXCLUDED.departed_at),
                      location_name = COALESCE(voyage_stops.location_name, EXCLUDED.location_name),
                      notes         = COALESCE(voyage_stops.notes, EXCLUDED.notes),
                      port_id       = COALESCE(voyage_stops.port_id, EXCLUDED.port_id)
            """, (vid, port_id, order, arr, dep, name, notes))
            stops_done += 1

        conn.commit()
    else:
        if stg_stops and (not has_ports or not has_vstops):
            print("NOTE: Skipping stops — 'ports' and/or 'voyage_stops' table not found.")

    print(f"Voyages: inserted {inserted_voyages}, matched {matched_voyages}")
    print(f"Passengers upserted: {upserted_pass}")
    print(f"Voyage-passengers linked: {linked_vp}, skipped {skipped_vp}")
    if stg_stops:
        print(f"Stops inserted/updated: {stops_done}, skipped {stops_skipped}")

    cur.close(); conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
