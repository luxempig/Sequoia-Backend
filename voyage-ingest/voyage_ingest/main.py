"""
Main entry for multi-voyage docs.

Env (required):
  DOC_ID
  SPREADSHEET_ID

Behavior:
- If DRY_RUN=false: enforce Sheets + DB to match the master Doc exactly (prune extras),
  while S3 is additive (no mass deletes). S3 objects are only removed when the SAME
  media link is being renamed/moved to a new required path.
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from voyage_ingest import (
    parser,
    validator,
    drive_sync,
    sheets_updater,
    reconciler,
    db_updater,
)

LOG = logging.getLogger("voyage_ingest")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def _classify_status(validation_errors, media_errors):
    if validation_errors:
        return "ERROR"
    if media_errors:
        return "WITH_WARNINGS"
    return "OK"

def _as_bool(s: str, default=False) -> bool:
    if s is None:
        return default
    return s.strip().lower() in {"1", "true", "yes", "y", "on"}

def main():
    load_dotenv()

    doc_id = os.environ.get("DOC_ID")
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    dry_run = _as_bool(os.environ.get("DRY_RUN"), default=False)

    if not doc_id or not spreadsheet_id:
        LOG.error("Missing required env vars: DOC_ID and/or SPREADSHEET_ID")
        return

    LOG.info("=== Voyage Ingest ===  DRY_RUN=%s", dry_run)

    # ---------------- Parse the Google Doc into voyage bundles ----------------
    bundles = parser.parse_doc_multi(doc_id)
    if not bundles:
        LOG.error("No voyages found in the document.")
        return

    # ---------------- Global exactness: remove voyages missing from Doc (Sheets/DB only) ----------------
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    log_rows = []
    total_errors = 0

    desired_slugs = {
        (b.get("voyage") or {}).get("voyage_slug", "").strip()
        for b in bundles
        if (b.get("voyage") or {}).get("voyage_slug")
    }
    desired_slugs = {s for s in desired_slugs if s}

    global_prune_stats = None
    # When not in dry-run, ensure exact match for Sheets/DB (no S3 global prune)
    global_prune_stats = reconciler.prune_voyages_missing_from_doc_with_set(
        desired_voyage_slugs=desired_slugs,
        dry_run=dry_run,
        prune_db=True,
        prune_sheets=True,
        prune_s3=False,  # keep S3 additive; we only rename on same-link changes
    )
    LOG.info("Global reconcile of missing voyages (Sheets/DB only): %s", global_prune_stats)

    # ---------------- Per-voyage processing ----------------
    for idx, bundle in enumerate(bundles, start=1):
        v = bundle.get("voyage") or {}
        vslug = (v.get("voyage_slug") or "").strip()
        LOG.info("--- Processing voyage %d/%d: %s ---", idx, len(bundles), vslug or "<no-slug>")

        # 1) Validate structured bundle
        errs = validator.validate_bundle(bundle)
        if errs:
            total_errors += len(errs)
            for e in errs:
                LOG.error(" - %s", e)
            log_rows.append([
                ts, doc_id, vslug or f"[bundle#{idx}]",
                "ERROR", str(len(errs)), "0",
                str(len(bundle.get("media", []) or [])),
                "0","0",
                "exact","TRUE" if dry_run else "FALSE",
                "0","0","0","0","0","0","0","0",
                errs[0][:250] if errs else "",
            ])
            continue

        # 2) Media → S3 (additive; move only on same-link rename)
        s3_links, media_errors = drive_sync.process_all_media(
            bundle.get("media", []), vslug, spreadsheet_id=spreadsheet_id
        )
        for me in media_errors:
            LOG.warning("Media issue: %s", me)

        # 3) Upsert Sheets
        sheets_updater.update_all(spreadsheet_id, bundle, s3_links)

        # 4) Per-voyage prune of joins (Sheets/DB) AFTER upserts to ensure exact match with Doc
        sheets_deleted_vm = sheets_deleted_vp = 0
        db_deleted_vm = db_deleted_vp = db_deleted_media = db_deleted_people = 0
        sheet_stats = reconciler.diff_and_prune_sheets(bundle, dry_run=dry_run)
        sheets_deleted_vm = sheet_stats.get("deleted_voyage_media", 0)
        sheets_deleted_vp = sheet_stats.get("deleted_voyage_passengers", 0)

        db_stats = reconciler.diff_and_prune_db(bundle, dry_run=dry_run, prune_masters=not dry_run)
        db_deleted_vm = db_stats.get("db_deleted_voyage_media", 0)
        db_deleted_vp = db_stats.get("db_deleted_voyage_passengers", 0)
        db_deleted_media = db_stats.get("db_deleted_media", 0)
        db_deleted_people = db_stats.get("db_deleted_people", 0)

        # 5) Upsert DB (idempotent)
        try:
            db_updater.upsert_all(bundle, s3_links)
        except Exception as e:
            LOG.warning("DB upsert failed for %s: %s", vslug, e)

        # 6) Per-voyage ingest_log row
        status = _classify_status(errs, media_errors)
        media_declared = len(bundle.get("media", []) or [])
        media_uploaded = sum(1 for _, (orig, _pub) in s3_links.items() if orig)
        thumbs_uploaded = sum(1 for _, (_orig, pub) in s3_links.items() if pub)
        note = (media_errors[0] if media_errors else "OK")

        log_rows.append([
            ts, doc_id, vslug or f"[bundle#{idx}]",
            status,
            "0", str(len(media_errors)),
            str(media_declared),
            str(media_uploaded),
            str(thumbs_uploaded),
            "exact", "TRUE" if dry_run else "FALSE",
            "0","0",
            str(sheets_deleted_vm), str(sheets_deleted_vp),
            str(db_deleted_vm), str(db_deleted_vp),
            str(db_deleted_media), str(db_deleted_people),
            note[:250],
        ])

    # 7) Add a GLOBAL row summarizing global reconcile (Sheets/DB)
    if global_prune_stats is not None:
        log_rows.append([
            ts, doc_id, "[GLOBAL]",
            "OK",
            "0","0","0","0","0",
            "exact","TRUE" if dry_run else "FALSE",
            "0","0",
            str(global_prune_stats.get("sheets_deleted_rows", 0)),
            "0",
            str(global_prune_stats.get("db_deleted_vm", 0)),
            str(global_prune_stats.get("db_deleted_vp", 0)),
            str(global_prune_stats.get("db_deleted_voyages", 0)),
            "0",
            f"missing_count={global_prune_stats.get('missing_count', 0)}",
        ])

    # 8) Write log
    if log_rows:
        try:
            sheets_updater.append_ingest_log(spreadsheet_id, log_rows)
            LOG.info("Wrote %d log row(s) to 'ingest_log'.", len(log_rows))
        except Exception as e:
            LOG.warning("Failed to write ingest_log: %s", e)

    if total_errors:
        LOG.warning("Completed with %d error(s). See logs above.", total_errors)
    else:
        LOG.info("Completed successfully: %d voyage(s) processed.", len(bundles))

if __name__ == "__main__":
    main()
