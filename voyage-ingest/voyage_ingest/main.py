# ================================================================
# voyage-ingest/voyage_ingest/main.py
# ================================================================
"""
Main entry for Sequoia multi-voyage ingest.

Environment (required):
  DOC_ID                 : Google Doc ID containing the ingestable markdown
  SPREADSHEET_ID        : Target Google Sheet (voyages, media, passengers, joins, presidents)
  GOOGLE_APPLICATION_CREDENTIALS : Path to service account JSON

Behavior:
- Parse the Doc into (presidents, bundles).
- Reset the presidents tab (Sheets) and table (DB) from the Doc's ## President headers.
- For each voyage bundle:
    * validate
    * process media → S3 (additive; rename/move if same link seen with new slugged path)
    * upsert to Sheets
    * prune per-voyage dangling joins in Sheets/DB to exactly match the Doc
    * upsert to DB
- Global reconcile: remove voyages missing from the Doc (Sheets/DB only; S3 untouched here).
- Append ingest_log rows.

Notes:
- No need to provide voyage_slug in the Doc; parser auto-generates it.
- The last seen ## President header applies to all following voyages until the next ## President.
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


def _as_bool(s: str, default=False) -> bool:
    if s is None:
        return default
    return s.strip().lower() in {"1", "true", "yes", "y", "on"}


def _classify_status(validation_errors, media_warnings):
    if validation_errors:
        return "ERROR"
    if media_warnings:
        return "WITH_WARNINGS"
    return "OK"


def main():
    load_dotenv()

    doc_id = os.environ.get("DOC_ID", "").strip()
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()
    dry_run = _as_bool(os.environ.get("DRY_RUN"), default=False)

    if not doc_id or not spreadsheet_id:
        LOG.error("Missing required env vars: DOC_ID and/or SPREADSHEET_ID")
        return

    LOG.info("=== Voyage Ingest ===  DRY_RUN=%s  DOC_ID=%s  SHEET=%s", dry_run, doc_id, spreadsheet_id)

    # ---------------- Parse the Google Doc into presidents + voyage bundles ----------------
    presidents, bundles = parser.parse_doc_multi(doc_id)
    if not bundles:
        LOG.error("No voyages found in the document.")
        return

    # ---------------- Reset presidents (Sheets + DB) from headers ----------------
    try:
        sheets_updater.reset_presidents_sheet(spreadsheet_id, presidents)
    except Exception as e:
        LOG.error("Failed to reset presidents in Sheets: %s", e)
        return
    try:
        db_updater.reset_presidents_table_from_list(presidents)
    except Exception as e:
        LOG.error("Failed to reset presidents table: %s", e)
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

    global_prune_stats = reconciler.prune_voyages_missing_from_doc_with_set(
        desired_voyage_slugs=desired_slugs,
        dry_run=dry_run,
        prune_db=True,
        prune_sheets=True,
        prune_s3=False,  # keep S3 additive globally (we only rename/move on same-link changes)
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
                "0", "0",
                "exact", "TRUE" if dry_run else "FALSE",
                "0", "0", "0", "0", "0", "0", "0", "0",
                (errs[0] if errs else "")[:250],
            ])
            continue

        # 2) Media → S3 (additive; move on same-link rename if needed)
        s3_links, media_warnings = drive_sync.process_all_media(
            bundle.get("media", []), vslug, spreadsheet_id=spreadsheet_id
        )
        for mw in media_warnings:
            LOG.warning("Media issue: %s", mw)

        # 3) Upsert Sheets (voyages/passengers/media & joins)
        try:
            sheets_updater.update_all(spreadsheet_id, bundle, s3_links)
        except Exception as e:
            LOG.error("Sheets update failed for %s: %s", vslug, e)

        # 4) Per-voyage prune of joins (Sheets/DB) AFTER upserts to ensure exact match with Doc
        sheets_deleted_vm = sheets_deleted_vp = 0
        db_deleted_vm = db_deleted_vp = db_deleted_media = db_deleted_people = 0

        try:
            sheet_stats = reconciler.diff_and_prune_sheets(bundle, dry_run=dry_run)
            sheets_deleted_vm = sheet_stats.get("deleted_voyage_media", 0)
            sheets_deleted_vp = sheet_stats.get("deleted_voyage_passengers", 0)
        except Exception as e:
            LOG.warning("Sheets prune failed for %s: %s", vslug, e)

        try:
            db_stats = reconciler.diff_and_prune_db(bundle, dry_run=dry_run, prune_masters=not dry_run)
            db_deleted_vm = db_stats.get("db_deleted_voyage_media", 0)
            db_deleted_vp = db_stats.get("db_deleted_voyage_passengers", 0)
            db_deleted_media = db_stats.get("db_deleted_media", 0)
            db_deleted_people = db_stats.get("db_deleted_people", 0)
        except Exception as e:
            LOG.warning("DB prune failed for %s: %s", vslug, e)

        # 5) Upsert DB (idempotent)
        try:
            db_updater.upsert_all(bundle, s3_links)
        except Exception as e:
            LOG.warning("DB upsert failed for %s: %s", vslug, e)

        # 6) Ingest log row
        status = _classify_status(errs, media_warnings)
        media_declared = len(bundle.get("media", []) or [])
        media_uploaded = sum(1 for _, (orig, _pub) in s3_links.items() if orig)
        thumbs_uploaded = sum(1 for _, (_orig, pub) in s3_links.items() if pub)
        note = (media_warnings[0] if media_warnings else "OK")

        log_rows.append([
            ts, doc_id, vslug or f"[bundle#{idx}]",
            status,
            "0", str(len(media_warnings)),
            str(media_declared),
            str(media_uploaded),
            str(thumbs_uploaded),
            "exact", "TRUE" if dry_run else "FALSE",
            "0", "0",
            str(sheets_deleted_vm), str(sheets_deleted_vp),
            str(db_deleted_vm), str(db_deleted_vp),
            str(db_deleted_media), str(db_deleted_people),
            (note or "")[:250],
        ])

    # 7) Add a GLOBAL row summarizing global reconcile (Sheets/DB)
    if global_prune_stats is not None:
        log_rows.append([
            ts, doc_id, "[GLOBAL]",
            "OK",
            "0", "0", "0", "0", "0",
            "exact", "TRUE" if dry_run else "FALSE",
            "0", "0",
            str(global_prune_stats.get("sheets_deleted_rows", 0)),
            "0",
            str(global_prune_stats.get("db_deleted_vm", 0)),
            str(global_prune_stats.get("db_deleted_vp", 0)),
            str(global_prune_stats.get("db_deleted_voyages", 0)),
            "0",
            f"missing_count={global_prune_stats.get('missing_count', 0)}",
        ])

    # 8) Write ingest_log
    if log_rows:
        try:
            sheets_updater.append_ingest_log(spreadsheet_id, log_rows)
            LOG.info("Wrote %d log row(s) to 'ingest_log'.", len(log_rows))
        except Exception as e:
            LOG.warning("Failed to write ingest_log: %s", e)

    if total_errors:
        LOG.warning("Completed with %d validation error(s). See logs above.", total_errors)
    else:
        LOG.info("Completed successfully: %d voyage(s) processed.", len(bundles))


if __name__ == "__main__":
    main()
