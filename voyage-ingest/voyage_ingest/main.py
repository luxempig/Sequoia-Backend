"""
Main entry for multi-voyage docs with optional PRUNE mode.

Env (required):
  DOC_ID
  SPREADSHEET_ID

Env (optional):
  SYNC_MODE       = "upsert" | "prune"        (default: upsert)
  DRY_RUN         = "true" | "false"          (default: false)
  PRUNE_MASTERS   = "true" | "false"          (default: false)  # DB master rows
  # DB_* vars required if using DB upserts/prune:
  # DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SCHEMA (default: sequoia)
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
    sync_mode = (os.environ.get("SYNC_MODE") or "upsert").strip().lower()
    dry_run = _as_bool(os.environ.get("DRY_RUN"), default=False)
    prune_masters = _as_bool(os.environ.get("PRUNE_MASTERS"), default=False)

    if not doc_id or not spreadsheet_id:
        LOG.error("Missing required env vars: DOC_ID and/or SPREADSHEET_ID")
        return

    if sync_mode not in {"upsert", "prune"}:
        LOG.warning("Unknown SYNC_MODE '%s'; defaulting to 'upsert'", sync_mode)
        sync_mode = "upsert"

    LOG.info(
        "=== Voyage Ingest (multi-doc) ===  SYNC_MODE=%s  DRY_RUN=%s  PRUNE_MASTERS=%s",
        sync_mode, dry_run, prune_masters
    )

    # ---------------- Parse the Google Doc into voyage bundles ----------------
    bundles = parser.parse_doc_multi(doc_id)
    if not bundles:
        LOG.error("No voyages found in the document.")
        return

    # ---------------- Global prune: voyages missing from the Doc ----------------
    # If a voyage was removed from the Doc, it won't appear in `bundles`.
    # This step prunes any voyage present in Sheets/DB/S3 but missing from the Doc.
    global_prune_stats = None
    if sync_mode == "prune":
        desired_slugs = {
            (b.get("voyage") or {}).get("voyage_slug", "").strip()
            for b in bundles
            if (b.get("voyage") or {}).get("voyage_slug")
        }
        # Remove potential empty strings
        desired_slugs = {s for s in desired_slugs if s}
        global_prune_stats = reconciler.prune_voyages_missing_from_doc_with_set(
            desired_voyage_slugs=desired_slugs,
            dry_run=dry_run,
            prune_db=True,
            prune_sheets=True,
            prune_s3=True,
        )
        LOG.info("Global prune of missing voyages: %s", global_prune_stats)

    # ---------------- Per-voyage processing ----------------
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    log_rows = []
    total_errors = 0

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
            # Log an error row and skip this voyage
            log_rows.append([
                ts, doc_id, vslug or f"[bundle#{idx}]",
                "ERROR",
                str(len(errs)), "0",
                str(len(bundle.get("media", []) or [])),  # media_declared
                "0", "0",                                  # media_uploaded, thumbs_uploaded
                "upsert" if sync_mode == "upsert" else "prune",
                "TRUE" if dry_run else "FALSE",
                "0", "0",                                   # s3_deleted, s3_archived
                "0", "0",                                   # sheets_deleted_vm, sheets_deleted_vp
                "0", "0",                                   # db_deleted_vm, db_deleted_vp
                "0", "0",                                   # db_deleted_media, db_deleted_people
                errs[0][:250] if errs else "",
            ])
            continue

        # 2) Drive → S3 uploads (originals + derivatives for images)
        s3_links, media_errors = drive_sync.process_all_media(
            bundle.get("media", []), vslug
        )
        for me in media_errors:
            LOG.warning("Media issue: %s", me)

        # 3) PRUNE S3 (per-voyage) BEFORE Sheets upserts
        s3_deleted = s3_archived = 0
        if sync_mode == "prune":
            s3_stats = reconciler.diff_and_prune_s3(
                vslug,
                bundle.get("media", []) or [],
                dry_run=dry_run
            )
            s3_deleted = s3_stats.get("s3_deleted", 0)
            s3_archived = s3_stats.get("s3_archived", 0)

        # 4) Upsert Sheets (this writes voyages, media master, people master, and join tabs)
        sheets_updater.update_all(spreadsheet_id, bundle, s3_links)

        # 5) PRUNE Sheets join rows (per-voyage) AFTER upserts
        sheets_deleted_vm = sheets_deleted_vp = 0
        if sync_mode == "prune":
            sheet_stats = reconciler.diff_and_prune_sheets(bundle, dry_run=dry_run)
            sheets_deleted_vm = sheet_stats.get("deleted_voyage_media", 0)
            sheets_deleted_vp = sheet_stats.get("deleted_voyage_passengers", 0)

        # 6) Upsert DB (per-voyage)
        try:
            db_updater.upsert_all(bundle, s3_links)
        except Exception as e:
            LOG.warning("DB upsert failed for %s: %s", vslug, e)

        # 7) PRUNE DB (per-voyage) AFTER upserts
        db_deleted_vm = db_deleted_vp = db_deleted_media = db_deleted_people = 0
        if sync_mode == "prune":
            db_stats = reconciler.diff_and_prune_db(
                bundle,
                dry_run=dry_run,
                prune_masters=prune_masters
            )
            db_deleted_vm = db_stats.get("db_deleted_voyage_media", 0)
            db_deleted_vp = db_stats.get("db_deleted_voyage_passengers", 0)
            db_deleted_media = db_stats.get("db_deleted_media", 0)
            db_deleted_people = db_stats.get("db_deleted_people", 0)

        # 8) Per-voyage ingest_log row
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
            "upsert" if sync_mode == "upsert" else "prune",
            "TRUE" if dry_run else "FALSE",
            str(s3_deleted), str(s3_archived),
            str(sheets_deleted_vm), str(sheets_deleted_vp),
            str(db_deleted_vm), str(db_deleted_vp),
            str(db_deleted_media), str(db_deleted_people),
            note[:250],
        ])

    # 9) Optional: log a GLOBAL row summarizing global prune (voyages missing from Doc)
    if sync_mode == "prune" and global_prune_stats is not None:
        # Put a synthetic row in ingest_log with voyage_slug = [GLOBAL]
        log_rows.append([
            ts, doc_id, "[GLOBAL]",
            "OK",
            "0", "0",           # errors_count, warnings_count
            "0", "0", "0",      # media_declared, media_uploaded, thumbs_uploaded
            "prune",
            "TRUE" if dry_run else "FALSE",
            str(global_prune_stats.get("s3_deleted", 0)),
            str(global_prune_stats.get("s3_archived", 0)),
            str(global_prune_stats.get("sheets_deleted_rows", 0)),  # we use a single total for sheets
            "0",  # sheets_deleted_vp not separated here
            str(global_prune_stats.get("db_deleted_vm", 0)),
            str(global_prune_stats.get("db_deleted_vp", 0)),
            str(global_prune_stats.get("db_deleted_voyages", 0)),
            "0",  # db_deleted_people not applicable in global prune (masters unaffected)
            f"missing_count={global_prune_stats.get('missing_count', 0)}",
        ])

    # 10) Write all log rows
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
