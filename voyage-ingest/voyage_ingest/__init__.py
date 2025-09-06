"""
Sequoia Voyage Ingest Package

Entry points:
- voyage_ingest.main: end-to-end ingest (Doc -> Sheets/DB/S3)

Modules:
- main.py           : Orchestrates everything
- parser.py         : Parse the Google Doc into bundles (President inheritance)
- validator.py      : Validate bundles (lenient media date)
- drive_sync.py     : Download (Drive/Dropbox), upload to S3, preview/thumb for images
- sheets_updater.py : Exact-update Google Sheets tabs
- db_updater.py     : Reset & upsert Postgres tables
- slugger.py        : Slugify helpers
- reconciler.py     : (Optional) prune extras to exactly match Doc (Sheets/DB)
- utils.py          : shared helpers
"""

__version__ = "0.2.0"
__author__ = "Sequoia Ingest Team"


__all__ = ["main"]
