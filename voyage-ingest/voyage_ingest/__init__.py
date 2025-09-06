"""
Sequoia Voyage Ingest Package

This package ingests a structured Google Doc per voyage and updates:
- S3 (originals + previews/thumbnails)
- Google Sheets (voyages, passengers, media, join tables, presidents)
- Postgres (full reset then insert fresh)

Modules:
- main.py           : entry point (FULL RESET of Sheets & DB each run)
- parser.py         : parse Google Doc -> presidents + structured voyage bundle(s)
- validator.py      : validate slugs, dates, links
- drive_sync.py     : download from Drive/Dropbox, upload to S3 (no global deletes)
- sheets_updater.py : reset presidents, upsert rows
- db_updater.py     : reset tables and upsert rows
- reconciler.py     : unused by default (kept for reference)
- utils.py          : shared helpers
"""

__version__ = "0.2.0"
__author__ = "Daniel Freymann"

from .main import main

__all__ = ["main"]
