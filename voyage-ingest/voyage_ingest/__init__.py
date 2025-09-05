"""
Sequoia Voyage Ingest Package

This package ingests a structured Google Doc per voyage and updates:
- S3 (originals + previews/thumbnails)
- Google Sheets (voyages, passengers, media, join tables)

Modules:
- main.py           : entry point
- parser.py         : parse Google Doc into structured dict
- validator.py      : validate slugs, dates, links
- drive_sync.py     : download from Google Drive, upload to S3
- sheets_updater.py : update Google Sheets tabs
- utils.py          : shared helpers
"""

__version__ = "0.1.0"
__author__ = "Daniel Freymann"

# Optional: expose the main entrypoint so you can run `voyage_ingest.main()`
from .main import main

__all__ = ["main"]
