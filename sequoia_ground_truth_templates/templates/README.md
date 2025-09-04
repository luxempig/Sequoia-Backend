# Sequoia Voyage Ground Truth (Human-Editable)

This folder contains CSV templates that act as the **source of truth** (SoT) for voyages and related entities.
Curators edit these CSVs (or maintain a Google Sheet that exports to the same headers), and a loader script
ingests them into Postgres idempotently.

## Files
- `voyages.csv` — one row per voyage (business key: `voyage_slug`)
- `passengers.csv` — one row per person (`person_slug`)
- `presidents.csv` — subset of notable people (`president_slug`)
- `media.csv` — one row per media asset (`media_slug`)
- `voyage_passengers.csv` — many-to-many join between voyages and people
- `voyage_presidents.csv` — many-to-many between voyages and presidents
- `voyage_media.csv` — many-to-many between voyages and media with ordering

### Workflow
1. Curators add / edit rows in these CSVs (or the equivalent Google Sheet tabs with identical headers).
2. Run `loader.py` (see below) which validates, then **upserts** rows into Postgres in a single transaction.
3. Your API/website reads from Postgres (plus an optional `voyage_with_presidency` view you already have).

See `data_dictionary.md` for column definitions and validation notes.
