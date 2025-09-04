# Data Dictionary & Validation Notes

## Global conventions
- All *_slug columns are **stable human-readable keys** used to link rows across tabs.
  - Use lowercase, alphanumeric plus hyphens; no spaces. e.g. `1934-06-15-fdr-royal-visit`
  - Slugs are used for idempotent upserts; changing them creates new rows.
- `tags` fields are pipe-separated lists. e.g., `FDR|royalty|press`
- Dates are `YYYY-MM-DD`. Empty is allowed when unknown.
- `source_urls` is pipe-separated list of URLs.

## voyages.csv
- `voyage_slug` (required, unique) — business key
- `title` (required)
- `start_date` (required) — for single-day events, set `end_date` empty or equal to start_date
- `end_date` (optional)
- `origin`, `destination` (optional free text)
- `vessel_name` (default "USS Sequoia" if empty)
- `voyage_type` (enum) — `official|private|maintenance|other`
- `summary_markdown` — rich description (Markdown supported on site)
- `notes_internal` — curator-only notes (not displayed publicly)
- `source_urls` — pipe-separated references
- `tags` — pipe-separated tags

## passengers.csv
- `person_slug` (required, unique)
- `full_name` (required)
- `role_title`, `organization` (optional)
- `birth_year`, `death_year` (optional, integers)
- `wikipedia_url` (optional)
- `notes_internal`, `tags` (optional)

## presidents.csv
- `president_slug` (required, unique) — should match a `person_slug` when the person also appears in passengers
- `full_name`, `party` (required)
- `term_start`, `term_end` (required dates)
- `wikipedia_url`, `notes_internal`, `tags` (optional)

## media.csv
- `media_slug` (required, unique)
- `media_type` — enum: `image|pdf|audio|video|other`
- `s3_url` — The canonical asset location (S3 URI or HTTPS)
- `thumbnail_s3_url` — optional preview/thumbnail
- `credit`, `date`, `description_markdown`, `tags`, `copyright_restrictions`

## voyage_passengers.csv
- Links a voyage to a person.
- `capacity_role` is free text ("President", "Crew", "Guest", "Press", ...)

## voyage_presidents.csv
- Links a voyage to a president (subset for fast filtering or existing views).

## voyage_media.csv
- Links a voyage to media assets with a `sort_order` integer for gallery display.
