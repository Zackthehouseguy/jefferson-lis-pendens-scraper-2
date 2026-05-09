# Property Signal Scraper Suite

Multi-source scraper used by the Lovable property-signal app. Originally built
for Jefferson County KY Lis Pendens, now extended to additional sources via a
single GitHub Actions workflow.

## Supported sources

| `source_type`                    | Description                                        | Method                                  |
|----------------------------------|----------------------------------------------------|-----------------------------------------|
| `lis_pendens` (default)          | Jefferson County KY Lis Pendens                    | jeffersondeeds.com instrument-type form |
| `wills`                          | Jefferson County KY Wills                          | jeffersondeeds.com instrument-type form |
| `louisville_code_violations`     | Louisville Metro property maintenance violations   | ArcGIS FeatureServer (REST)             |
| `indianapolis_code_violations`   | Indianapolis Accela Enforcement                    | **Scaffold only** — see below           |

All sources emit a canonical 5-column CSV (`Date`, `Defendants/Parties`,
`Property Address`, `PDF Link`, `Notes`) and are normalized into the same
ingest payload by `.github/scripts/upload_results.py`.

## GitHub Actions

`.github/workflows/run-scraper.yml` exposes the following `workflow_dispatch`
inputs:

| Input             | Required | Default        | Notes                                                                 |
|-------------------|----------|----------------|-----------------------------------------------------------------------|
| `run_id`          | yes      |                | Lovable run identifier; echoed back to the ingest endpoint.           |
| `source_type`     | no       | `lis_pendens`  | One of the source types above. Omit for backward compatibility.       |
| `start_date`      | yes      |                | `MM/DD/YYYY` or `YYYY-MM-DD`.                                          |
| `end_date`        | yes      |                | Same format as `start_date`.                                           |
| `resume`          | no       | `true`         | Jefferson only — reuse downloaded docs/OCR if present.                 |
| `pva_cross_check` | no       | `true`         | Lis Pendens only — append a Jefferson PVA verification URL.            |
| `search_mode`     | no       | `auto`         | Jefferson only — `direct`, `browser`, or `auto` (direct + fallback).   |
| `ingest_url`      | yes      |                | Lovable ingest endpoint.                                               |
| `ingest_token`    | yes      |                | Bearer token for the ingest endpoint.                                  |

The workflow always runs the dispatcher (`python -m scrapers.run_source`),
which picks the right scraper for `source_type` and writes a
`source_meta.json` so the upload step knows which CSV/schema to use.

## Local CLI examples

```bash
# Jefferson Lis Pendens (existing behavior)
python -m scrapers.run_source --source-type lis_pendens \
  --start-date 05/07/2026 --end-date 05/08/2026 \
  --output-dir scraper_output --search-mode auto --resume --pva-cross-check

# Jefferson Wills (same site, instrument type WI)
python -m scrapers.run_source --source-type wills \
  --start-date 2026-05-01 --end-date 2026-05-08 \
  --output-dir scraper_output

# Louisville code violations (ArcGIS)
python -m scrapers.run_source --source-type louisville_code_violations \
  --start-date 2026-05-01 --end-date 2026-05-08 \
  --output-dir scraper_output

# Indianapolis code violations (scaffold — writes empty CSV today)
python -m scrapers.run_source --source-type indianapolis_code_violations \
  --start-date 2026-05-01 --end-date 2026-05-08 \
  --output-dir scraper_output
```

You can also invoke each scraper directly:

```bash
# Direct Louisville query
python -m scrapers.louisville_code_violations \
  --start-date 2026-05-01 --end-date 2026-05-08 \
  --output-dir scraper_output

# Direct Jefferson scraper with explicit instrument code
python jefferson_lis_pendens_scraper.py \
  --start-date 2026-05-01 --end-date 2026-05-08 \
  --output-dir scraper_output \
  --instrument-code "WI " --instrument-label "WILLS" \
  --csv-name wills_results.csv --skip-validation
```

## Source details

### Jefferson Deeds (Lis Pendens / Wills)

Posts to `https://search.jeffersondeeds.com/p6.php` with `searchtype=ITYPE`.
The instrument type is set via `itype1`:

- `LP ` (with trailing space) → Lis Pendens (`--instrument-label "LIS PENDENS"`)
- `WI ` (with trailing space) → Wills (`--instrument-label "WILLS"`)

If the deeds site changes the dropdown code for Wills, update
`scrapers/run_source.py::_jefferson_command` (search for `instrument_code = "WI "`).

The PVA cross-check and the canned 2026 benchmark validation report are
Lis-Pendens-specific and are skipped automatically for the Wills source.

### Louisville code violations (ArcGIS)

Uses the official Louisville Metro feature service:

```
https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/PM_SiteVisit_Violations/FeatureServer/0/query
```

- Date filter is on `G6A_G6_COMPL_DD` using ArcGIS `TIMESTAMP 'YYYY-MM-DD HH:MM:SS'`
  literals; the end date is widened to 23:59:59 so the range is inclusive.
- Pagination uses `resultOffset` + `resultRecordCount` (1000/page) and stops
  when ArcGIS clears `exceededTransferLimit`.
- Each feature is mapped to the canonical schema and a structured sidecar
  JSON file (`louisville_code_violations_records.json`) so `upload_results.py`
  can populate `instrument_number` from `B1_ALT_ID` and `filing_date` from the
  ArcGIS epoch-ms date without re-parsing the CSV.

### Indianapolis code violations (scaffold)

Source: `https://aca-prod.accela.com/INDY/Cap/CapHome.aspx?module=Enforcement&TabName=HOME`

Status: **scaffold only.** Accela Citizen Access is an ASP.NET WebForms app
that requires a real browser session (Playwright) and selectors that drift
over time. The scaffold module:

- accepts the same CLI as the other scrapers,
- writes a valid (empty) canonical CSV so the workflow's upload step succeeds
  rather than failing the run,
- logs a clear "not yet implemented" message.

To finish the implementation, see the docstring at the top of
`scrapers/indianapolis_code_violations.py` — it lists the exact steps,
selectors, and pagination notes.

## Tests

```bash
python -m unittest discover -s tests -v
```

Covers:

- Louisville feature → canonical row transform (full address, partial address,
  citation amount handling, parcel/notes).
- ArcGIS epoch-ms → ISO date helper, including null/garbage inputs.
- ArcGIS `TIMESTAMP` where-clause format.
- `upload_results.py` schema routing and record parsing for both Jefferson
  and Louisville schemas, including the structured sidecar.
- `--start-date` / `--end-date` parsing for both `MM/DD/YYYY` and `YYYY-MM-DD`.

## Lovable backend / UI changes required

The GitHub workflow now accepts a new optional input, `source_type`. For the
Lovable side to use it:

1. **UI** — add a "Source" dropdown to the run-creation form with options:
   - Jefferson Lis Pendens (`lis_pendens`) — default
   - Jefferson Wills (`wills`)
   - Louisville Code Violations (`louisville_code_violations`)
   - Indianapolis Code Violations (`indianapolis_code_violations`)
2. **Backend / dispatch** — when triggering the GitHub workflow via
   `workflow_dispatch`, include the chosen value as `inputs.source_type`. If
   omitted, the workflow defaults to `lis_pendens`, so existing flows keep
   working.
3. **Ingest endpoint** — the JSON payload now contains three new top-level
   fields:
   - `source_type` (one of the values above)
   - `source_label` (human-readable, e.g. "Louisville Code Violations")
   - `source_schema` (`jefferson_deeds`, `louisville_code_violations`, or
     `indianapolis_code_violations`)
   The `summary` block also includes `source_type`. Existing fields
   (`run_id`, `status`, `records`, `files`, `summary.total_records`, etc.)
   are unchanged.
4. **Records** — the `records[]` shape is identical across sources
   (`run_id`, `filing_date`, `instrument_number`, `parties`, `property_address`,
   `pdf_link`, `notes`, `pva_verification_link`). `pva_verification_link` is
   `null` for non-Jefferson sources. For Louisville, `instrument_number` is
   the ArcGIS `B1_ALT_ID`; if your DB has a unique constraint on
   `(run_id, instrument_number)` it will continue to work without changes.
5. **Status updates** — `update_run.py` is unchanged; the "running" message
   now includes the source type, e.g.
   `"GitHub Actions worker started (source: louisville_code_violations)."`
