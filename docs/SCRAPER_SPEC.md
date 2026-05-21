# Scraper Spec

This document is the **source of truth** for the Python-side scraper contract
used by every adapter in this repository. The Lovable app maintains a mirrored
copy at `.lovable/plan.md` in the app repo; if the two ever disagree, this
file wins for anything that touches scraper runtime, output schema, or QA.

The contract has two goals:

1. Every scraper, regardless of source, produces records that the Lovable
   ingest endpoint can consume **without per-source branching**.
2. A new contributor can stand up a new adapter by copying an existing module
   and following this spec.

---

## 1. Runtime

- **Python**: 3.11 or newer.
- **Allowed libraries** (no others without prior discussion):
  - `requests`, `httpx` — HTTP clients
  - `beautifulsoup4`, `lxml` — HTML parsing
  - `playwright` — **only** when the target page is JS-rendered and cannot be
    fetched via plain HTTP (ASP.NET WebForms, SPA-only flows, etc.)
  - `pandas` — tabular shaping and CSV/JSON export
- **Optional, source-specific**: `pytesseract` / `pdfminer.six` are acceptable
  for OCR-only steps already in use (Jefferson Deeds); do not introduce them
  for new sources without a strong reason.

Pin every new dependency in `requirements.txt`.

## 2. Invocation

Each adapter must be runnable as a **standalone script**:

```bash
python scrape_<source>.py --out results.json
```

Adapters in this repo also expose themselves through the unified dispatcher:

```bash
python -m scrapers.run_source --source-type <source> \
  --start-date 2026-05-01 --end-date 2026-05-08 \
  --output-dir scraper_output
```

Both entry points must accept, at minimum:

| Flag           | Purpose                                                              |
|----------------|----------------------------------------------------------------------|
| `--out`        | Output JSON file path (standalone mode).                             |
| `--output-dir` | Directory for CSV + JSON sidecar (dispatcher mode).                  |
| `--start-date` | Inclusive start, `MM/DD/YYYY` or `YYYY-MM-DD`.                       |
| `--end-date`   | Inclusive end, same formats.                                         |
| `--limit N`    | Cap on records emitted; for smoke tests and CI. **Required.**         |

`--limit` exists so a developer can run the scraper end-to-end in seconds
without hitting the live source hard. It must short-circuit *after* a record
has been fully normalized, not mid-record.

## 3. Output

### 3.1 Format

- **One JSON array per run**, written to `--out` (standalone) or to a
  `*_records.json` sidecar in `--output-dir` (dispatcher).
- One object per property/record. Never one object per HTML row when multiple
  rows describe the same property — dedupe at the scraper.
- UTF-8, no BOM.
- The dispatcher may additionally write a source-specific CSV for human
  review. The JSON sidecar is authoritative for ingest.

### 3.2 Required fields

Every record **must** include every key below. Use `null` (not `""`, not
`"N/A"`) when a value is genuinely unknown. Never fabricate.

| Field                    | Type            | Notes                                                                                  |
|--------------------------|-----------------|----------------------------------------------------------------------------------------|
| `source`                 | string          | Stable slug, e.g. `lis_pendens`, `jefferson_delinquent_taxes`.                          |
| `source_url`             | string          | URL of the record on the originating site (or closest landing page).                   |
| `scraped_at`             | string (ISO-8601, UTC) | When this record was produced, e.g. `2026-05-21T14:30:00Z`.                     |
| `parcel_id`              | string \| null  | Normalized parcel/PIDN if the source provides one.                                     |
| `property_address`       | string \| null  | Full street address as displayed; do not invent ZIP/city components.                   |
| `owner_name`             | string \| null  | Primary owner / defendant / taxpayer as named by the source.                           |
| `owner_mailing_address`  | string \| null  | Mailing address when distinct from the property address.                               |
| `signal_type`            | string          | Canonical distress signal: see §3.3.                                                   |
| `signal_date`            | string (date)   | `YYYY-MM-DD` for the event (filing date, sale date, citation date, etc.).              |
| `amount_owed`            | number \| null  | USD, no currency symbol or commas. Use the most specific available figure.             |
| `case_number`            | string \| null  | Case / instrument / citation number from the source.                                   |
| `raw`                    | object          | Verbatim source fields kept for auditing. Never mutate to fit the schema.              |

### 3.3 `signal_type` vocabulary

Use one of these exact slugs so downstream scoring is consistent:

- `lis_pendens`
- `tax_delinquent`
- `tax_sale`
- `code_violation`
- `probate` (Wills)
- `landbank_inventory`
- `clerk_tax_sale`
- `other` — only as a last resort; include a `raw.signal_subtype` so we can
  refine the taxonomy later.

### 3.4 Example record

```json
{
  "source": "jefferson_delinquent_taxes",
  "source_url": "https://www.jeffersoncountyclerk.org/delinquenttaxes/",
  "scraped_at": "2026-05-21T14:30:00Z",
  "parcel_id": "012345670000",
  "property_address": "123 MAIN ST, LOUISVILLE KY 40202",
  "owner_name": "DOE JOHN",
  "owner_mailing_address": "PO BOX 1, LOUISVILLE KY 40201",
  "signal_type": "tax_delinquent",
  "signal_date": "2026-04-15",
  "amount_owed": 1842.55,
  "case_number": "2025-0001234",
  "raw": {
    "bill_number": "2025-0001234",
    "tax_year": "2025",
    "status": "Unpaid"
  }
}
```

### 3.5 Example output (full file)

```json
[
  { "...record 1..." },
  { "...record 2..." }
]
```

The file must be a valid JSON array even when empty (`[]`), so downstream
ingest never has to special-case the no-results path.

## 4. Polite scraping

- **Rate limit**: at most **1 request per second** to a given host. Use a
  shared sleep / token bucket; do not parallelize requests against the same
  source.
- **Retries**: exponential backoff with jitter for transient 5xx and
  connection errors. Cap at ~5 attempts. Do not retry 4xx (other than 429,
  which should back off harder).
- **User-Agent**: send a real, identifiable UA. No spoofing browser strings.
  Suggested form:
  `LovablePropertySignalBot/1.0 (+contact: ops@lovable.example)`.
- **robots.txt**: check on first request to a new host. If a path is
  disallowed for our UA, do not scrape it — escalate instead.
- **Caching**: when a source publishes the same page on every run (e.g. a
  parcel detail), prefer ETag / `If-Modified-Since` over re-downloading.

## 5. Logging

- Progress and errors go to **stderr**, never stdout (stdout is reserved for
  the JSON payload in standalone mode).
- Use structured, one-line messages: timestamp, level, source slug, message.
- Log at least: run start (with params), every page/offset fetched, every
  record skipped (with reason), run end with counts.

## 6. Error handling

- **Row-level errors are skipped, not fatal.** A single malformed detail
  page must never crash the entire run. Log it (`level=warning`, include the
  source_url or row key), continue.
- **Run-level errors** (auth failure, the entire site is down, schema drift
  detected) should exit non-zero **after** flushing whatever records were
  successfully gathered to `--out`. Never lose partial results.
- Detect schema drift early: if a required selector returns nothing on the
  first page, raise loudly rather than silently emitting empty records.

## 7. QA checklist for each source

Before merging a new or modified adapter, confirm:

- [ ] `python scrape_<source>.py --out /tmp/x.json --limit 5` returns within
      a few minutes and produces a valid JSON array.
- [ ] Every record contains all required fields from §3.2; missing values are
      `null`, not empty strings.
- [ ] `signal_type` is from the vocabulary in §3.3.
- [ ] `signal_date` parses as `YYYY-MM-DD`.
- [ ] `amount_owed` is numeric (no `$`, no `,`) or `null`.
- [ ] `raw` contains enough of the source fields that we could re-derive the
      normalized values if the schema changes.
- [ ] Polite-scraping settings (§4) are in place: rate limit, UA, retries.
- [ ] Logs are on stderr and include start/end counts.
- [ ] A row with intentionally malformed data does not crash the run.
- [ ] `--limit` actually caps output.
- [ ] Unit tests cover at least the source → canonical record transform and
      any date / money parsing.
- [ ] README and `docs/SOURCE_ADAPTERS.md` are updated if the source's
      classification (lead / verification / skip) changes.
