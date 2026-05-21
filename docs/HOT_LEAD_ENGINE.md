# Hot Lead Engine

This document describes how raw scraper output becomes a ranked, deduped,
AI-annotated lead list inside the Lovable app. It is intentionally
implementation-light — the scrapers in this repo only need to produce
records that conform to `docs/SCRAPER_SPEC.md`. The rest of the pipeline
lives on the app side, but the rules here are the **contract** the scrapers
should keep in mind when choosing what to emit and what to skip.

## 1. Pipeline overview

```
scrape  →  normalize  →  enrich / verify  →  dedupe  →  stack signals  →  score  →  AI Lead Insight  →  export / follow-up
```

1. **Scrape.** Each adapter pulls fresh records for a date range from one
   lead source (see `docs/SOURCE_ADAPTERS.md`).
2. **Normalize.** Records are coerced into the canonical schema from
   `docs/SCRAPER_SPEC.md` §3.2. Addresses are upper-cased and trimmed; money
   is numeric; dates are ISO.
3. **Enrich / verify.** Per-lead lookups against the verification sources
   (PVA, qPublic, Sheriff, LOJIC) fill in owner mailing address, assessed
   value, tax status, GIS context. Enrichment is **opt-in per lead** — not
   bulk crawled.
4. **Dedupe.** See §3 below.
5. **Stack signals.** Multiple records on the same property are merged into
   a single lead carrying a list of distress signals (e.g. lis pendens **and**
   delinquent taxes **and** an open code violation).
6. **Score.** See §2 below.
7. **AI Lead Insight.** See §4 below.
8. **Export / follow-up.** CSV download, mailers, and follow-up tasks.

## 2. Lead scoring

Leads are bucketed into one of four tiers. Tiers are recomputed every time
new signals attach to a property.

| Tier            | When                                                                                                  |
|-----------------|-------------------------------------------------------------------------------------------------------|
| **Hot**         | Sale-stage distress (lis pendens with hearing/judgment, tax sale scheduled), or any **stacked** signals (2+ independent distress types), or a single severe signal (e.g. condemned, structural code violation). |
| **Warm**        | One strong but not-yet-sale-stage signal: open code violation with real distress codes, fresh lis pendens filing, probate (Wills) on a property with prior tax issues. |
| **Needs Review**| Ambiguous — single moderate signal, possible owner-mismatch after dedupe, or enrichment incomplete.   |
| **Archive**     | Closed cases with no recurrence, paid-off tax bills, expired filings, rental-registration-only code rows. |

### Tax-only rule

A property whose **only** signal is delinquent taxes is **not Hot** by
default. Tax delinquency alone is too noisy — most of those bills get paid.
Promote a tax-only lead to Hot only when at least one of the following is
true:

- The bill is at or near **sale stage** (about to be sold to a third-party
  purchaser per the Clerk's calendar).
- The amount is **severe** for the assessed value (rule of thumb: ≥10% of
  assessed value, or multi-year accumulation).
- The trend is **worsening** (new unpaid year stacks on prior unpaid years).
- It is **stacked** with another signal (lis pendens, code violation,
  probate, landbank, etc.). At that point it stops being a tax-only lead.

Adapters should not pre-decide tier; they should emit faithfully and let the
scoring layer apply these rules.

## 3. Dedupe rules

The same physical property must collapse to one lead even when it appears in
multiple sources, multiple times within a source, or under slightly
different owner names. Dedupe is applied in this order:

1. **`parcel_id` exact match.** When both records carry a parcel ID, that is
   authoritative — same parcel = same lead.
2. **Normalized `property_address` match.** Upper-case, trimmed, with unit
   designators and directional prefixes normalized (`N` ↔ `NORTH`,
   `STREET` ↔ `ST`, etc.). Apartment / unit suffix matters: `123 MAIN ST
   APT 4` is a different lead than `123 MAIN ST`.
3. **`owner_name` + normalized address.** Use this only when the parcel ID
   is missing on one side. Helps catch the same household across sources
   that print addresses slightly differently.
4. **High-confidence fuzzy match.** Token-set ratio ≥ ~92 on both address
   and owner, with no conflicting parcel IDs. Anything weaker is **not** a
   dedupe — it goes to Needs Review with a "possible duplicate" flag for a
   human.

When two records merge, keep the earliest `signal_date` per signal type and
union the `raw` blobs under their source slugs. Never silently drop a signal.

## 4. AI Lead Insight

Each lead carries an LLM-generated "Lead Insight" block. It must include
three things and nothing else that isn't grounded in the underlying record:

- **Why motivated.** A short narrative of why this owner is likely
  motivated to sell or settle — derived only from the actual signals on the
  lead.
- **Proof / signals.** A bullet list of the concrete records that support
  the narrative: each bullet cites the source, the date, and the case /
  instrument / bill number when present.
- **Recommended next action.** One concrete next step (call the owner, pull
  the PVA card, check the Clerk's sale calendar, send a mailer, etc.),
  matched to the lead's tier and the available contact info.

### No-hallucination rules

- Never invent a signal that isn't in the lead's record set. If the lead has
  a lis pendens but no tax data, the insight must not assert anything about
  taxes.
- Never invent an owner phone number, email, or relationship.
- Quote dates and amounts straight from the normalized record. Round only
  if you also show the source value (e.g. "$1,842 owed").
- If the signals are too thin to support a confident narrative, say so
  ("Single moderate signal — verify before outreach") rather than padding.

## 5. Deterministic `lead_id`

Lead IDs must be **deterministic** so that re-ingesting the same record
does not create a duplicate row. The dedupe step relies on this for safe
re-runs.

Recommended construction, in order of preference:

1. `sha1(source || ':' || case_number)` when `case_number` is reliably
   unique within a source (lis pendens instrument numbers, code-violation
   case IDs, tax bill numbers).
2. `sha1(source || ':' || parcel_id || ':' || signal_date)` when there is no
   case number but there is a parcel.
3. `sha1(source || ':' || normalized_address || ':' || signal_date)` as a
   last resort.

### Why this matters: the instrument-number lesson

We have hit duplicate-ingest failures when two records from the same source
landed on the same `instrument_number` with slightly different metadata
(e.g. a re-pulled lis pendens row whose `parties` string changed after an
OCR retry). With a deterministic `lead_id`, the upsert path is idempotent
and the second ingest updates the existing lead instead of failing the run.

Adapters should:

- Emit `case_number` whenever the source provides one, in its raw form
  (don't re-pad zeros, don't drop suffixes) so that the hash is stable
  across runs.
- Treat re-fetched rows as **updates** to the same logical record, not new
  records. If the upstream changes the case number itself, that is a new
  record by definition.
