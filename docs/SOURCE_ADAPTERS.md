# Source Adapters

Catalog of every data source the Lovable property-signal app talks to, and
**how each one is allowed to be used**. The split into _lead sources_,
_verification / enrichment sources_, and _skip / future_ is load-bearing —
the app UI exposes these categories differently:

- **Scrape new leads** → lead sources only.
- **Verify / enrich existing leads** → verification sources only.
- **Reference / docs** → everything else.

A verification source must never be exposed as a bulk lead source, even if a
crawl is technically possible. Many of them are pay-per-lookup, slow, or
explicitly prohibit bulk extraction.

---

## 1. Source map

| Slug                              | Class         | Tier | Creates leads? | Notes                                                                 |
|-----------------------------------|---------------|------|----------------|-----------------------------------------------------------------------|
| `lis_pendens`                     | Lead          | P0   | Yes            | Jefferson Deeds, instrument code `LP `. Live in this repo.            |
| `wills`                           | Lead          | P0   | Yes            | Jefferson Deeds, instrument code `WIL`. Live in this repo.            |
| `louisville_code_violations`      | Lead          | P0   | Yes            | ArcGIS FeatureServer. Live in this repo.                              |
| `jefferson_delinquent_taxes`      | Lead          | P0   | Yes            | Jefferson County Clerk delinquent tax list. Live in this repo.        |
| `louisville_landbank`             | Lead          | P1   | Yes            | Public inventory of Landbank-held parcels.                            |
| `fayette_clerk_tax_sale`          | Lead          | P1   | Yes            | **Placeholder** — Clerk URL discovery still needed.                   |
| `jefferson_pva`                   | Verification  | —    | No             | Look up parcel/owner detail given an address or PIDN.                 |
| `fayette_pva`                     | Verification  | —    | No             | Look up Fayette parcel/owner detail.                                  |
| `fayette_qpublic`                 | Verification  | —    | No             | Alternate Fayette parcel viewer (Schneider qPublic).                  |
| `fayette_sheriff`                 | Verification  | —    | No             | Property tax lookup by bill / parcel — **not** the bulk list.         |
| `lojic`                           | Verification  | —    | No             | Louisville/Jefferson GIS. Map and parcel context, not bulk pulls.     |
| `indianapolis_code_violations`    | Skip / future | —    | No             | Accela WebForms; scaffold only. Defer until clearly prioritized.      |
| _PVA bulk crawls (any county)_    | Skip          | —    | No             | High legal/ToS risk and noisy; pursue official datasets instead.      |
| _Pay-per-lead APIs_               | Skip          | —    | No             | Out of scope for the open scraper repo.                               |

`lis_pendens`, `wills`, `louisville_code_violations`, and
`jefferson_delinquent_taxes` are the four lead sources implemented today.
`louisville_landbank` and `fayette_clerk_tax_sale` are the next two on the
P1 list and should follow the same shape.

## 2. Lead sources vs. verification sources

**Lead sources** are bulk pulls. They produce one or more records per run
without needing any prior knowledge — give them a date range and they emit
properties. The Lovable UI surfaces these under "Scrape new leads", and they
populate the lead table.

**Verification / enrichment sources** require an **existing lead** as input:
a property address, a parcel ID, a bill number, or similar. They are run
one-record-at-a-time from a lead detail page (the "Verify / enrich existing
leads" surface), or in small batches keyed by lead ID. They must not be
exposed as a bulk source type in the run-creation form, and the dispatcher
must not accept a date-range-only invocation for them. If a verification
adapter is added to this repo, it should be invoked as
`python -m scrapers.verify_<source> --parcel <id>` (or `--address`,
`--bill-number`), not via the lead-scraping workflow.

If a future source could plausibly support both modes (e.g. an open dataset
that we _could_ bulk-crawl but currently only hit per-lead), pick one mode
and document it. Do not let a single adapter silently switch between bulk
and per-lead behavior based on its flags.

## 3. Known URLs

Authoritative landing pages for each source. Keep this list in sync when a
county redesigns its site — broken URLs here usually mean a broken adapter
within a release or two.

| Source                                  | URL                                                                                                                                                 |
|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| Jefferson Clerk delinquent taxes        | <https://www.jeffersoncountyclerk.org/delinquenttaxes/>                                                                                              |
| Jefferson PVA                           | <https://jeffersonpva.ky.gov/property-search/>                                                                                                       |
| Louisville code violations (Data.gov)   | <https://catalog.data.gov/dataset/louisville-metro-ky-property-maintenance-inspection-violations/resource/bd0155ac-25e5-4146-82ea-9a8980e1bfde>     |
| Louisville Landbank                     | <https://louisvillelandbank.org>                                                                                                                     |
| Fayette Sheriff property-tax lookup     | <https://www.fayettesheriff.com/property_taxes_lookup.php>                                                                                           |
| Fayette PVA                             | <https://fayettepva.com>                                                                                                                             |
| Fayette qPublic                         | <https://qpublic.net/ky/fayette/search1.html>                                                                                                        |
| Kentucky DOR — delinquent tax overview  | <https://revenue.ky.gov/Property/Pages/Delinquent-Property-Tax.aspx>                                                                                 |

### Fayette Sheriff is verification-only

The Fayette Sheriff property-tax lookup is a **per-bill lookup**, not the
bulk delinquent-tax list. It is the correct tool for "given this lead, what
does the Sheriff show?" but it is the wrong tool for "give me every
delinquent property in Fayette County this week."

The real bulk Fayette delinquent list is published by the **Fayette County
Clerk** and is the target of the `fayette_clerk_tax_sale` lead source. The
Clerk URL is still being discovered; until then the lead source remains a
placeholder and must not silently fall back to the Sheriff lookup. The
Kentucky DOR overview link above is useful background on how the statewide
process works (third-party purchasers, sale timing) and explains why the
Clerk's list — not the Sheriff's lookup — is the lead-grade source.

## 4. Adding a new source

1. Decide its class (lead / verification / skip) before writing any code.
2. If it's a lead source, follow `docs/SCRAPER_SPEC.md` end-to-end and
   register it in `scrapers/run_source.py`.
3. If it's a verification source, build it as a per-lead lookup with no
   date-range entry point, and wire it into the enrichment UI rather than
   the run-creation form.
4. Update the table in §1 of this file in the same PR that adds the adapter.
