# TheReaper — GPT Operator Commands

Current implemented default market: **Jefferson County / Louisville, Kentucky**.

## Natural-language commands

### `Run Jefferson daily mix`
Default acquisition command. Return up to **25 high-score single-family residential leads + 25 high-score private land leads**.

Requirements:
- SFR lane: LOJIC land-use screen must equal `SINGLE FAMILY`; exclude multifamily/apartments and ambiguous non-SFR parcels.
- Land lane: confirmed vacant-lot source context, private-owner screen, parcel verification, and strong acquisition ranking.
- Default score floor: `priority_score >= 60` (`STRONG` or `CALL FIRST`).
- Prefer highest priority first.
- Do not fill a quota with weak leads. If fewer than 25 qualify, return fewer and say why.

### `Run Jefferson SFR 25`
Return up to 25 fresh high-score SFR leads only.

### `Run Jefferson land 25`
Return up to 25 fresh high-score private land leads only.

### `Run Jefferson land 50`
Run the proven 50-land heavy batch and return the ranked high-score results.

### `Run Jefferson refresh`
Re-check previously delivered properties for material new distress events. Do not create a new duplicate property merely because the same parcel appears again.

### `Explain lead <parcel or case>`
Return the full evidence trail, scoring breakdown, source links, and which facts are confirmed vs reported/speculative.

## Freshness / repeat rules

Permanent identity is parcel-first, normalized address fallback.

An unchanged property should not consume a new-lead slot. Reactivate the same property when a material event changes, including:
- a new enforcement case,
- an additional unresolved case,
- a new citation event,
- a materially escalated complaint/inspection finding,
- a verified tax-delinquency event,
- a final order,
- a wrecking/demolition transition,
- another new public distress signal.

Recommended event fingerprint:
`county + parcel_id + case_number + event_date + citation_assessed_total + material_evidence_hash`

## Lead-card contract

Every returned lead card must include, when available:
- full property address,
- property type,
- occupancy (`Vacant`, `Occupied`, `Unknown`, or `Vacant lot`),
- priority score,
- distress/motivation score,
- saturation risk,
- freshness,
- concise reason the lead ranked,
- key complaint/distress evidence,
- owner and owner mailing,
- parcel ID,
- official Jefferson PVA property-search link plus parcel ID,
- land lot size/zoning/land use where applicable,
- citation assessed amount, if actually extracted,
- verified current balance only if an authoritative source verifies it,
- tax status only when verified,
- demolition status only when verified,
- exact official source URL.

Jefferson PVA search: `https://jeffersonpva.ky.gov/property-search/`

## Anti-hallucination rules

1. AI interprets evidence; deterministic code owns numeric scores.
2. Never call an assessed citation the current amount owed unless an authoritative current-balance source verifies it.
3. `null` / unavailable means **unknown**, never zero.
4. Complaint allegations stay labeled as reports unless independently confirmed.
5. Owner mailing different from situs is a possible absentee indicator, not proof of absentee ownership.
6. A wrecking permit is a transition signal, not proof demolition is complete.
7. PVA/ownership records may lag; preserve source and provenance.
8. On source failure, return the failure state rather than fabricated or stale substitute facts.
9. Preserve official source URLs on every lead.
10. If a parser loses required fields or source quality degrades, stop the batch rather than write garbage.

## Scale rule

Scan broad structured sources cheaply. Deep-browser-enrich only narrowed candidates. Current proven heavy-batch size is **50 deeply enriched records per lane/run**; do not interpret that as a 50-row upstream scan limit.
