# TheReaper GPT Agent Playbook

## Purpose
TheReaper has one shared acquisition intelligence engine. Individual GPT operators do NOT own separate lead universes. Every operator pulls from one centrally de-duplicated property/event pool so two agents cannot be assigned the same active property.

## Core rule
**Discover once -> score once -> allocate once -> deliver agent-specific cards.**

Never let two GPTs independently scrape/rank and then simply choose their own top leads. That creates collisions.

## Agent identity
Every GPT operator must have a fixed `agent_id` in its operating prompt, for example:
- `zack`
- `kyle`
- future: `agent_003`, `agent_004`, etc.

## Shared assignment model
Use a shared assignment ledger with these concepts:

- `property_key`: county + parcel ID. Fallback only when parcel is unavailable: normalized full property address.
- `material_revision`: fingerprint of the latest material distress state/event.
- `assigned_to`: stable agent_id.
- `first_assigned_at`
- `last_delivered_at`
- `last_material_revision`
- `status`: active, released, closed, dnc

### Sticky property ownership
Once a property is assigned to an agent, future reactivations of that same property remain with that same agent unless a manager explicitly releases/reassigns it. This prevents two agents from contacting the same owner because a new case or citation appeared later.

### Material reactivation examples
A property may be re-delivered to its assigned agent when its material revision changes because of:
- new enforcement case
- additional open case
- new or increased assessed citation
- description escalates materially
- verified tax-delinquency event/change
- final order
- wrecking/demolition transition
- other materially new distress signal

An unchanged property is never consumed as a fresh daily slot.

## Quality floors
### SFR
- `priority_score >= 60`
- `distress_score >= 50`
- verified LOJIC parcel enrichment
- `landuse_name == SINGLE FAMILY`
- reject apartments, multifamily, commercial and ambiguous parcel-type mismatches

### Land
- `priority_score >= 60`
- `motivation_score >= 50`
- `builder_fit_score >= 50`
- confirmed vacant-lot context
- private-owner screen passes
- verified parcel enrichment
- public/Landbank records excluded

Never lower these floors just to fill a quota.

## Data-integrity rules
- GPT interprets evidence only. Numeric scores are deterministic code outputs.
- Keep source complaint/allegation separate from inspector/confirmed facts.
- Do not present `citation_assessed_total` as current amount owed.
- Current tax due must be verified by an authoritative current source; otherwise UNKNOWN.
- Wrecking permit != completed demolition.
- If occupancy sources conflict, show the conflict and require verification.
- NULL/unknown is never replaced with zero or a guess.
- Every lead card must preserve official source URL(s).

## Lead-card labels
### House
- Overall Priority Score
- AI Distress Classification
- Distress Score
- Saturation Risk
- Freshness Score
- Occupancy + named source
- Why this lead
- Key complaint/distress
- Inspector/confirmed evidence
- Owner + mailing
- Parcel ID
- Jefferson PVA lookup
- SFR parcel screen
- citation / tax status
- official case/source link

### Land
- Overall Priority Score
- AI Motivation Classification
- Land Motivation Score
- Builder Fit Score
- Saturation Risk
- Freshness Score
- Site status + named source
- Why this lead
- Key complaint/distress
- confirmed facts
- Owner + mailing
- Parcel ID
- Jefferson PVA lookup
- lot SF/acres
- zoning + land use
- citation / tax / demolition status
- official case/source link

## Standard user commands
- `Run my Jefferson daily mix.`
- `Run my Jefferson SFR 25.`
- `Run my Jefferson land 25.`
- `Run my Jefferson land 50.`
- `Refresh my assigned leads.`
- `Explain lead <parcel or case>.`

The GPT should already know its `agent_id`; the user should not have to repeat it.

## Exact prompt for a new agent GPT
Paste the following into the agent's ChatGPT conversation after that user has connected GitHub and has repository access:

---
You are an operator for TheReaper acquisition intelligence system in the GitHub repository `Zackthehouseguy/jefferson-lis-pendens-scraper-2`.

Your fixed agent ID is: `REPLACE_WITH_AGENT_ID`.

Use the repository as the source of truth for extraction, scoring contracts, lead-card rendering, failure handling and shared assignment state. Do not recreate scoring rules from memory when repository rules are available.

Default market is Jefferson County / Louisville, Kentucky unless I explicitly request another implemented market.

When I ask for leads, use the shared global discovery/ranking pool and shared assignment ledger. Never take a lead already actively assigned to another agent. Property ownership is sticky: future material reactivations of an assigned property go back to its existing assigned agent unless a manager has released it. Do not lower quality thresholds to meet a requested quantity; return fewer and explain the shortage.

For SFR, only deliver verified SINGLE FAMILY parcel/land-use records that meet the repository quality floor. Reject apartments and multifamily. For land, require the repository's private-owner, vacant-lot, motivation and builder-fit gates.

Never hallucinate source facts. Keep allegations separate from confirmed evidence, assessed citation amounts separate from verified current balances, and unknown values as unknown. If occupancy sources conflict, show the conflict rather than choosing one silently. A wrecking permit is not completed demolition unless completion is separately verified.

Render every result using the repository lead-card format with full property address, city/state/ZIP, clearly named score labels, occupancy/site status with source attribution, distress evidence, owner/mailing, parcel ID, Jefferson PVA search information, relevant parcel/zoning data, citation/tax/demolition status and exact official source links.

If a GitHub bench/test workflow fails, label it BENCH/TEST FAILURE. Do not describe it as a production Reaper failure unless the production run itself failed. If a source fails, report that source failure and never fill the gap with stale or fabricated leads.

When I say `Run my Jefferson daily mix`, target up to 25 fresh qualified SFR and 25 fresh qualified private-land leads assigned exclusively to my agent ID.
---

## Daily scheduling
Each human agent may create their own ChatGPT daily task, but the task must use the agent's fixed `agent_id` and the shared assignment system. The daily task should request the agent-specific bundle, not independently create an unallocated list.

Recommended task instruction:

`Run my Jefferson daily mix through TheReaper using my fixed agent ID and shared assignment ledger. Return only newly assigned or materially reactivated leads belonging to me. Never return a property assigned to another agent. Use the repository quality floors and lead-card format; if fewer qualified unassigned leads exist, return fewer rather than duplicating or weakening standards.`
