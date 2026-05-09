"""Wills-specific smart-field extraction.

Reads OCR text for a Jefferson County WIL (will) filing and pulls a small
set of high-signal fields used by downstream lead workflows:
  - Decedent
  - Date of death
  - Property address (delegates to the existing OCR address extractor)
  - Surviving spouse
  - Beneficiary / heir / devisee
  - Complexity flag (Simple | Needs Review | Avoid - Trust/Complex)
  - Reasons for the complexity flag
  - Confidence and human-readable notes

Design rules:
  - No hallucination. Every field defaults to "Unknown" when the OCR text
    does not contain an unambiguous match.
  - Heuristics only. We do not call out to any LLM and do not invent text
    that is not visible in the provided OCR/parties/legal-description
    inputs.
  - Pure function. The caller is responsible for OCR and address
    extraction; we only read what we are given.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


UNKNOWN = "Unknown"

COMPLEXITY_SIMPLE = "Simple"
COMPLEXITY_NEEDS_REVIEW = "Needs Review"
COMPLEXITY_AVOID = "Avoid - Trust/Complex"

# Trust / fiduciary cues that flip a will into "avoid" territory. These are
# strong signals — when the text mentions a trust instrument or corporate
# fiduciary the lead is rarely a clean single-heir flip, so we surface that
# up-front rather than burying it in notes.
_TRUST_CUES = (
    "revocable trust",
    "living trust",
    "pour-over",
    "pour over",
    "trust agreement",
    "trustee",
    "successor trustee",
    "co-trustee",
    "in trust",
    "the trust",
    "family trust",
)

# Corporate/professional fiduciaries usually mean the estate is being
# administered through an institution rather than a single named heir.
_CORPORATE_FIDUCIARY_CUES = (
    "bank",
    "trust company",
    "n.a.",
    "n. a.",
    ", n.a",
    "corporation",
    " llc",
    " inc.",
    " inc ",
    " co.",
    " plc",
    "fiduciary",
)

# Words that can precede a name in beneficiary language. Order matters —
# longer / more specific phrases first so they win on overlapping matches.
_BENEFICIARY_CUES = (
    "devise and bequeath to",
    "give devise and bequeath to",
    "give and bequeath to",
    "give and devise to",
    "devise to",
    "bequeath to",
    "give to",
    "leave to",
    "to my beloved wife",
    "to my beloved husband",
    "to my wife",
    "to my husband",
    "to my spouse",
    "to my son",
    "to my daughter",
    "to my child",
    "to my children",
    "beneficiary:",
    "devisee:",
    "heir:",
)


@dataclass
class WillsFields:
    decedent: str = UNKNOWN
    date_of_death: str = UNKNOWN
    property_address: str = UNKNOWN
    surviving_spouse: str = UNKNOWN
    beneficiary_heir_devisee: str = UNKNOWN
    complexity_flag: str = COMPLEXITY_NEEDS_REVIEW
    complexity_reasons: list[str] = field(default_factory=list)
    confidence: str = "low"
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "decedent": self.decedent,
            "date_of_death": self.date_of_death,
            "property_address": self.property_address,
            "surviving_spouse": self.surviving_spouse,
            "beneficiary_heir_devisee": self.beneficiary_heir_devisee,
            "complexity_flag": self.complexity_flag,
            "complexity_reasons": "; ".join(self.complexity_reasons),
            "confidence": self.confidence,
            "notes": "; ".join(self.notes),
        }


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _clean_name(value: str) -> str:
    """Trim trailing punctuation and stop words off a captured name."""
    name = _normalize_whitespace(value)
    # Drop trailing punctuation.
    name = name.strip(" ,.;:'\"-")
    # Cut at common downstream phrases that indicate the name has ended.
    cut_markers = [
        r"\b(of|residing|living|who resides|formerly|now of|deceased|decedent|herein|hereby|then living|surviving|being)\b",
        r"\b(do hereby|does hereby|hereby declare|declare this)\b",
    ]
    for marker in cut_markers:
        match = re.search(marker, name, flags=re.IGNORECASE)
        if match:
            name = name[: match.start()].strip(" ,.;:'\"-")
    # Names should have at least two letters and contain a letter.
    if len(name) < 2 or not re.search(r"[A-Za-z]", name):
        return ""
    # Reject obvious junk like long sentences captured by greedy regexes.
    if len(name) > 80:
        return ""
    return name


def _is_corporate_name(value: str) -> bool:
    lower = (value or "").lower()
    return any(cue in lower for cue in _CORPORATE_FIDUCIARY_CUES)


def _extract_decedent(text: str, parties_hint: str = "") -> str:
    flat = _normalize_whitespace(text)

    # Patterns ordered roughly by reliability. The trailing capture is
    # deliberately greedy-stopped at common end markers so we do not pull in
    # the rest of the sentence.
    patterns = [
        r"\bLast\s+Will\s+and\s+Testament\s+of\s+([A-Z][A-Za-z .,'\-]{2,80}?)(?:\.|,|;|\s+(?:do|does|hereby|residing|of\s+[A-Z]|deceased|decedent))",
        r"\bWill\s+of\s+([A-Z][A-Za-z .,'\-]{2,80}?)(?:\.|,|;|\s+(?:do|does|hereby|residing|of\s+[A-Z]|deceased|decedent))",
        r"\bEstate\s+of\s+([A-Z][A-Za-z .,'\-]{2,80}?)(?:\.|,|;|\s+(?:deceased|decedent|a\s+resident|late\s+of))",
        r"\bDecedent\s*[:\-]\s*([A-Z][A-Za-z .,'\-]{2,80}?)(?:\.|,|;|$)",
        r"\bI,\s*([A-Z][A-Za-z .,'\-]{2,80}?)(?:,|\s+of\s+[A-Z]|\s+being\s+of\s+sound)",
        r"\b([A-Z][A-Za-z .'\-]{2,80}?),\s*deceased\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, flat)
        if match:
            cleaned = _clean_name(match.group(1))
            if cleaned and not _is_corporate_name(cleaned):
                return cleaned

    # Fall back to the grantor list if the OCR did not give us a clean
    # match. Parties on Wills filings are usually decedent (grantor) ->
    # beneficiary (grantee), but only trust this when there is exactly one
    # human-looking name.
    if parties_hint:
        first = parties_hint.split(";")[0].strip()
        if first and not _is_corporate_name(first):
            cleaned = _clean_name(first)
            if cleaned:
                return cleaned

    return UNKNOWN


def _extract_date_of_death(text: str) -> str:
    flat = _normalize_whitespace(text)
    date_re = (
        r"((?:January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+\d{1,2},?\s+\d{4}"
        r"|\d{1,2}/\d{1,2}/\d{2,4}"
        r"|\d{4}-\d{2}-\d{2})"
    )
    cues = [
        rf"\bdate\s+of\s+death\s*[:\-]?\s*{date_re}",
        rf"\bdied\s+on\s+(?:or\s+about\s+)?{date_re}",
        rf"\bdeceased\s+on\s+(?:or\s+about\s+)?{date_re}",
        rf"\bwho\s+died\s+on\s+(?:or\s+about\s+)?{date_re}",
        rf"\bdate\s+of\s+decedent'?s?\s+death\s*[:\-]?\s*{date_re}",
    ]
    for pattern in cues:
        match = re.search(pattern, flat, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip(" ,.")
    return UNKNOWN


def _extract_surviving_spouse(text: str) -> tuple[str, str | None]:
    """Return (spouse, override_complexity_reason).

    The override is set only when the will explicitly says the spouse
    predeceased — that is signal worth surfacing on the row even when no
    spouse name is captured.
    """
    flat = _normalize_whitespace(text)

    predecease_cues = [
        r"\bspouse\s+predeceased\b",
        r"\bwife\s+predeceased\b",
        r"\bhusband\s+predeceased\b",
        r"\bwife\s+(?:is\s+)?deceased\b",
        r"\bhusband\s+(?:is\s+)?deceased\b",
        r"\bmy\s+(?:late|deceased)\s+(?:wife|husband|spouse)\b",
    ]
    predeceased = any(re.search(p, flat, flags=re.IGNORECASE) for p in predecease_cues)

    # If the OCR explicitly says the spouse predeceased, do not also try to
    # extract a "spouse name" — sentences like "My wife predeceased me" will
    # otherwise capture "predeceased me" as a name.
    name_patterns = [] if predeceased else [
        r"\bsurvived\s+by\s+(?:my|his|her)\s+(?:wife|husband|spouse)\s+([A-Z][A-Za-z .'\-]{2,60}?)(?:\.|,|;|\s+and\b|$)",
        r"\bmy\s+(?:wife|husband|spouse),?\s+([A-Z][A-Za-z .'\-]{2,60}?)(?:\.|,|;|\s+who\b|\s+and\b|$)",
        r"\b(?:wife|husband|spouse)\s*[:\-]\s*([A-Z][A-Za-z .'\-]{2,60}?)(?:\.|,|;|$)",
    ]
    # Words to reject as a captured "name" — these are sentence fragments,
    # not people. Match case-insensitively against the cleaned candidate.
    name_blocklist = {
        "predeceased",
        "predeceased me",
        "predeceased him",
        "predeceased her",
        "deceased",
        "is deceased",
    }
    for pattern in name_patterns:
        match = re.search(pattern, flat, flags=re.IGNORECASE)
        if match:
            cleaned = _clean_name(match.group(1))
            if cleaned and not _is_corporate_name(cleaned) and cleaned.lower() not in name_blocklist:
                return cleaned, ("Spouse predeceased per OCR" if predeceased else None)

    if predeceased:
        return UNKNOWN, "Spouse predeceased per OCR"
    return UNKNOWN, None


def _extract_beneficiary(text: str) -> str:
    flat = _normalize_whitespace(text)
    found: list[str] = []
    for cue in _BENEFICIARY_CUES:
        # Match the cue followed by a capitalized name. We do not require a
        # full last name because OCR sometimes drops it.
        pattern = re.escape(cue) + r"\s+([A-Z][A-Za-z .'\-]{2,80}?)(?:\.|,|;|\s+and\b|\s+all\b|\s+the\b|$)"
        for match in re.finditer(pattern, flat, flags=re.IGNORECASE):
            cleaned = _clean_name(match.group(1))
            if not cleaned:
                continue
            if _is_corporate_name(cleaned):
                # Corporate beneficiary is a strong "avoid" signal — leave it
                # for the complexity check, do not record as a clean heir.
                continue
            if cleaned.lower() not in {f.lower() for f in found}:
                found.append(cleaned)
            if len(found) >= 3:
                break
        if len(found) >= 3:
            break
    if not found:
        return UNKNOWN
    return ", ".join(found)


def _classify_complexity(
    text: str,
    decedent: str,
    beneficiary: str,
    property_address: str,
    extra_reasons: list[str],
) -> tuple[str, list[str], str]:
    lower = (text or "").lower()
    reasons: list[str] = list(extra_reasons)

    trust_hits = [cue for cue in _TRUST_CUES if cue in lower]
    if trust_hits:
        reasons.append("Trust language detected: " + ", ".join(sorted(set(trust_hits))))
    corporate_hits = [cue.strip() for cue in _CORPORATE_FIDUCIARY_CUES if cue in lower]
    # Corporate fiduciary alone is not a hard avoid (e.g. "deposited at the
    # county clerk" might trip false positives); only treat it as a reason
    # when paired with fiduciary language nearby.
    if any(re.search(r"(executor|trustee|fiduciary|administrator)[^.]*?(bank|trust company|n\.?a\.?|corporation|llc|inc\.?)", lower) for _ in [0]):
        corporate_match = re.search(r"(executor|trustee|fiduciary|administrator)[^.]{0,80}?(bank|trust company|n\.?a\.?|corporation|llc|inc\.?)", lower)
        if corporate_match:
            reasons.append(f"Corporate fiduciary referenced ({corporate_match.group(0).strip()})")

    if beneficiary == UNKNOWN:
        reasons.append("No clear beneficiary identified")
    if property_address in (UNKNOWN, "", "Address not found"):
        reasons.append("No property address identified")
    if decedent == UNKNOWN:
        reasons.append("No decedent name identified")

    has_avoid_signal = bool(trust_hits) or any(
        r.startswith("Corporate fiduciary referenced") for r in reasons
    )

    if has_avoid_signal:
        flag = COMPLEXITY_AVOID
        confidence = "medium"
    elif (
        decedent != UNKNOWN
        and beneficiary != UNKNOWN
        and property_address not in (UNKNOWN, "", "Address not found")
    ):
        flag = COMPLEXITY_SIMPLE
        confidence = "medium"
        reasons = [r for r in reasons if not r.startswith("No ")]
    else:
        flag = COMPLEXITY_NEEDS_REVIEW
        confidence = "low"

    return flag, reasons, confidence


def extract_wills_fields(
    text: str,
    parties: str = "",
    legal_description: str = "",
    existing_address: str = "",
) -> WillsFields:
    """Extract smart fields from a Jefferson County WIL filing.

    Args:
        text: Combined OCR text for the filing (all pages).
        parties: Optional ";"-separated grantors+grantees from the
            search-result row, used as a low-confidence fallback for the
            decedent name.
        legal_description: Optional legal description from the search
            result. Never used as the property address — passed in only so
            we can record it in notes.
        existing_address: Output of the project's address extractor for
            this filing. We trust the upstream extractor and treat
            "Address not found" / "" as missing.
    """
    fields = WillsFields()

    # Decedent.
    fields.decedent = _extract_decedent(text, parties_hint=parties)

    # Date of death.
    fields.date_of_death = _extract_date_of_death(text)

    # Property address — delegate entirely. We do NOT consult the legal
    # description here; per the spec, legal descriptions are not addresses.
    if existing_address and existing_address.strip() and existing_address.strip() != "Address not found":
        fields.property_address = existing_address.strip()
    else:
        fields.property_address = UNKNOWN

    # Surviving spouse.
    spouse, spouse_reason = _extract_surviving_spouse(text)
    fields.surviving_spouse = spouse
    extra_reasons: list[str] = []
    if spouse_reason:
        extra_reasons.append(spouse_reason)

    # Beneficiary / heir / devisee.
    fields.beneficiary_heir_devisee = _extract_beneficiary(text)

    # Complexity classification.
    flag, reasons, confidence = _classify_complexity(
        text,
        fields.decedent,
        fields.beneficiary_heir_devisee,
        fields.property_address,
        extra_reasons,
    )
    fields.complexity_flag = flag
    fields.complexity_reasons = reasons
    fields.confidence = confidence

    # Notes.
    if legal_description:
        fields.notes.append(f"Legal Desc: {legal_description}")
    if fields.complexity_reasons:
        fields.notes.append("Reasons: " + "; ".join(fields.complexity_reasons))

    return fields
