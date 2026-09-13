#!/usr/bin/env python3
"""Conservative, idempotent display formatting for verified PVA owner names."""
from __future__ import annotations

import re
from typing import Any, TypedDict

SUFFIXES = {"JR", "SR", "II", "III", "IV", "VI", "VII", "JR.", "SR."}
ROMAN = {"II", "III", "IV", "V", "VI", "VII"}
PARTICLES = {
    "DE", "DEL", "DELA", "DELLA", "DI", "DA", "DOS", "DU", "LA", "LE",
    "VAN", "VON", "VANDER", "TER", "TEN", "BIN", "IBN", "AL", "ST", "SAN", "SANTA",
}
ENTITY_WORDS = {
    "LLC", "L L C", "L.L.C", "INC", "CORP", "CORPORATION", "COMPANY", "CO",
    "LP", "LLP", "LTD", "TRUST", "TRU", "TRUSTEE", "REVOCABLE", "REVOCAB",
    "IRREVOCABLE", "FAMILY", "ESTATE", "PROPERTIES", "PROPERTY", "HOLDINGS",
    "HOLDING", "INVESTMENTS", "INVESTMENT", "ENTERPRISES", "ASSOCIATES",
    "ASSOCIATION", "PARTNERS", "PARTNERSHIP", "GROUP", "REALTY", "HOMES",
    "HOME", "RENOVATION", "RENOVATIONS", "CONSTRUCTION", "DEVELOPMENT",
    "BUILDERS", "SERVICES", "SOLUTIONS", "BANK", "CHURCH", "MINISTRIES",
    "FOUNDATION", "AUTHORITY", "CITY", "COUNTY", "METRO", "HOUSING",
    "VENTURES", "CAPITAL", "EQUITY", "REI", "ACQUISITIONS", "MANAGEMENT",
}

class NameResult(TypedDict):
    display: str
    raw: str
    is_entity: bool
    ambiguous: bool
    changed: bool
    review_needed: bool


def _proper_word(word: str) -> str:
    if not word or word[:1].isdigit():
        return word
    upper = word.upper()
    if upper in ROMAN:
        return upper
    if len(upper.replace(".", "")) == 1:
        return upper.replace(".", "")
    if "-" in word or "'" in word:
        return "".join(
            part if part in {"-", "'"} else _proper_word(part)
            for part in re.split(r"([-'])", word)
        )
    lower = upper.lower()
    value = lower[:1].upper() + lower[1:]
    if re.match(r"^mc[a-z]{2,}", lower):
        value = "Mc" + lower[2:3].upper() + lower[3:]
    elif re.match(r"^mac[a-z]{3,}", lower):
        value = "Mac" + lower[3:4].upper() + lower[4:]
    return value


def _proper(value: str) -> str:
    return " ".join(_proper_word(x) for x in value.split() if x)


def _is_entity(value: str) -> bool:
    padded = " " + re.sub(r"[.,]", "", value.upper()) + " "
    return bool(re.match(r"^\s*\d", padded)) or any(f" {word} " in padded for word in ENTITY_WORDS)


def _is_natural(value: str) -> bool:
    letters = re.sub(r"[^A-Za-z]", "", value)
    return not letters or (bool(re.search(r"[a-z]", letters)) and not bool(re.fullmatch(r"[A-Z\s.'&()-]+", value)))


def _is_initial(value: str) -> bool:
    return len(value.replace(".", "")) == 1


def _part(value: str, inherited: str | None, first: bool) -> tuple[str, str | None, bool]:
    annotations: list[str] = []
    work = re.sub(r"\(([^)]*)\)", lambda m: annotations.append(m.group(0)) or " ", value).strip()
    deceased = re.search(r"\bDECEASED\b\s*$", work, re.I)
    if deceased:
        annotations.append(deceased.group(0).strip())
        work = work[:deceased.start()].strip()
    work = re.sub(r"[,;]\s*$", "", work).strip()

    comma_surname = None
    if "," in work and work.index(",") > 0:
        comma_surname, work = (x.strip() for x in work.split(",", 1))

    tokens = work.split()
    suffix: list[str] = []
    while len(tokens) > 1 and tokens[-1].upper() in SUFFIXES:
        suffix.insert(0, _proper_word(tokens.pop()))

    def rendered(parts: list[str]) -> str:
        return " ".join(x for x in [*parts, *annotations] if x)

    fallback = rendered([_proper(" ".join(tokens)), *suffix])
    if comma_surname:
        return rendered([_proper(" ".join(tokens)), _proper(comma_surname), *suffix]), comma_surname, False
    if not tokens:
        return fallback, None, False
    if any(x.upper().replace(".", "") in PARTICLES for x in tokens):
        return fallback, None, True
    if len(tokens) == 1:
        if not first and inherited:
            return rendered([_proper_word(tokens[0]), _proper_word(inherited), *suffix]), inherited, False
        return fallback, None, False
    if not first and inherited:
        starts_with_surname = tokens[0].upper() == inherited.upper()
        if not starts_with_surname and len(tokens) <= 2 and _is_initial(tokens[-1]):
            return rendered([*map(_proper_word, tokens), _proper_word(inherited), *suffix]), inherited, False
    if len(tokens) == 3 and _is_initial(tokens[1]):
        return fallback, None, True
    if len(tokens) > 3:
        return fallback, None, True
    surname = tokens[0]
    return rendered([*map(_proper_word, tokens[1:]), _proper_word(surname), *suffix]), surname, False


def format_owner_name(value: object) -> NameResult:
    raw = str(value or "").strip()
    base: NameResult = {
        "display": raw, "raw": raw, "is_entity": False,
        "ambiguous": False, "changed": False, "review_needed": False,
    }
    if not raw or _is_natural(raw):
        return base
    if _is_entity(raw):
        return {**base, "is_entity": True}

    parts = [x.strip() for x in re.split(r"\s*(?:&|\bAND\b)\s*", raw, flags=re.I) if x.strip()]
    inherited = None
    formatted: list[str] = []
    ambiguous = False
    for index, part in enumerate(parts):
        display, surname, unsafe = _part(part, inherited, index == 0)
        if index == 0 and surname:
            inherited = surname
        formatted.append(display)
        ambiguous = ambiguous or unsafe
    if ambiguous:
        return {**base, "ambiguous": True, "review_needed": True}
    display = " & ".join(formatted)
    return {**base, "display": display, "changed": display != raw}
