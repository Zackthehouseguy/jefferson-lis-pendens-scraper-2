"""Acquisition filters for TheReaper land lane."""
from __future__ import annotations
import re

PUBLIC_OWNER_TERMS = (
    "LOUISVILLE METRO", "LOUISVILLE/JEFFERSON", "JEFFERSON COUNTY",
    "COMMONWEALTH OF KENTUCKY", "STATE OF KENTUCKY", "UNITED STATES OF AMERICA",
    "METRO GOVERNMENT", "LOUISVILLE WATER", "METROPOLITAN SEWER DISTRICT",
    "MSD ", "LAND BANK", "LANDBANK",
)
PUBLIC_OWNER_EXACT = {"LOUISVILLE", "LOUISVILLE METRO GOVERNMENT", "JEFFERSON COUNTY"}


def normalize_owner(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").upper()).strip()


def obvious_public_owner(value: str | None) -> bool:
    owner = normalize_owner(value)
    if not owner:
        return False
    if owner in PUBLIC_OWNER_EXACT:
        return True
    return any(term in owner for term in PUBLIC_OWNER_TERMS)


def private_owner_screen(value: str | None) -> dict:
    public = obvious_public_owner(value)
    return {
        "private_owner_screen_passed": not public,
        "public_owner_detected": public,
        "owner_screen_basis": "obvious_public_name_filter" if public else "no_obvious_public_owner_name",
    }
