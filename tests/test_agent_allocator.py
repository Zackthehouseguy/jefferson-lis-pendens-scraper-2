from scrapers.agent_allocator import qualify_house, qualify_land


def verified_base():
    return {
        "ai_scoring_status": "LIVE",
        "ai_contract_version": "reaper-live-ai-v1",
        "priority_score": 80,
        "distress_score": 80,
        "motivation_score": 80,
        "builder_fit_score": 80,
        "lojic_parcel_verified": True,
        "pva_verified": True,
        "current_owner_individual": True,
        "verified_owner": "DOE JANE",
    }


def test_house_requires_explicit_parcel_and_current_owner_verification():
    row = {**verified_base(), "landuse_name": "SINGLE FAMILY"}
    assert qualify_house(row)
    for field in ("lojic_parcel_verified", "pva_verified", "current_owner_individual"):
        broken = dict(row)
        broken.pop(field)
        assert not qualify_house(broken)


def test_land_never_defaults_unknown_property_to_vacant():
    row = {**verified_base(), "landuse_name": "VACANT", "vacant_lot_context": True}
    assert qualify_land(row)
    for field, value in (
        ("landuse_name", None),
        ("landuse_name", "SINGLE FAMILY"),
        ("vacant_lot_context", False),
        ("pva_verified", False),
    ):
        broken = dict(row)
        broken[field] = value
        assert not qualify_land(broken)
