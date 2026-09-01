from scrapers.reaper_saturation import METHOD, public_source_exposure_score


def test_exposure_score_changes_with_source_age_and_evidence_specificity():
    fresh_specific, fresh_factors = public_source_exposure_score(
        sources=["louisville_code_violations"],
        freshness_score=100,
        evidence=[{"details": "Unsafe structural foundation collapse with sewage and a boarded opening. " * 4}],
    )
    old_generic, old_factors = public_source_exposure_score(
        sources=["lis_pendens"],
        freshness_score=20,
        evidence=[{"details": "Property address extracted."}],
        owner_mailing_differs=True,
    )

    assert 5 <= fresh_specific <= 100
    assert 5 <= old_generic <= 100
    assert fresh_specific < old_generic
    assert "same_day_or_under_24h=-12" in fresh_factors
    assert "older_than_30_days=+18" in old_factors
    assert "mailing_differs_common_absentee_filter=+8" in old_factors


def test_multi_source_visibility_is_auditable_and_not_claimed_as_competition():
    single, _ = public_source_exposure_score(
        sources=["louisville_code_violations"], freshness_score=82
    )
    stacked, factors = public_source_exposure_score(
        sources=["louisville_code_violations", "tax_delinquent", "lis_pendens"],
        freshness_score=82,
    )

    assert stacked > single
    assert "multi_source_public_visibility=+16" in factors
    assert "not observed competition" in METHOD
