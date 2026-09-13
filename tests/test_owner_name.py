from scrapers.owner_name import format_owner_name


def test_formats_verified_pva_people_in_natural_order():
    cases = {
        "BAILEY SANDRA C": "Sandra C Bailey",
        "SCOTT ALVIN": "Alvin Scott",
        "ALLEN DEAN SCOTT": "Dean Scott Allen",
        "WILLIAMS MICHAEL H & DIANE L": "Michael H Williams & Diane L Williams",
        "THOMAS JEREMY & THOMAS AMBER": "Jeremy Thomas & Amber Thomas",
        "BRAMER ROBERT JR": "Robert Bramer Jr",
    }
    for raw, expected in cases.items():
        result = format_owner_name(raw)
        assert result["display"] == expected
        assert result["raw"] == raw


def test_preserves_entities_and_ambiguous_names():
    entity = format_owner_name("PAUL R BROOKSHIRE FAMILY REVOCABLE T")
    assert entity["is_entity"] is True
    assert entity["display"] == entity["raw"]

    ambiguous = format_owner_name("PRICE CHRISTOPHER THOMAS DANIEL")
    assert ambiguous["ambiguous"] is True
    assert ambiguous["review_needed"] is True
    assert ambiguous["display"] == ambiguous["raw"]


def test_is_idempotent():
    natural = format_owner_name("Sandra C Bailey")
    assert natural["display"] == "Sandra C Bailey"
    assert natural["changed"] is False
