from src.clean_names import clean_legal_name, normalize_name, strip_legal_suffixes


def test_normalize_name_unicode_and_ampersand() -> None:
    assert normalize_name("Müller & Söhne GmbH") == "muller and sohne gmbh"


def test_strip_legal_suffixes_terminal_only() -> None:
    assert strip_legal_suffixes("acme inc") == "acme"
    assert strip_legal_suffixes("acme technology") == "acme technology"


def test_clean_legal_name_examples() -> None:
    assert clean_legal_name("ACME, Inc.") == "acme"
    assert clean_legal_name("PT Nusantara Energi Tbk") == "nusantara energi"
    assert clean_legal_name("Example S.p.A.") == "example"
