from core.reference_price_selector import ReferencePriceSelector
from tests.conftest import make_payload


def test_select_best_price_uses_lowest_enabled_ss_source():
    payload = make_payload(fetched_max=20.0, inline_min_price="10.0")
    payload.ss1_check = "1"
    payload.ss1_profit = 18
    payload.ss1_hesonhan = 0.85
    payload.ss1_quydoidonvi = 1
    payload.fetched_ss1_price = 10.0
    payload.ss2_check = "1"
    payload.ss2_profit = 10
    payload.ss2_hesonhan = 1
    payload.ss2_quydoidonvi = 1
    payload.fetched_ss2_price = 12.0

    candidate = ReferencePriceSelector().select_best_price(payload, competitor_price=14.2)

    assert candidate is not None
    assert candidate.source_name == "SS1"
    assert candidate.price == 10.03


def test_select_best_price_keeps_competitor_when_sources_are_higher():
    payload = make_payload(fetched_max=20.0, inline_min_price="10.0")
    payload.ss1_check = "1"
    payload.ss1_profit = 18
    payload.ss1_hesonhan = 1
    payload.ss1_quydoidonvi = 1
    payload.fetched_ss1_price = 20.0

    candidate = ReferencePriceSelector().select_best_price(payload, competitor_price=14.2)

    assert candidate is not None
    assert candidate.source_name == "Competition"
    assert candidate.price == 14.2
