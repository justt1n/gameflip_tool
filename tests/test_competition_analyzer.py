import pytest

from core.competition_analyzer import CompetitionAnalyzer
from models.standard_models import StandardCompetitorOffer
from models.sheet_models import Payload


def _make_payload(blacklist=None, inline_min=None):
    row = [""] * 28
    row[1] = "1"
    row[2] = "TestProduct"
    if inline_min is not None:
        row[27] = str(inline_min)
    p = Payload.from_row(row, row_index=5)
    p.fetched_black_list = blacklist
    return p


@pytest.fixture
def analyzer():
    return CompetitionAnalyzer()


class TestCompetitionAnalyzer:
    def test_finds_lowest_eligible(self, analyzer):
        offers = [
            StandardCompetitorOffer(seller_name="A", price=14.00, is_eligible=True),
            StandardCompetitorOffer(seller_name="B", price=12.00, is_eligible=True),
            StandardCompetitorOffer(seller_name="C", price=16.00, is_eligible=True),
        ]
        result = analyzer.analyze(_make_payload(), offers)
        assert result.competitor_name == "B"
        assert result.competitive_price == 12.00

    def test_blacklist_filters_sellers(self, analyzer):
        offers = [
            StandardCompetitorOffer(seller_name="CheapKeys", price=10.00, is_eligible=True),
            StandardCompetitorOffer(seller_name="GoodSeller", price=14.00, is_eligible=True),
        ]
        result = analyzer.analyze(_make_payload(blacklist=["CheapKeys"]), offers)
        assert result.competitor_name == "GoodSeller"
        assert result.competitive_price == 14.00

    def test_blacklist_case_insensitive(self, analyzer):
        offers = [
            StandardCompetitorOffer(seller_name="CheapKeys", price=10.00, is_eligible=True),
            StandardCompetitorOffer(seller_name="GoodSeller", price=14.00, is_eligible=True),
        ]
        result = analyzer.analyze(_make_payload(blacklist=["cheapkeys"]), offers)
        assert result.competitor_name == "GoodSeller"

    def test_empty_offers_returns_none(self, analyzer):
        result = analyzer.analyze(_make_payload(), [])
        assert result.competitor_name is None
        assert result.competitive_price is None

    def test_all_blacklisted_returns_none(self, analyzer):
        offers = [
            StandardCompetitorOffer(seller_name="BadA", price=10.00, is_eligible=True),
            StandardCompetitorOffer(seller_name="BadB", price=12.00, is_eligible=True),
        ]
        result = analyzer.analyze(_make_payload(blacklist=["BadA", "BadB"]), offers)
        assert result.competitor_name is None
        assert result.competitive_price is None

    def test_all_ineligible_returns_no_target(self, analyzer):
        offers = [
            StandardCompetitorOffer(seller_name="A", price=5.00, is_eligible=False, note="Too low"),
            StandardCompetitorOffer(seller_name="B", price=25.00, is_eligible=False, note="Too high"),
        ]
        result = analyzer.analyze(_make_payload(), offers)
        assert result.competitive_price is None
        assert result.competitor_name is None
        assert result.top_sellers_for_log == []

    def test_sellers_below_min_identified(self, analyzer):
        offers = [
            StandardCompetitorOffer(seller_name="Low1", price=8.00, is_eligible=True),
            StandardCompetitorOffer(seller_name="Low2", price=9.00, is_eligible=True),
            StandardCompetitorOffer(seller_name="OK", price=14.00, is_eligible=True),
        ]
        result = analyzer.analyze(_make_payload(inline_min="12.00"), offers)
        below = [s.seller_name for s in result.sellers_below_min]
        assert "Low1" in below
        assert "Low2" in below
        assert "OK" not in below

    def test_no_blacklist_no_filtering(self, analyzer):
        offers = [
            StandardCompetitorOffer(seller_name="A", price=10.00, is_eligible=True),
            StandardCompetitorOffer(seller_name="B", price=12.00, is_eligible=True),
        ]
        result = analyzer.analyze(_make_payload(blacklist=None), offers)
        assert result.competitor_name == "A"
        assert len(result.top_sellers_for_log) == 2

    def test_top_sellers_only_include_eligible_offers(self, analyzer):
        offers = [
            StandardCompetitorOffer(seller_name="Ineligible", price=5.00, is_eligible=False),
            StandardCompetitorOffer(seller_name="Eligible1", price=14.00, is_eligible=True),
            StandardCompetitorOffer(seller_name="Eligible2", price=12.00, is_eligible=True),
        ]
        result = analyzer.analyze(_make_payload(), offers)
        log_names = [o.seller_name for o in result.top_sellers_for_log]
        assert log_names[0] == "Eligible2"
        assert log_names[1] == "Eligible1"
        assert "Ineligible" not in log_names
