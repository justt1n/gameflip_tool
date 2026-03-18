import pytest
from core.competition_analyzer import CompetitionAnalyzer
from models.standard_models import StandardCompetitorOffer
from tests.conftest import make_payload


@pytest.fixture
def analyzer():
    return CompetitionAnalyzer()


class TestCompetitionAnalyzerE2E:
    def test_normal_with_blacklist(self, analyzer, competitors_normal):
        """Blacklisted CheapKeys removed → AlphaKeys is target."""
        p = make_payload(fetched_blacklist=["CheapKeys"], inline_min_price="12.00")
        result = analyzer.analyze(p, competitors_normal)
        assert result.competitor_name == "AlphaKeys"
        assert result.competitive_price == 14.20

    def test_all_blacklisted(self, analyzer, competitors_all_blacklisted):
        p = make_payload(fetched_blacklist=["CheapKeys", "ShadySeller"])
        result = analyzer.analyze(p, competitors_all_blacklisted)
        assert result.competitor_name is None
        assert result.competitive_price is None

    def test_no_competitors(self, analyzer, competitors_empty):
        p = make_payload()
        result = analyzer.analyze(p, competitors_empty)
        assert result.competitor_name is None

    def test_all_ineligible(self, analyzer, competitors_all_ineligible):
        p = make_payload()
        result = analyzer.analyze(p, competitors_all_ineligible)
        assert result.competitive_price is None
        assert result.top_sellers_for_log == []

    def test_single_rival(self, analyzer, competitors_single_rival):
        p = make_payload()
        result = analyzer.analyze(p, competitors_single_rival)
        assert result.competitor_name == "OnlyRival"
        assert result.competitive_price == 13.50

    def test_sellers_below_min_detected(self, analyzer, competitors_normal):
        p = make_payload(inline_min_price="12.00", fetched_blacklist=None)
        result = analyzer.analyze(p, competitors_normal)
        below_names = [s.seller_name for s in result.sellers_below_min]
        assert "CheapKeys" in below_names
        assert "EpsilonStore" in below_names

    def test_top_sellers_sorted(self, analyzer, competitors_normal):
        p = make_payload(fetched_blacklist=None)
        result = analyzer.analyze(p, competitors_normal)
        eligible = [o for o in result.top_sellers_for_log if o.is_eligible]
        for i in range(len(eligible) - 1):
            assert eligible[i].price <= eligible[i + 1].price
