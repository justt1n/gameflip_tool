import pytest
from core.log_formatter import LogFormatter
from models.processing_models import AnalysisResult
from models.standard_models import StandardCompetitorOffer
from tests.conftest import make_payload


@pytest.fixture
def formatter():
    return LogFormatter()


class TestLogFormatterE2E:
    def test_format_compare_update(self, formatter):
        p = make_payload(fetched_min=12.50, fetched_max=18.00)
        analysis = AnalysisResult(
            competitor_name="AlphaKeys",
            competitive_price=14.20,
            top_sellers_for_log=[
                StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True),
            ],
            sellers_below_min=[],
        )
        result = formatter.format("compare", p, 14.15, analysis)
        assert "UPDATE" in result
        assert "14.150" in result
        assert "AlphaKeys" in result
        assert "12.500" in result
        assert "18.000" in result

    def test_format_not_compare(self, formatter):
        p = make_payload()
        result = formatter.format("not_compare", p, 8.50)
        assert "UPDATE" in result
        assert "no comparison" in result
        assert "8.500" in result

    def test_format_below_min(self, formatter):
        p = make_payload(inline_min_price="12.50")
        result = formatter.format("below_min", p, 11.00)
        assert "SKIP" in result
        assert "below min" in result
        assert "11.000" in result
        assert "12.500" in result

    def test_format_no_min_price(self, formatter):
        p = make_payload()
        result = formatter.format("no_min_price", p, 0.0)
        assert "SKIP" in result
        assert "Min price not configured" in result

    def test_format_equal(self, formatter):
        p = make_payload()
        result = formatter.format("equal", p, 14.18)
        assert "SKIP" in result
        assert "matches target" in result

    def test_format_with_sellers_below_min(self, formatter):
        p = make_payload(fetched_min=12.50, fetched_max=18.00)
        analysis = AnalysisResult(
            competitor_name="AlphaKeys",
            competitive_price=14.20,
            top_sellers_for_log=[],
            sellers_below_min=[
                StandardCompetitorOffer(seller_name="CheapKeys", price=11.00, is_eligible=True),
                StandardCompetitorOffer(seller_name="LowSeller", price=9.00, is_eligible=True),
            ],
        )
        result = formatter.format("compare", p, 14.15, analysis)
        assert "Below Min:" in result
        assert "CheapKeys" in result

    def test_format_with_top_sellers(self, formatter):
        p = make_payload(fetched_min=12.50, fetched_max=18.00)
        sellers = [
            StandardCompetitorOffer(seller_name=f"Seller{i}", price=12.0 + i, is_eligible=True)
            for i in range(6)
        ]
        analysis = AnalysisResult(
            competitor_name="Seller0",
            competitive_price=12.00,
            top_sellers_for_log=sellers,
            sellers_below_min=[],
        )
        result = formatter.format("compare", p, 11.99, analysis)
        assert "Top Sellers:" in result
        # Should show at most 4 sellers in the top list
        top_line = [l for l in result.split("\n") if "Top Sellers:" in l][0]
        entries = top_line.split(";")
        assert len(entries) <= 4

    def test_format_no_competition_fallback(self, formatter):
        p = make_payload(fetched_min=12.50, fetched_max=18.00)
        analysis = AnalysisResult(
            competitor_name=None,
            competitive_price=None,
            top_sellers_for_log=[],
            sellers_below_min=[],
        )
        result = formatter.format("compare", p, 18.00, analysis)
        assert "Max price" in result

