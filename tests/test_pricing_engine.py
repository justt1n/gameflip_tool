import pytest

from core.competition_analyzer import CompetitionAnalyzer
from core.log_formatter import LogFormatter
from core.pricing_engine import PricingEngine
from models.standard_models import StandardCompetitorOffer
from tests.conftest import make_payload, make_prepared_input


def _make_payload(
    compare="1",
    min_adj=None,
    max_adj=None,
    rounding=2,
    fetched_min=None,
    fetched_max=None,
    inline_min=None,
    current_price=None,
):
    payload = make_payload(
        compare_mode=compare,
        min_adj=min_adj,
        max_adj=max_adj,
        rounding=rounding,
        fetched_min=fetched_min,
        fetched_max=fetched_max,
        inline_min_price=inline_min,
    )
    if current_price is not None:
        payload.current_price = current_price
    return payload


@pytest.fixture
def engine():
    return PricingEngine(CompetitionAnalyzer(), LogFormatter())


class TestCalcFinalPrice:
    def test_no_competitor_uses_max(self, engine):
        p = _make_payload(fetched_max=10.0, rounding=2)
        price = engine._calc_final_price(p, competitor_price=None)
        assert price == 10.0

    def test_undercut_with_fixed_adjustment(self, engine):
        p = _make_payload(min_adj=0.01, max_adj=0.01, rounding=2)
        price = engine._calc_final_price(p, competitor_price=5.00)
        assert price == 4.99

    def test_clamp_to_min(self, engine):
        p = _make_payload(fetched_min=8.0, min_adj=5.0, max_adj=5.0, rounding=2)
        price = engine._calc_final_price(p, competitor_price=10.0)
        assert price == 8.0

    def test_clamp_to_max(self, engine):
        p = _make_payload(fetched_max=12.0, rounding=2)
        price = engine._calc_final_price(p, competitor_price=15.0)
        assert price == 12.0

    def test_round_up(self, engine):
        p = _make_payload(rounding=2)
        price = engine._calc_final_price(p, competitor_price=12.341)
        assert price == 12.35

    def test_no_adjustment_when_none(self, engine):
        p = _make_payload(min_adj=None, max_adj=None, rounding=2)
        price = engine._calc_final_price(p, competitor_price=10.00)
        assert price == 10.00
        assert p.applied_adj == 0.0

    def test_adj_min_gt_max_swapped(self, engine):
        p = _make_payload(min_adj=0.05, max_adj=0.01, rounding=2)
        price = engine._calc_final_price(p, competitor_price=10.00)
        assert 9.95 <= price <= 9.99


class TestSignificance:
    def test_noise_within_threshold(self, engine):
        p = _make_payload(min_adj=0.01, max_adj=0.05, rounding=2)
        assert engine._is_significant(10.00, 10.04, p) is False

    def test_real_change(self, engine):
        p = _make_payload(min_adj=0.01, max_adj=0.05, rounding=2)
        assert engine._is_significant(10.00, 10.10, p) is True

    def test_no_adjustment_small_threshold(self, engine):
        p = _make_payload(min_adj=None, max_adj=None, rounding=2)
        assert engine._is_significant(10.00, 10.01, p) is False
        assert engine._is_significant(10.00, 10.02, p) is True


class TestProcess:
    @pytest.mark.asyncio
    async def test_mode_0_uses_min_price(self, engine):
        p = _make_payload(compare="0", fetched_min=8.50, inline_min="8.50")
        prepared = make_prepared_input(p, my_price=15.0)
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 8.50

    @pytest.mark.asyncio
    async def test_mode_0_no_min_price_skips(self, engine):
        p = _make_payload(compare="0")
        prepared = make_prepared_input(p, my_price=15.0)
        result = await engine.process(prepared)
        assert result.status == 0
        assert "No min price" in result.log_message

    @pytest.mark.asyncio
    async def test_mode_1_undercut(self, engine):
        competitors = [
            StandardCompetitorOffer(seller_name="rival", price=14.20, is_eligible=True)
        ]
        p = _make_payload(
            compare="1",
            min_adj=0.01,
            max_adj=0.05,
            fetched_min=12.50,
            fetched_max=18.00,
            inline_min="12.50",
        )
        prepared = make_prepared_input(p, my_price=15.0, competitors=competitors)
        result = await engine.process(prepared)
        assert result.status == 1
        assert 12.50 <= result.final_price.price <= 14.19

    @pytest.mark.asyncio
    async def test_mode_2_holds_when_already_lower(self, engine):
        competitors = [
            StandardCompetitorOffer(seller_name="rival", price=14.20, is_eligible=True)
        ]
        p = _make_payload(
            compare="2",
            min_adj=0.01,
            max_adj=0.05,
            fetched_min=12.50,
            fetched_max=18.00,
            inline_min="12.50",
        )
        prepared = make_prepared_input(p, my_price=13.00, competitors=competitors)
        result = await engine.process(prepared)
        assert result.status == 2

    @pytest.mark.asyncio
    async def test_no_min_price_skips(self, engine):
        competitors = [
            StandardCompetitorOffer(seller_name="rival", price=14.20, is_eligible=True)
        ]
        p = _make_payload(compare="1")
        prepared = make_prepared_input(p, my_price=15.0, competitors=competitors)
        result = await engine.process(prepared)
        assert result.status == 0
        assert "Min price" in result.log_message

    @pytest.mark.asyncio
    async def test_min_price_protection_force_update(self, engine):
        competitors = [
            StandardCompetitorOffer(seller_name="rival", price=14.20, is_eligible=True)
        ]
        p = _make_payload(
            compare="1",
            min_adj=0.01,
            max_adj=0.01,
            fetched_min=12.50,
            fetched_max=18.00,
            inline_min="12.50",
        )
        prepared = make_prepared_input(p, my_price=10.00, competitors=competitors)
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 12.50
