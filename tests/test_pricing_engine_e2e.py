import pytest

from core.competition_analyzer import CompetitionAnalyzer
from core.log_formatter import LogFormatter
from core.pricing_engine import PricingEngine
from models.standard_models import StandardCompetitorOffer
from tests.conftest import make_payload, make_prepared_input


@pytest.fixture
def engine():
    return PricingEngine(CompetitionAnalyzer(), LogFormatter())


class TestPricingEngineE2E:
    @pytest.mark.asyncio
    async def test_mode1_undercut_competitor(self, engine):
        prepared = make_prepared_input(
            make_payload(min_adj=0.01, max_adj=0.05, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=15.00,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert 12.50 <= result.final_price.price <= 14.19
        assert result.log_message is not None

    @pytest.mark.asyncio
    async def test_mode1_no_competitors_use_max(self, engine):
        prepared = make_prepared_input(
            make_payload(min_adj=0.01, max_adj=0.05, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=15.00,
            competitors=[],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert 12.50 <= result.final_price.price <= 18.00

    @pytest.mark.asyncio
    async def test_mode1_competitor_below_min(self, engine):
        prepared = make_prepared_input(
            make_payload(min_adj=0.01, max_adj=0.05, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=15.00,
            competitors=[StandardCompetitorOffer(seller_name="LowSeller", price=10.00, is_eligible=False, note="Base < Min")],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price >= 12.50

    @pytest.mark.asyncio
    async def test_mode1_price_already_matches(self, engine):
        prepared = make_prepared_input(
            make_payload(min_adj=0.01, max_adj=0.05, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=14.18,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 2

    @pytest.mark.asyncio
    async def test_mode0_inline_min(self, engine):
        prepared = make_prepared_input(
            make_payload(compare_mode="0", fetched_min=8.50, inline_min_price="8.50"),
            my_price=15.00,
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 8.50

    @pytest.mark.asyncio
    async def test_mode0_no_min_price(self, engine):
        prepared = make_prepared_input(make_payload(compare_mode="0"), my_price=15.00)
        result = await engine.process(prepared)
        assert result.status == 0
        assert "No min price" in result.log_message

    @pytest.mark.asyncio
    async def test_mode2_already_cheaper(self, engine):
        prepared = make_prepared_input(
            make_payload(compare_mode="2", min_adj=0.01, max_adj=0.05, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=13.00,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 2

    @pytest.mark.asyncio
    async def test_mode2_needs_decrease(self, engine):
        prepared = make_prepared_input(
            make_payload(compare_mode="2", min_adj=0.01, max_adj=0.05, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=16.00,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price < 16.00

    @pytest.mark.asyncio
    async def test_mode2_within_noise(self, engine):
        prepared = make_prepared_input(
            make_payload(compare_mode="2", min_adj=0.01, max_adj=0.05, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=14.17,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 2

    @pytest.mark.asyncio
    async def test_min_price_protection_rescue(self, engine):
        prepared = make_prepared_input(
            make_payload(min_adj=0.01, max_adj=0.05, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=10.00,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 12.50

    @pytest.mark.asyncio
    async def test_clamp_to_min_floor(self, engine):
        prepared = make_prepared_input(
            make_payload(min_adj=0.50, max_adj=0.50, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=15.00,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=12.80, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 12.50

    @pytest.mark.asyncio
    async def test_clamp_to_max_ceiling(self, engine):
        prepared = make_prepared_input(
            make_payload(min_adj=0.0, max_adj=0.0, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=15.00,
            competitors=[],
        )
        result = await engine.process(prepared)
        assert result.final_price.price == 18.00

    @pytest.mark.asyncio
    async def test_round_up_behavior(self, engine):
        prepared = make_prepared_input(
            make_payload(fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=15.00,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.236, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 14.24

    @pytest.mark.asyncio
    async def test_zero_rounding(self, engine):
        prepared = make_prepared_input(
            make_payload(rounding=0, fetched_min=10.00, fetched_max=20.00, inline_min_price="10.00"),
            my_price=20.00,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.70, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 15.0

    @pytest.mark.asyncio
    async def test_no_adjustment_range(self, engine):
        prepared = make_prepared_input(
            make_payload(fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=15.00,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 14.20

    @pytest.mark.asyncio
    async def test_update_command_emitted(self, engine):
        prepared = make_prepared_input(
            make_payload(min_adj=0.01, max_adj=0.05, fetched_min=12.50, fetched_max=18.00, inline_min_price="12.50"),
            my_price=15.00,
            competitors=[StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.update_command is not None
        assert result.update_command.offer_id == "offer_001"
