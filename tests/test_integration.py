import pytest

from core.competition_analyzer import CompetitionAnalyzer
from core.log_formatter import LogFormatter
from core.orchestrator import Orchestrator
from core.pricing_engine import PricingEngine
from models.standard_models import StandardCompetitorOffer
from tests.conftest import ConfigurableMockAdapter, make_payload, make_prepared_input


@pytest.fixture
def engine():
    return PricingEngine(CompetitionAnalyzer(), LogFormatter())


class TestFullPipeline:
    @pytest.mark.asyncio
    async def test_mode_1_with_competition(self, engine):
        payload = make_payload(compare_mode="1", fetched_min=8.0, fetched_max=12.0, inline_min_price="8.0")
        prepared = make_prepared_input(
            payload,
            my_price=10.0,
            competitors=[StandardCompetitorOffer(seller_name="rival", price=9.5, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price is not None
        assert 8.0 <= result.final_price.price <= 9.5

    @pytest.mark.asyncio
    async def test_mode_0_uses_min(self, engine):
        payload = make_payload(compare_mode="0", fetched_min=8.0, inline_min_price="8.0")
        prepared = make_prepared_input(payload, my_price=10.0)
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 8.0

    @pytest.mark.asyncio
    async def test_mode_2_hold_when_cheaper(self, engine):
        payload = make_payload(compare_mode="2", fetched_min=8.0, fetched_max=12.0, inline_min_price="8.0")
        prepared = make_prepared_input(payload, my_price=8.5)
        result = await engine.process(prepared)
        assert result.status == 2

    @pytest.mark.asyncio
    async def test_no_min_price_skips(self, engine):
        payload = make_payload(compare_mode="1", fetched_min=None, fetched_max=12.0, inline_min_price=None)
        prepared = make_prepared_input(payload, my_price=10.0)
        result = await engine.process(prepared)
        assert result.status == 0

    @pytest.mark.asyncio
    async def test_update_price_called(self, engine):
        adapter = ConfigurableMockAdapter(my_price=10.0)
        payload = make_payload(compare_mode="1", fetched_min=8.0, fetched_max=12.0, inline_min_price="8.0")
        target = (await adapter.resolve_payload_targets(payload))[0]
        prepared = await adapter.prepare_pricing_input(target)
        result = await engine.process(prepared)
        assert result.status == 1
        success = await adapter.update_price(result.update_command.offer_id, result.update_command.new_price)
        assert success is True
        assert len(adapter.updated_prices) == 1


class TestDetectPlatform:
    def test_detect_from_url(self):
        orch = Orchestrator(
            sheet_engine=None,
            pricing_engine=None,
            adapter_registry={"mock": ConfigurableMockAdapter()},
        )
        assert orch.detect_platform("https://gameflip.com/item/123") == "gameflip"
        assert orch.detect_platform("https://driffle.com/product/123") == "driffle"
        assert orch.detect_platform("https://gamivo.com/product/slug") == "gamivo"
        assert orch.detect_platform("https://g2a.com/product-i123") == "g2a"
        assert orch.detect_platform("https://kinguin.net/product/456") == "kinguin"

    def test_detect_single_adapter_fallback(self):
        orch = Orchestrator(
            sheet_engine=None,
            pricing_engine=None,
            adapter_registry={"mock": ConfigurableMockAdapter()},
        )
        assert orch.detect_platform("12345") == "mock"

    def test_detect_no_match_raises(self):
        orch = Orchestrator(
            sheet_engine=None,
            pricing_engine=None,
            adapter_registry={"a": ConfigurableMockAdapter(), "b": ConfigurableMockAdapter()},
        )
        with pytest.raises(ValueError):
            orch.detect_platform("12345")
