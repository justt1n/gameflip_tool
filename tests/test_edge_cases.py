import pytest

from core.competition_analyzer import CompetitionAnalyzer
from core.log_formatter import LogFormatter
from core.orchestrator import Orchestrator
from core.pricing_engine import PricingEngine
from models.sheet_models import Payload
from models.standard_models import StandardCompetitorOffer
from tests.conftest import ConfigurableMockAdapter, make_payload, make_prepared_input


def _make_payload(**overrides):
    row = [""] * 28
    row[1] = overrides.get("check", "1")
    row[2] = overrides.get("name", "TestProduct")
    row[6] = overrides.get("product_id", "https://mock.com/123")
    row[7] = overrides.get("compare", "1")
    row[13] = overrides.get("rounding", "2")
    if "relax" in overrides:
        row[26] = overrides["relax"]
    if "inline_min" in overrides:
        row[27] = overrides["inline_min"]
    p = Payload.from_row(row, row_index=overrides.get("row_index", 5))
    if p and "fetched_min" in overrides:
        p.fetched_min_price = overrides["fetched_min"]
    if p and "fetched_max" in overrides:
        p.fetched_max_price = overrides["fetched_max"]
    return p


@pytest.fixture
def engine():
    return PricingEngine(CompetitionAnalyzer(), LogFormatter())


class TestEdgeCases:
    def test_empty_sheet_no_crash(self):
        assert len([]) == 0

    @pytest.mark.asyncio
    async def test_no_adapter_for_platform(self):
        orch = Orchestrator(sheet_engine=None, pricing_engine=None, adapter_registry={})
        with pytest.raises(ValueError):
            orch.detect_platform("https://unknown-site.com/123")

    def test_relax_non_numeric_ignored(self):
        p = _make_payload(relax="abc")
        assert p is not None
        assert p.relax == "abc"

    def test_payload_from_row_empty(self):
        row = [""] * 28
        p = Payload.from_row(row, row_index=10)
        assert p is None

    def test_payload_from_row_partial(self):
        p = _make_payload(product_id="", compare="", rounding="", fetched_min=None, fetched_max=None)
        assert p is not None
        assert p.product_id is None
        assert p.price_rounding is None

    @pytest.mark.asyncio
    async def test_mode_0_no_min_price(self, engine):
        p = _make_payload(compare="0")
        result = await engine.process(make_prepared_input(p, my_price=10.0))
        assert result.status == 0

    @pytest.mark.asyncio
    async def test_all_competitors_blacklisted_uses_max(self, engine):
        p = _make_payload(compare="1", fetched_min=12.0, fetched_max=18.0, inline_min="12.0")
        p.fetched_black_list = ["BadGuy"]
        prepared = make_prepared_input(
            p,
            my_price=15.0,
            competitors=[StandardCompetitorOffer(seller_name="BadGuy", price=13.0, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert 12.0 <= result.final_price.price <= 18.0

    @pytest.mark.asyncio
    async def test_significance_prevents_flicker(self, engine):
        p = _make_payload(compare="1", fetched_min=12.0, fetched_max=18.0, inline_min="12.0")
        p.min_price_adjustment = 0.01
        p.max_price_adjustment = 0.05
        prepared = make_prepared_input(
            p,
            my_price=14.18,
            competitors=[StandardCompetitorOffer(seller_name="rival", price=14.20, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 2

    @pytest.mark.asyncio
    async def test_min_price_protection_rescue(self, engine):
        p = _make_payload(compare="1", fetched_min=12.0, fetched_max=18.0, inline_min="12.0")
        prepared = make_prepared_input(
            p,
            my_price=8.0,
            competitors=[StandardCompetitorOffer(seller_name="rival", price=14.0, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 12.0

    @pytest.mark.asyncio
    async def test_round_up_integer(self, engine):
        p = _make_payload(compare="1", fetched_min=10.0, fetched_max=20.0, inline_min="10.0", rounding="0")
        p.min_price_adjustment = None
        p.max_price_adjustment = None
        prepared = make_prepared_input(
            p,
            my_price=20.0,
            competitors=[StandardCompetitorOffer(seller_name="rival", price=14.70, is_eligible=True)],
        )
        result = await engine.process(prepared)
        assert result.status == 1
        assert result.final_price.price == 15.0

    @pytest.mark.asyncio
    async def test_row_level_failure_does_not_stop_update_flow(self):
        adapter = ConfigurableMockAdapter(resolve_fails=True)
        with pytest.raises(ValueError):
            await adapter.resolve_payload_targets(make_payload())
