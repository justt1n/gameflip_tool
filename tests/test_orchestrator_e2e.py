import asyncio
import time
import pytest
from core.competition_analyzer import CompetitionAnalyzer
from core.log_formatter import LogFormatter
from core.orchestrator import Orchestrator
from core.pricing_engine import PricingEngine
from models.runtime_models import PreparedPricingInput, ResolvedListingTarget
from models.standard_models import PlatformIdentifiers, StandardCompetitorOffer
from tests.conftest import ConfigurableMockAdapter, make_payload, make_prepared_input


class MockSheetEngine:
    """Simulates SheetEngine with pre-built payloads."""
    def __init__(self, payloads=None, hydration_overrides=None):
        self.payloads = payloads or []
        self.hydration_overrides = hydration_overrides or {}
        self.written_logs = []

    def get_payloads(self):
        return self.payloads

    def hydrate_payload(self, payload):
        overrides = self.hydration_overrides.get(payload.row_index, {})
        for k, v in overrides.items():
            setattr(payload, k, v)
        return payload

    def batch_write_logs(self, updates):
        self.written_logs.extend(updates)


def _make_orchestrator(payloads, adapter=None, workers=1, hydration=None):
    sheet_engine = MockSheetEngine(payloads, hydration)
    pricing_engine = PricingEngine(CompetitionAnalyzer(), LogFormatter())
    adapter = adapter or ConfigurableMockAdapter(
        competitors=[StandardCompetitorOffer(seller_name="rival", price=14.20, is_eligible=True)]
    )
    return Orchestrator(
        sheet_engine=sheet_engine,
        pricing_engine=pricing_engine,
        adapter_registry={"mock": adapter},
        workers=workers,
        sleep_time=0,
    ), sheet_engine, adapter


class TestOrchestratorE2E:
    @pytest.mark.asyncio
    async def test_one_round_processes_all_enabled(self):
        payloads = [
            make_payload(product_name=f"Product {i}", fetched_min=12.0, fetched_max=18.0, inline_min_price="12.0", min_adj=0.01, max_adj=0.05, row_index=i+4)
            for i in range(3)
        ]
        orch, sheet_eng, adapter = _make_orchestrator(payloads)
        await orch._run_one_round()
        assert len(sheet_eng.written_logs) == 3

    @pytest.mark.asyncio
    async def test_batch_splitting(self):
        payloads = [
            make_payload(product_name=f"P{i}", fetched_min=12.0, fetched_max=18.0, inline_min_price="12.0", min_adj=0.01, max_adj=0.05, row_index=i+4)
            for i in range(5)
        ]
        orch, sheet_eng, adapter = _make_orchestrator(payloads, workers=2)
        await orch._run_one_round()
        assert len(sheet_eng.written_logs) == 5

    @pytest.mark.asyncio
    async def test_platform_detection_single_adapter(self):
        p = make_payload(product_id="12345", fetched_min=12.0, fetched_max=18.0, inline_min_price="12.0", min_adj=0.01, max_adj=0.05)
        orch, sheet_eng, adapter = _make_orchestrator([p])
        await orch._run_one_round()
        assert len(sheet_eng.written_logs) == 1

    @pytest.mark.asyncio
    async def test_no_adapter_error(self):
        p = make_payload(product_id="https://unknown.com/123", fetched_min=12.0, fetched_max=18.0, inline_min_price="12.0")
        sheet_engine = MockSheetEngine([p])
        pricing_engine = PricingEngine(CompetitionAnalyzer(), LogFormatter())
        # Register two adapters so single-adapter fallback doesn't apply
        orch = Orchestrator(
            sheet_engine=sheet_engine,
            pricing_engine=pricing_engine,
            adapter_registry={
                "driffle": ConfigurableMockAdapter(platform="driffle"),
                "gamivo": ConfigurableMockAdapter(platform="gamivo"),
            },
            workers=1, sleep_time=0,
        )
        await orch._run_one_round()
        assert len(sheet_engine.written_logs) == 1
        log_note = sheet_engine.written_logs[0][1]["note"]
        assert "Error" in log_note

    @pytest.mark.asyncio
    async def test_update_success_logged(self):
        p = make_payload(fetched_min=12.0, fetched_max=18.0, inline_min_price="12.0", min_adj=0.01, max_adj=0.05)
        orch, sheet_eng, adapter = _make_orchestrator([p])
        await orch._run_one_round()
        assert len(sheet_eng.written_logs) == 1
        log_entry = sheet_eng.written_logs[0]
        assert "UPDATE" in log_entry[1]["note"]

    @pytest.mark.asyncio
    async def test_update_failure_logged(self):
        p = make_payload(fetched_min=12.0, fetched_max=18.0, inline_min_price="12.0", min_adj=0.01, max_adj=0.05)
        adapter = ConfigurableMockAdapter(
            my_price=15.0, update_succeeds=False,
            competitors=[StandardCompetitorOffer(seller_name="rival", price=14.20, is_eligible=True)]
        )
        orch, sheet_eng, _ = _make_orchestrator([p], adapter=adapter)
        await orch._run_one_round()
        assert len(sheet_eng.written_logs) == 1
        log_note = sheet_eng.written_logs[0][1]["note"]
        assert "API update failed" in log_note

    @pytest.mark.asyncio
    async def test_bad_relax_ignored(self):
        p = make_payload(relax="abc", fetched_min=12.0, fetched_max=18.0, inline_min_price="12.0", min_adj=0.01, max_adj=0.05)
        orch, sheet_eng, adapter = _make_orchestrator([p])
        await orch._run_one_round()
        assert len(sheet_eng.written_logs) == 1  # No crash

    @pytest.mark.asyncio
    async def test_exception_in_process_one(self):
        """Adapter raises exception — should be caught, other rows unaffected."""
        class FailingAdapter(ConfigurableMockAdapter):
            async def resolve_payload_targets(self, payload):
                raise RuntimeError("Unexpected API error")

        p1 = make_payload(product_name="Good", product_id="12345", fetched_min=12.0, fetched_max=18.0, inline_min_price="12.0", row_index=4)
        p2 = make_payload(product_name="Bad", product_id="12345", fetched_min=12.0, fetched_max=18.0, inline_min_price="12.0", row_index=5)

        sheet_engine = MockSheetEngine([p1, p2])
        pricing_engine = PricingEngine(CompetitionAnalyzer(), LogFormatter())
        orch = Orchestrator(
            sheet_engine=sheet_engine,
            pricing_engine=pricing_engine,
            adapter_registry={"mock": FailingAdapter()},
            workers=2, sleep_time=0,
        )
        await orch._run_one_round()
        # Both rows should produce logs (error or success)
        assert len(sheet_engine.written_logs) == 2

    @pytest.mark.asyncio
    async def test_empty_payloads_round(self):
        orch, sheet_eng, adapter = _make_orchestrator([])
        await orch._run_one_round()
        assert len(sheet_eng.written_logs) == 0

    @pytest.mark.asyncio
    async def test_one_row_can_expand_to_multiple_listing_updates(self):
        class ExpandingAdapter(ConfigurableMockAdapter):
            async def resolve_payload_targets(self, payload):
                payload_a = payload.model_copy(
                    update={"resolved_listing_id": "listing-a", "resolved_listing_name": "Listing A"},
                    deep=True,
                )
                payload_b = payload.model_copy(
                    update={"resolved_listing_id": "listing-b", "resolved_listing_name": "Listing B"},
                    deep=True,
                )
                return [
                    ResolvedListingTarget(payload=payload_a, listing_id="listing-a", listing_name="Listing A"),
                    ResolvedListingTarget(payload=payload_b, listing_id="listing-b", listing_name="Listing B"),
                ]

            async def prepare_pricing_input(self, target: ResolvedListingTarget) -> PreparedPricingInput:
                return make_prepared_input(
                    target.payload,
                    my_price=self.my_price,
                    competitors=self.competitors,
                    offer_id=target.listing_id,
                    product_id=f"prod-{target.listing_id}",
                    platform=self.platform,
                )

        payload = make_payload(
            fetched_min=12.0,
            fetched_max=18.0,
            inline_min_price="12.0",
            min_adj=0.01,
            max_adj=0.05,
        )
        adapter = ExpandingAdapter(
            competitors=[StandardCompetitorOffer(seller_name="rival", price=14.20, is_eligible=True)]
        )
        orch, sheet_eng, _ = _make_orchestrator([payload], adapter=adapter)

        await orch._run_one_round()

        assert len(sheet_eng.written_logs) == 1
        assert [item["offer_id"] for item in adapter.updated_prices] == ["listing-a", "listing-b"]
        note = sheet_eng.written_logs[0][1]["note"]
        assert "[Listing A]" in note
        assert "[Listing B]" in note
