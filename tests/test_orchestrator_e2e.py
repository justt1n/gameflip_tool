import asyncio
import time
import pytest
from core.competition_analyzer import CompetitionAnalyzer
from core.log_formatter import LogFormatter
from core.orchestrator import Orchestrator
from core.pricing_engine import PricingEngine
from models.runtime_models import DuplicateListingResult, PreparedPricingInput, ResolvedListingTarget
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


def _make_orchestrator(payloads, adapter=None, workers=1, hydration=None, target_workers=1):
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
        target_workers=target_workers,
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
    async def test_no_change_is_logged_to_console(self, caplog):
        p = make_payload(
            product_name="No Change Item",
            compare_mode="0",
            fetched_min=12.0,
            fetched_max=18.0,
            inline_min_price="12.0",
            row_index=7,
        )
        adapter = ConfigurableMockAdapter(my_price=12.0)
        orch, sheet_eng, _ = _make_orchestrator([p], adapter=adapter)

        with caplog.at_level("INFO"):
            await orch._run_one_round()

        assert "Row 7 [NO_EDIT] No Change Item -> N/A (reason: no change)" in caplog.text

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
        assert "update failed" in log_note

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
        assert "MULTI_TARGET (2)" in note

    @pytest.mark.asyncio
    async def test_duplicate_append_note_preserves_pricing_log(self):
        class DuplicateAppendAdapter(ConfigurableMockAdapter):
            async def ensure_duplicate_listing_quota(self, payload, duplicate_price):
                return DuplicateListingResult(
                    append_note="Duplicate created: 1\nTarget: k=2, active_before=1, active_after=2"
                )

        payload = make_payload(
            fetched_min=12.0,
            fetched_max=18.0,
            inline_min_price="12.0",
            min_adj=0.01,
            max_adj=0.05,
        )
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 2
        orch, sheet_eng, _ = _make_orchestrator([payload], adapter=DuplicateAppendAdapter())

        await orch._run_one_round()

        note = sheet_eng.written_logs[0][1]["note"]
        assert "- Final Price:" in note
        assert "Duplicate created: 1" in note

    @pytest.mark.asyncio
    async def test_duplicate_override_note_for_zero_offer_remain(self):
        class DuplicateOverrideAdapter(ConfigurableMockAdapter):
            async def resolve_payload_targets(self, payload):
                raise ValueError("No owned listings matched search definition")

            async def ensure_duplicate_listing_quota(self, payload, duplicate_price):
                return DuplicateListingResult(
                    override_note="0 OFFER REMAIN\nDuplicate skipped\nTarget: k=2, active=0\nReason: no active source listing"
                )

        payload = make_payload(
            fetched_min=12.0,
            fetched_max=18.0,
            inline_min_price="12.0",
        )
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 2
        orch, sheet_eng, _ = _make_orchestrator([payload], adapter=DuplicateOverrideAdapter())

        await orch._run_one_round()

        note = sheet_eng.written_logs[0][1]["note"]
        assert note.startswith("0 OFFER REMAIN")

    @pytest.mark.asyncio
    async def test_multi_target_note_groups_identical_bodies(self):
        grouped = Orchestrator._compile_target_notes([
            "[Listing] Listing A\nUPDATE\n[18/03 21:47] Updated (no comparison): 12.000\n- Competitors: 0",
            "[Listing] Listing B\nUPDATE\n[18/03 21:48] Updated (no comparison): 12.000\n- Competitors: 0",
        ])

        assert "MULTI_TARGET (2)" in grouped
        assert "GROUP 1" not in grouped
        assert "- Listings: [Listing A]; [Listing B]" in grouped

    @pytest.mark.asyncio
    async def test_targets_can_run_concurrently(self):
        class SlowExpandingAdapter(ConfigurableMockAdapter):
            async def resolve_payload_targets(self, payload):
                targets = []
                for idx in range(3):
                    payload_copy = payload.model_copy(
                        update={
                            "resolved_listing_id": f"listing-{idx}",
                            "resolved_listing_name": f"Listing {idx}",
                        },
                        deep=True,
                    )
                    targets.append(
                        ResolvedListingTarget(
                            payload=payload_copy,
                            listing_id=f"listing-{idx}",
                            listing_name=f"Listing {idx}",
                        )
                    )
                return targets

            async def prepare_pricing_input(self, target: ResolvedListingTarget) -> PreparedPricingInput:
                await asyncio.sleep(0.05)
                return make_prepared_input(
                    target.payload,
                    my_price=self.my_price,
                    competitors=self.competitors,
                    offer_id=target.listing_id,
                    product_id=f"prod-{target.listing_id}",
                    platform=self.platform,
                )

            async def update_price(self, offer_id: str, new_price: float, current_version=None, current_status=None) -> bool:
                await asyncio.sleep(0.05)
                return await super().update_price(
                    offer_id,
                    new_price,
                    current_version=current_version,
                    current_status=current_status,
                )

        payload = make_payload(
            fetched_min=12.0,
            fetched_max=18.0,
            inline_min_price="12.0",
            min_adj=0.01,
            max_adj=0.05,
        )
        adapter = SlowExpandingAdapter(
            competitors=[StandardCompetitorOffer(seller_name="rival", price=14.20, is_eligible=True)]
        )
        orch, sheet_eng, _ = _make_orchestrator(
            [payload],
            adapter=adapter,
            target_workers=3,
        )

        started = time.perf_counter()
        await orch._run_one_round()
        elapsed = time.perf_counter() - started

        assert len(sheet_eng.written_logs) == 1
        assert elapsed < 0.25

    @pytest.mark.asyncio
    async def test_mode_zero_skips_when_target_already_matches_current_price(self):
        payload = make_payload(
            compare_mode="0",
            fetched_min=12.0,
            inline_min_price="12.0",
        )
        adapter = ConfigurableMockAdapter(my_price=12.0)
        orch, sheet_eng, _ = _make_orchestrator([payload], adapter=adapter)

        await orch._run_one_round()

        assert len(sheet_eng.written_logs) == 1
        assert len(adapter.updated_prices) == 0
        note = sheet_eng.written_logs[0][1]["note"]
        assert "SKIP" in note
        assert "Compare Mode: 0 | Feedback Min: N/A | Raw Hits: skipped | After Filters: skipped" in note
        assert "Competitors: skipped (Mode 0 / No Compare)" in note

    @pytest.mark.asyncio
    async def test_compare_note_includes_feedback_and_raw_hit_counts(self):
        class RawCountAdapter(ConfigurableMockAdapter):
            async def prepare_pricing_input(self, target: ResolvedListingTarget) -> PreparedPricingInput:
                prepared = await super().prepare_pricing_input(target)
                prepared.competition.raw_count = 39
                return prepared

        payload = make_payload(
            fetched_min=12.0,
            fetched_max=18.0,
            inline_min_price="12.0",
            min_adj=0.01,
            max_adj=0.05,
        )
        payload.feedback_min = 100
        adapter = RawCountAdapter(
            competitors=[StandardCompetitorOffer(seller_name=f"rival-{idx}", price=14.20 + idx, is_eligible=True) for idx in range(12)]
        )
        orch, sheet_eng, _ = _make_orchestrator([payload], adapter=adapter)

        await orch._run_one_round()

        assert len(sheet_eng.written_logs) == 1
        note = sheet_eng.written_logs[0][1]["note"]
        assert "Compare Mode: 1 | Feedback Min: 100 | Raw Hits: 39 | After Filters: 12" in note
        assert "Competitors: 12" in note

    @pytest.mark.asyncio
    async def test_compare_note_after_filters_respects_blacklist(self):
        class RawCountAdapter(ConfigurableMockAdapter):
            async def prepare_pricing_input(self, target: ResolvedListingTarget) -> PreparedPricingInput:
                prepared = await super().prepare_pricing_input(target)
                prepared.competition.raw_count = 39
                return prepared

        payload = make_payload(
            fetched_min=12.0,
            fetched_max=18.0,
            inline_min_price="12.0",
            min_adj=0.01,
            max_adj=0.05,
        )
        payload.fetched_black_list = ["rival-0"]
        adapter = RawCountAdapter(
            competitors=[
                StandardCompetitorOffer(seller_name="rival-0", price=14.20, is_eligible=True),
                StandardCompetitorOffer(seller_name="rival-1", price=14.40, is_eligible=True),
                StandardCompetitorOffer(seller_name="rival-2", price=14.60, is_eligible=True),
            ]
        )
        orch, sheet_eng, _ = _make_orchestrator([payload], adapter=adapter)

        await orch._run_one_round()

        assert len(sheet_eng.written_logs) == 1
        note = sheet_eng.written_logs[0][1]["note"]
        assert "Compare Mode: 1 | Feedback Min: N/A | Raw Hits: 39 | After Filters: 2" in note
        assert "Competitors: 2" in note
        assert "Best Price Found: 14.400" in note
