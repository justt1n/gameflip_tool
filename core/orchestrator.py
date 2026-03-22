import asyncio
import logging
import re
from datetime import datetime
from typing import Dict, Optional, Tuple

from core.pricing_engine import PricingEngine
from core.sheet_engine import SheetEngine
from interfaces.marketplace_adapter import IMarketplaceAdapter
from models.runtime_models import ResolvedListingTarget
from models.sheet_models import Payload

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Main automation loop.

    FLOW:
    1. Hydrate sheet row
    2. Resolve row into owned listings from local artifacts
    3. Prefetch live marketplace data for each target
    4. Feed prepared inputs into the pricing engine
    5. Apply updates after the engine returns decisions
    """

    def __init__(
        self,
        sheet_engine: SheetEngine,
        pricing_engine: PricingEngine,
        adapter_registry: Dict[str, IMarketplaceAdapter],
        workers: int = 1,
        sleep_time: int = 5,
        target_workers: int = 1,
    ):
        self.sheet_engine = sheet_engine
        self.pricing_engine = pricing_engine
        self.adapter_registry = adapter_registry
        self.workers = workers
        self.sleep_time = sleep_time
        self.target_workers = max(1, target_workers)

    def detect_platform(self, *values: str) -> str:
        """
        Detect marketplace platform from one or more raw identifier/search strings.
        """
        seen_non_empty = False
        domain_map = {
            "gameflip.com": "gameflip",
            "driffle.com": "driffle",
            "g2a.com": "g2a",
            "gamivo.com": "gamivo",
            "kinguin.net": "kinguin",
        }
        for raw_value in values:
            if not raw_value:
                continue
            seen_non_empty = True
            lowered = raw_value.lower()
            for domain, platform in domain_map.items():
                if domain in lowered:
                    return platform

        if not seen_non_empty:
            raise ValueError("product_id is empty")

        if len(self.adapter_registry) == 1:
            return next(iter(self.adapter_registry))

        raise ValueError("Cannot detect platform from the payload search fields")

    async def run_forever(self):
        """Main infinite loop with error recovery."""
        while True:
            try:
                logger.info("===== NEW ROUND =====")
                await self._run_one_round()
                logger.info(f"Round finished. Sleeping {self.sleep_time}s...")
                await asyncio.sleep(self.sleep_time)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.critical(f"Error in main loop: {e}. Retrying in 30s.", exc_info=True)
                await asyncio.sleep(30)

    async def _run_one_round(self):
        """Process all enabled payloads in batched chunks."""
        payloads = await asyncio.to_thread(self.sheet_engine.get_payloads)
        if not payloads:
            logger.info("No payloads to process.")
            return

        logger.info(f"Found {len(payloads)} payloads. Workers={self.workers}")

        sheets_lock = asyncio.Semaphore(1)  # Serialize sheet reads

        for i in range(0, len(payloads), self.workers):
            batch = payloads[i:i + self.workers]
            batch_num = (i // self.workers) + 1
            logger.info(f"--- Batch {batch_num} ({len(batch)} items) ---")

            tasks = [
                self._process_one(p, sheets_lock)
                for p in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Collect successful results for logging
            updates = []
            for r in results:
                if isinstance(r, Exception):
                    logger.error(f"Task failed: {r}", exc_info=True)
                elif r is not None:
                    updates.append(r)

            # Batch-write logs
            if updates:
                await asyncio.to_thread(self.sheet_engine.batch_write_logs, updates)

    async def _process_one(
        self, payload: Payload, sheets_lock: asyncio.Semaphore
    ) -> Optional[Tuple[Payload, dict]]:
        """Process a single payload end-to-end."""
        try:
            # 1. Hydrate (serialized via lock to respect Google API rate limits)
            async with sheets_lock:
                hydrated = await asyncio.to_thread(
                    self.sheet_engine.hydrate_payload, payload
                )

            # 2. Detect platform
            platform = self.detect_platform(
                str(hydrated.product_id or ""),
                str(hydrated.product_compare or ""),
                str(hydrated.product_link or ""),
            )
            adapter = self.adapter_registry.get(platform)
            if not adapter:
                return (payload, {
                    'note': f"Error: No adapter for platform '{platform}'",
                    'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            # 3. Expand one row into concrete owned listings when supported
            resolved_targets = await self._expand_payloads(hydrated, adapter)
            if not resolved_targets:
                return (payload, {
                    'note': "Error: No owned listings resolved for this row",
                    'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            # 4. Run pricing engine and updates for each resolved listing
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            note_chunks = await self._process_resolved_targets(resolved_targets, adapter, payload)
            final_note = self._compile_target_notes(note_chunks)

            log_data = {
                'note': final_note,
                'last_update': timestamp
            }

            # 5. Relax
            if payload.relax:
                try:
                    sleep_s = int(payload.relax)
                    if sleep_s > 0:
                        await asyncio.sleep(sleep_s)
                except (ValueError, TypeError):
                    pass

            return (payload, log_data) if log_data else None

        except Exception as e:
            logger.error(f"Error processing row {payload.row_index}: {e}", exc_info=True)
            return (payload, {
                'note': f"Error: {e}",
                'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

    async def _expand_payloads(
        self,
        payload: Payload,
        adapter: IMarketplaceAdapter,
    ) -> list[ResolvedListingTarget]:
        return await adapter.resolve_payload_targets(payload)

    async def _process_resolved_targets(
        self,
        resolved_targets: list[ResolvedListingTarget],
        adapter: IMarketplaceAdapter,
        original_payload: Payload,
    ) -> list[str]:
        semaphore = asyncio.Semaphore(self.target_workers)

        async def run_target(target: ResolvedListingTarget) -> str:
            async with semaphore:
                return await self._process_resolved_target(target, adapter, original_payload)

        tasks = [run_target(target) for target in resolved_targets]
        return await asyncio.gather(*tasks)

    async def _process_resolved_target(
        self,
        target: ResolvedListingTarget,
        adapter: IMarketplaceAdapter,
        original_payload: Payload,
    ) -> str:
        prepared = await adapter.prepare_pricing_input(target)
        result = await self.pricing_engine.process(prepared)
        display_offers = prepared.competition.offers
        if result.analysis is not None and result.analysis.top_sellers_for_log is not None:
            display_offers = result.analysis.top_sellers_for_log

        competitor_count = len(display_offers)
        raw_competitor_count = prepared.competition.raw_count
        best_price = min((offer.price for offer in display_offers), default=None)
        base_log = result.log_message or "INFO\nNo log message"
        compare_enabled = target.payload.compare_mode > 0

        if result.status == 1 and result.final_price and result.update_command:
            success = await adapter.update_price(
                offer_id=result.update_command.offer_id,
                new_price=result.update_command.new_price,
                current_version=prepared.current_offer.version,
                current_status=prepared.current_offer.raw_status,
            )
            if success:
                note = self._build_audit_note(
                    target=target,
                    base_log=base_log,
                    competitors=competitor_count,
                    raw_competitors=raw_competitor_count,
                    best_price=best_price,
                    edited=True,
                    final_price=result.final_price.price,
                    compare_enabled=compare_enabled,
                )
                self._log_target_result(
                    payload=original_payload,
                    target=target,
                    edited=True,
                    final_price=result.final_price.price,
                    reason="updated",
                )
                return note

            note = self._build_audit_note(
                target=target,
                base_log=base_log,
                competitors=competitor_count,
                raw_competitors=raw_competitor_count,
                best_price=best_price,
                edited=False,
                final_price=result.final_price.price,
                reason="update failed",
                compare_enabled=compare_enabled,
            )
            self._log_target_result(
                payload=original_payload,
                target=target,
                edited=False,
                final_price=result.final_price.price,
                reason="update failed",
                level="warning",
            )
            return note

        reason = "no change" if result.status == 2 else "skip"
        note = self._build_audit_note(
            target=target,
            base_log=base_log,
            competitors=competitor_count,
            raw_competitors=raw_competitor_count,
            best_price=best_price,
            edited=False,
            final_price=result.final_price.price if result.final_price else None,
            reason=reason,
            compare_enabled=compare_enabled,
        )
        self._log_target_result(
            payload=original_payload,
            target=target,
            edited=False,
            final_price=result.final_price.price if result.final_price else None,
            reason=reason,
        )
        return note

    @staticmethod
    def _log_target_result(
        payload: Payload,
        target: ResolvedListingTarget,
        edited: bool,
        final_price: Optional[float],
        reason: str,
        level: str = "info",
    ) -> None:
        listing_label = target.listing_name or target.listing_id or payload.product_name
        final_text = f"{final_price:.3f}" if final_price is not None else "N/A"
        action = "EDIT" if edited else "NO_EDIT"
        message = (
            f"Row {payload.row_index} [{action}] {listing_label} -> {final_text} "
            f"(reason: {reason})"
        )
        getattr(logger, level)(message)

    @staticmethod
    def _build_audit_note(
        target: ResolvedListingTarget,
        base_log: str,
        competitors: int,
        raw_competitors: int,
        best_price: Optional[float],
        edited: bool,
        final_price: Optional[float] = None,
        reason: Optional[str] = None,
        compare_enabled: bool = True,
    ) -> str:
        item_label = target.listing_name or target.listing_id
        best_text = f"{best_price:.3f}" if best_price is not None else "N/A"
        final_text = f"{final_price:.3f}" if final_price is not None else "N/A"
        action = "EDIT" if edited else "NO_EDIT"

        parts = [
            f"[Listing] {item_label}",
            base_log.rstrip(),
            Orchestrator._build_compare_meta_line(
                compare_mode=target.payload.compare_mode,
                feedback_min=target.payload.feedback_min,
                raw_competitors=raw_competitors,
                competitors=competitors,
                compare_enabled=compare_enabled,
            ),
            f"- Best Price Found: {best_text}",
            f"- Final Price: {final_text}",
            f"- Action: {action}",
        ]
        if compare_enabled:
            parts.insert(3, f"- Competitors: {competitors}")
        else:
            parts.insert(3, "- Competitors: skipped (Mode 0 / No Compare)")
        if reason:
            parts.append(f"- Reason: {reason}")
        return "\n".join(parts)

    @staticmethod
    def _build_compare_meta_line(
        compare_mode: int,
        feedback_min: Optional[float],
        raw_competitors: int,
        competitors: int,
        compare_enabled: bool,
    ) -> str:
        feedback_text = (
            str(int(feedback_min))
            if feedback_min is not None and float(feedback_min).is_integer()
            else (f"{feedback_min}" if feedback_min is not None else "N/A")
        )
        raw_text = str(raw_competitors) if compare_enabled else "skipped"
        after_filters_text = str(competitors) if compare_enabled else "skipped"
        return (
            f"- Compare Mode: {compare_mode}"
            f" | Feedback Min: {feedback_text}"
            f" | Raw Hits: {raw_text}"
            f" | After Filters: {after_filters_text}"
        )

    @classmethod
    def _compile_target_notes(cls, note_chunks: list[str]) -> str:
        if len(note_chunks) == 1:
            return note_chunks[0]

        grouped: dict[str, dict[str, object]] = {}
        for note in note_chunks:
            label, body = cls._split_note(note)
            key = cls._normalize_note_body(body)
            group = grouped.setdefault(key, {"labels": [], "body": body})
            labels = group["labels"]
            assert isinstance(labels, list)
            labels.append(label)

        if len(grouped) == 1:
            only_group = next(iter(grouped.values()))
            return "\n".join([
                f"MULTI_TARGET ({len(note_chunks)})",
                f"- Listings: {cls._format_listing_labels(only_group['labels'])}",
                str(only_group["body"]),
            ])

        compiled = [f"MULTI_TARGET ({len(note_chunks)})"]
        for idx, group in enumerate(grouped.values(), start=1):
            compiled.append(f"GROUP {idx} ({len(group['labels'])})")
            compiled.append(f"- Listings: {cls._format_listing_labels(group['labels'])}")
            compiled.append(str(group["body"]))
        return "\n\n".join(compiled)

    @staticmethod
    def _split_note(note: str) -> tuple[str, str]:
        lines = note.splitlines()
        if lines and lines[0].startswith("[Listing] "):
            return lines[0].replace("[Listing] ", "", 1), "\n".join(lines[1:]).strip()
        return "Unknown listing", note.strip()

    @staticmethod
    def _normalize_note_body(body: str) -> str:
        return re.sub(r"\[\d{2}/\d{2} \d{2}:\d{2}\]", "[TIME]", body)

    @staticmethod
    def _format_listing_labels(labels: list[str]) -> str:
        counts: dict[str, int] = {}
        for label in labels:
            counts[label] = counts.get(label, 0) + 1

        formatted = []
        for label, count in counts.items():
            entry = f"[{label}]"
            if count > 1:
                entry = f"{entry} x{count}"
            formatted.append(entry)
        return "; ".join(formatted)
