import asyncio
import logging
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
        sleep_time: int = 5
    ):
        self.sheet_engine = sheet_engine
        self.pricing_engine = pricing_engine
        self.adapter_registry = adapter_registry
        self.workers = workers
        self.sleep_time = sleep_time
        self._running = True

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
        while self._running:
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
            note_chunks = []
            for target in resolved_targets:
                note = await self._process_resolved_target(target, adapter, payload)
                if len(resolved_targets) > 1:
                    label = (
                        target.listing_name
                        or target.listing_id
                    )
                    note = f"[{label}] {note}"
                note_chunks.append(note)

            log_data = {
                'note': "\n\n".join(note_chunks),
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

    def stop(self):
        """Signal the orchestrator to stop after current round."""
        self._running = False

    async def _expand_payloads(
        self,
        payload: Payload,
        adapter: IMarketplaceAdapter,
    ) -> list[ResolvedListingTarget]:
        return await adapter.resolve_payload_targets(payload)

    async def _process_resolved_target(
        self,
        target: ResolvedListingTarget,
        adapter: IMarketplaceAdapter,
        original_payload: Payload,
    ) -> str:
        prepared = await adapter.prepare_pricing_input(target)
        result = await self.pricing_engine.process(prepared)
        if result.status == 1 and result.final_price and result.update_command:
            success = await adapter.update_price(
                offer_id=result.update_command.offer_id,
                new_price=result.update_command.new_price,
            )
            if success:
                logger.info(
                    f"SUCCESS: {original_payload.product_name} -> {result.final_price.price:.3f}"
                )
                return result.log_message or "Updated successfully"
            return f"{result.log_message}\n\nERROR: API update failed."
        return result.log_message or "No log message"
