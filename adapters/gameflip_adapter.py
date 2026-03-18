from typing import Optional

from clients.gameflip_client import GameflipClient
from core.gameflip_artifact_store import GameflipArtifactStore
from core.gameflip_listing_resolver import GameflipListingResolver
from core.gameflip_prefetch_service import GameflipPrefetchService
from core.gameflip_price_updater import GameflipPriceUpdater
from interfaces.marketplace_adapter import IMarketplaceAdapter
from models.runtime_models import PreparedPricingInput, ResolvedListingTarget
from models.sheet_models import Payload


class GameflipAdapter(IMarketplaceAdapter):
    """Thin orchestration facade around resolver, prefetch, and updater services."""

    def __init__(
        self,
        client: GameflipClient,
        listings_dump_path: str,
        listings_index_path: str,
    ):
        self.client = client
        self.artifact_store = GameflipArtifactStore(
            dump_path=listings_dump_path,
            index_path=listings_index_path,
        )
        self.listing_resolver = GameflipListingResolver(self.artifact_store)
        self.prefetch_service = GameflipPrefetchService(client)
        self.price_updater = GameflipPriceUpdater(client)

    def get_platform_name(self) -> str:
        return "gameflip"

    async def resolve_payload_targets(self, payload: Payload) -> list[ResolvedListingTarget]:
        return await self.listing_resolver.resolve_payload(payload)

    async def prepare_pricing_input(self, target: ResolvedListingTarget) -> PreparedPricingInput:
        return await self.prefetch_service.prepare_pricing_input(target)

    async def update_price(
        self,
        offer_id: str,
        new_price: float,
        current_version: Optional[str | int] = None,
        current_status: Optional[str] = None,
    ) -> bool:
        return await self.price_updater.update_price(
            offer_id,
            new_price,
            current_version=current_version,
            current_status=current_status,
        )

    async def close(self):
        await self.client.close()
