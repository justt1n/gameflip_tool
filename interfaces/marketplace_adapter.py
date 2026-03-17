from abc import ABC, abstractmethod

from models.runtime_models import PreparedPricingInput, ResolvedListingTarget


class IMarketplaceAdapter(ABC):
    @abstractmethod
    def get_platform_name(self) -> str:
        ...

    @abstractmethod
    async def resolve_payload_targets(self, payload) -> list[ResolvedListingTarget]:
        """Expand one sheet row into concrete marketplace targets."""
        ...

    @abstractmethod
    async def prepare_pricing_input(self, target: ResolvedListingTarget) -> PreparedPricingInput:
        """Fetch and normalize all marketplace data needed by the pricing engine."""
        ...

    @abstractmethod
    async def update_price(self, offer_id: str, new_price: float) -> bool:
        """Apply the final calculated price to the marketplace listing."""
        ...

    async def close(self):
        pass
