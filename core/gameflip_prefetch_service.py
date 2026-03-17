from typing import Any, Optional

from clients.gameflip_client import GameflipClient
from constants.gameflip_constants import (
    GAMEFLIP_ACTIVE_STATUSES,
    GAMEFLIP_DEFAULT_SEARCH_SORT,
    GAMEFLIP_PAUSED_STATUSES,
)
from models.gameflip_models import GameflipListing
from models.runtime_models import (
    PreparedCompetition,
    PreparedCurrentOffer,
    PreparedPricingInput,
    ResolvedListingTarget,
)
from models.standard_models import PlatformIdentifiers, StandardCompetitorOffer
from utils.price_utils import cents_to_usd_decimal


class GameflipPrefetchService:
    def __init__(self, client: GameflipClient):
        self.client = client

    async def prepare_pricing_input(self, target: ResolvedListingTarget) -> PreparedPricingInput:
        listing = await self.client.listing_get(target.listing_id)
        identifiers = self._build_identifiers(listing)
        current_offer = self._normalize_current_offer(listing, identifiers.product_id)
        competition = PreparedCompetition()

        if target.payload.compare_mode > 0:
            owner_id = listing.owner or await self.client.get_owner_id()
            search_result = await self.client.listing_search(self._build_search_query(listing))
            competition = PreparedCompetition(
                offers=self._normalize_competitors(
                    search_result.listings,
                    current_listing=listing,
                    owner_id=owner_id,
                    min_price=target.payload.fetched_min_price,
                    max_price=target.payload.fetched_max_price,
                )
            )

        prepared_payload = target.payload.model_copy(
            update={
                "offer_id": identifiers.offer_id,
                "real_product_id": identifiers.product_id,
                "current_price": current_offer.price,
            },
            deep=True,
        )
        prepared_target = target.model_copy(update={"payload": prepared_payload}, deep=True)
        return PreparedPricingInput(
            payload=prepared_payload,
            target=prepared_target,
            identifiers=identifiers,
            current_offer=current_offer,
            competition=competition,
        )

    def _build_identifiers(self, listing: GameflipListing) -> PlatformIdentifiers:
        context = self._build_search_context(listing)
        return PlatformIdentifiers(
            offer_id=listing.id,
            product_id=context["product_key"],
            platform="gameflip",
        )

    def _normalize_current_offer(
        self,
        listing: GameflipListing,
        product_id: str,
    ) -> PreparedCurrentOffer:
        return PreparedCurrentOffer(
            offer_id=listing.id,
            product_id=product_id,
            price=cents_to_usd_decimal(listing.price),
            status=self._normalize_status(listing.status),
            offer_type=self._infer_offer_type(listing),
            currency="USD",
        )

    def _normalize_competitors(
        self,
        listings: list[GameflipListing],
        current_listing: GameflipListing,
        owner_id: str,
        min_price: Optional[float],
        max_price: Optional[float],
    ) -> list[StandardCompetitorOffer]:
        offers: list[StandardCompetitorOffer] = []
        for listing in listings:
            if listing.id == current_listing.id:
                continue
            if listing.owner == owner_id:
                continue
            if (listing.status or "").lower() != "onsale":
                continue
            if listing.price is None:
                continue

            price = cents_to_usd_decimal(listing.price)
            is_eligible = True
            note = None
            if min_price is not None and price < min_price:
                is_eligible = False
                note = "Price below min"
            elif max_price is not None and price > max_price:
                is_eligible = False
                note = "Price above max"

            offers.append(
                StandardCompetitorOffer(
                    seller_name=self._seller_name(listing),
                    price=price,
                    is_eligible=is_eligible,
                    note=note,
                )
            )

        offers.sort(key=lambda item: item.price)
        return offers

    @staticmethod
    def _build_search_context(listing: GameflipListing) -> dict[str, Any]:
        product_key = "gf:" + "::".join([
            listing.category or "",
            listing.upc or "",
            listing.platform or "",
            "|".join(sorted(listing.tags or [])),
            listing.name or "",
        ])
        return {
            "offer_id": listing.id,
            "owner": listing.owner,
            "category": listing.category,
            "upc": listing.upc,
            "platform": listing.platform,
            "tags": listing.tags or [],
            "name": listing.name,
            "product_key": product_key,
        }

    @staticmethod
    def _build_search_query(listing: GameflipListing) -> dict[str, Any]:
        query: dict[str, Any] = {
            "status": "onsale",
            "sort": GAMEFLIP_DEFAULT_SEARCH_SORT,
            "limit": 50,
        }
        if listing.category:
            query["category"] = listing.category
        if listing.upc:
            query["upc"] = listing.upc
        elif listing.name:
            query["term"] = listing.name
        if listing.platform:
            query["platform"] = listing.platform
        if listing.tags:
            query["tags"] = "^".join(listing.tags)
        return query

    @staticmethod
    def _normalize_status(status: Optional[str]) -> str:
        normalized = (status or "").lower()
        if normalized in GAMEFLIP_ACTIVE_STATUSES:
            return "active"
        if normalized in GAMEFLIP_PAUSED_STATUSES:
            return "paused"
        return "inactive"

    @staticmethod
    def _infer_offer_type(listing: GameflipListing) -> str:
        if listing.digital:
            if (listing.category or "").upper() == "DIGITAL_INGAME":
                return "dropshipping"
            return "key"
        if (listing.category or "").upper() == "ACCOUNT":
            return "account"
        return "gift"

    @staticmethod
    def _seller_name(listing: GameflipListing) -> str:
        return listing.owner or f"gameflip:{listing.id}"
