import logging
from typing import Optional

from clients.gameflip_client import GameflipAPIError
from clients.gameflip_client import GameflipClient
from core.gameflip_artifact_store import GameflipArtifactStore
from core.gameflip_listing_resolver import GameflipListingResolver, ListingSearchDefinition
from models.gameflip_models import GameflipListing
from models.runtime_models import DuplicateListingResult
from models.sheet_models import Payload
from utils.price_utils import usd_decimal_to_cents

logger = logging.getLogger(__name__)


class GameflipDuplicateService:
    def __init__(
        self,
        client: GameflipClient,
        artifact_store: GameflipArtifactStore,
        resolver: GameflipListingResolver,
        include_ready: bool = False,
        skip_digital_goods_put: bool = True,
    ):
        self.client = client
        self.artifact_store = artifact_store
        self.resolver = resolver
        self.include_ready = include_ready
        self.skip_digital_goods_put = skip_digital_goods_put

    async def ensure_duplicate_listing_quota(
        self,
        payload: Payload,
        duplicate_price: Optional[float],
    ) -> DuplicateListingResult:
        if not payload.is_duplicate_listing_enabled:
            return DuplicateListingResult()

        target = payload.duplicate_listing_target
        if target is None:
            return DuplicateListingResult(
                append_note="\n".join([
                    "Duplicate skipped",
                    "Reason: invalid DUPLICATE_LISTING value",
                ])
            )

        definition = self.resolver.build_search_definition(payload)
        eligible_definition = definition.model_copy(update={"status": None}, deep=True)
        candidates = await self._load_matching_owned_listings(eligible_definition, target=target)
        active_before = len(candidates)

        if active_before == 0:
            return DuplicateListingResult(
                override_note="\n".join([
                    "0 OFFER REMAIN",
                    "Duplicate skipped",
                    f"Target: k={target}, active=0",
                    "Reason: no active source listing",
                ]),
                active_before=0,
                active_after=0,
            )

        if active_before >= target:
            return DuplicateListingResult(
                append_note="\n".join([
                    "Duplicate not needed",
                    f"Target: k={target}, active={active_before}",
                ]),
                active_before=active_before,
                active_after=active_before,
            )

        source_listing = self._pick_source_listing(candidates)
        if source_listing is None:
            return DuplicateListingResult(
                override_note="\n".join([
                    "0 OFFER REMAIN",
                    "Duplicate skipped",
                    f"Target: k={target}, active=0",
                    "Reason: no active source listing",
                ]),
                active_before=0,
                active_after=0,
            )

        if source_listing.digital and self.skip_digital_goods_put:
            return DuplicateListingResult(
                append_note="\n".join([
                    "Duplicate skipped",
                    f"Target: k={target}, active={active_before}",
                    "Reason: digital listing requires digital_goods_put",
                ]),
                active_before=active_before,
                active_after=active_before,
            )

        created: list[GameflipListing] = []
        photo_failures = 0
        create_failures = 0
        desired_creates = target - active_before

        for _ in range(desired_creates):
            try:
                price = duplicate_price
                if price is None:
                    price = (source_listing.price or 0) / 100 if source_listing.price is not None else 0.0
                created_listing = await self.client.listing_post(
                    self._build_create_payload(source_listing, price)
                )

                try:
                    await self._copy_photos(source_listing, created_listing)
                except Exception:
                    photo_failures += 1
                    logger.exception(
                        "Failed to copy one or more photos from %s to %s",
                        source_listing.id,
                        created_listing.id,
                    )

                created_listing = await self._patch_with_retry(
                    created_listing.id,
                    [{"op": "replace", "path": "/status", "value": "onsale"}],
                    version=created_listing.version,
                )
                created.append(created_listing)
            except Exception:
                create_failures += 1
                logger.exception("Failed to create duplicate listing from source %s", source_listing.id)

        if created:
            self.artifact_store.merge_owned_listings(created)

        active_after = active_before + len(created)

        if create_failures or photo_failures:
            reason = "failed to create remaining listings"
            if not create_failures and photo_failures:
                reason = "failed to copy photos for one or more listings"
            return DuplicateListingResult(
                append_note="\n".join([
                    "Duplicate partial failure",
                    f"Target: k={target}, active_before={active_before}, created={len(created)}",
                    f"Reason: {reason}",
                ]),
                created_count=len(created),
                active_before=active_before,
                active_after=active_after,
            )

        return DuplicateListingResult(
            append_note="\n".join([
                f"Duplicate created: {len(created)}",
                f"Target: k={target}, active_before={active_before}, active_after={active_after}",
            ]),
            created_count=len(created),
            active_before=active_before,
            active_after=active_after,
        )

    async def _load_matching_owned_listings(
        self,
        definition: ListingSearchDefinition,
        target: Optional[int] = None,
    ) -> list[GameflipListing]:
        owner_id = await self.client.get_owner_id()
        query: dict[str, object] = {
            "owner": owner_id,
            "limit": max(1, target or 1),
            "status": ",".join(self._eligible_statuses()),
        }
        if definition.platform:
            query["platform"] = definition.platform
        if definition.category:
            query["category"] = definition.category
        if definition.digital_region:
            query["digital_region"] = definition.digital_region
        if definition.term:
            query["term"] = definition.term
        if definition.tags:
            query["tags"] = "^".join(definition.tags)

        result = await self.client.listing_search_all(query, max_listings=max(1, target or 1))
        filtered = [
            listing for listing in result.listings
            if listing.owner == owner_id and (listing.status or "").lower() in self._eligible_statuses()
        ]
        index = self.artifact_store.build_owned_listings_index(filtered)
        matches = self.resolver.match_owned_listings(definition, index)
        match_ids = {entry.id for entry in matches}
        return [listing for listing in filtered if listing.id in match_ids]

    def _eligible_statuses(self) -> set[str]:
        statuses = {"onsale"}
        if self.include_ready:
            statuses.add("ready")
        return statuses

    @staticmethod
    def _pick_source_listing(listings: list[GameflipListing]) -> Optional[GameflipListing]:
        ordered = sorted(
            listings,
            key=lambda item: (0 if (item.status or "").lower() == "onsale" else 1, item.id),
        )
        return ordered[0] if ordered else None

    @staticmethod
    def _build_create_payload(source: GameflipListing, price: float) -> dict:
        payload = {
            "name": source.name,
            "description": source.description,
            "category": source.category,
            "platform": source.platform,
            "accept_currency": source.accept_currency,
            "price": usd_decimal_to_cents(price),
            "upc": source.upc,
            "condition": source.condition,
            "digital": source.digital,
            "digital_region": source.digital_region,
            "digital_deliverable": source.digital_deliverable,
            "expire_in_days": source.expire_in_days,
            "shipping_paid_by": source.shipping_paid_by,
            "shipping_fee": source.shipping_fee,
            "shipping_within_days": source.shipping_within_days,
            "shipping_from_state": source.shipping_from_state,
            "shipping_predefined_package": source.shipping_predefined_package,
            "tags": list(source.tags or []),
        }
        return {key: value for key, value in payload.items() if value is not None}

    async def _copy_photos(self, source: GameflipListing, target: GameflipListing) -> None:
        if not source.photo:
            return

        items = list(source.photo.items())
        cover_id = source.cover_photo
        for photo_id, photo in sorted(items, key=lambda item: item[1].display_order or 9999):
            if not photo.view_url or (photo.status or "").lower() == "deleted":
                continue
            display_order = None if photo_id == cover_id else photo.display_order
            await self.client.upload_photo_from_url(target.id, photo.view_url, display_order=display_order)

    async def _patch_with_retry(
        self,
        listing_id: str,
        operations: list[dict],
        version: str | int | None,
    ) -> GameflipListing:
        try:
            return await self.client.listing_patch(listing_id, operations, if_match=version)
        except GameflipAPIError as exc:
            if exc.code != 412:
                raise
            refreshed = await self.client.listing_get(listing_id)
            return await self.client.listing_patch(listing_id, operations, if_match=refreshed.version)
