import logging
from types import SimpleNamespace

from clients.gameflip_client import GameflipAPIError, GameflipClient
from constants.gameflip_constants import GAMEFLIP_DEFAULT_LISTING_STATUS
from utils.price_utils import usd_decimal_to_cents

logger = logging.getLogger(__name__)


class GameflipPriceUpdater:
    PRICE_LOCK_MESSAGE = "Cannot change 'price' when status is onsale"
    EDIT_STATUS = "draft"

    def __init__(self, client: GameflipClient):
        self.client = client

    async def update_price(
        self,
        offer_id: str,
        new_price: float,
        current_version: str | int | None = None,
        current_status: str | None = None,
    ) -> bool:
        cents = usd_decimal_to_cents(new_price)
        current = None
        version = current_version
        normalized_status = (current_status or "").lower()
        try:
            if version is None:
                current = await self.client.listing_get(offer_id)
                version = current.version
                normalized_status = (current.status or "").lower()
            elif normalized_status == GAMEFLIP_DEFAULT_LISTING_STATUS:
                snapshot = SimpleNamespace(version=version, status=normalized_status)
                return await self._update_via_pause_resume(offer_id, cents, snapshot)
            await self._patch_with_retry(
                offer_id,
                [{"op": "replace", "path": "/price", "value": cents}],
                version=version,
            )
            return True
        except GameflipAPIError as exc:
            if self._is_onsale_price_lock(
                exc,
                current_status=(current.status if current else normalized_status),
            ):
                return await self._update_via_pause_resume(offer_id, cents, current)

            if exc.code != 412:
                logger.exception("Gameflip price update failed for %s", offer_id)
                return False

            try:
                refreshed = await self.client.listing_get(offer_id)
                await self._patch_with_retry(
                    offer_id,
                    [{"op": "replace", "path": "/price", "value": cents}],
                    version=refreshed.version,
                )
                return True
            except GameflipAPIError:
                logger.exception("Gameflip retry after version conflict failed for %s", offer_id)
                return False

    async def _update_via_pause_resume(self, offer_id: str, cents: int, current_listing) -> bool:
        try:
            listing = current_listing or await self.client.listing_get(offer_id)
            original_status = (listing.status or "").lower()

            if original_status != GAMEFLIP_DEFAULT_LISTING_STATUS:
                logger.error(
                    "Cannot apply pause/resume fallback for %s: unexpected status '%s'",
                    offer_id,
                    original_status,
                )
                return False

            paused = await self._patch_with_retry(
                offer_id,
                [{"op": "replace", "path": "/status", "value": self.EDIT_STATUS}],
                version=listing.version,
            )

            priced = await self._patch_with_retry(
                offer_id,
                [{"op": "replace", "path": "/price", "value": cents}],
                version=paused.version,
            )

            await self._patch_with_retry(
                offer_id,
                [{"op": "replace", "path": "/status", "value": GAMEFLIP_DEFAULT_LISTING_STATUS}],
                version=priced.version,
            )
            return True
        except GameflipAPIError:
            logger.exception("Gameflip pause/resume price update failed for %s", offer_id)
            return False

    async def _patch_with_retry(self, offer_id: str, operations: list[dict], version: str | int | None):
        try:
            return await self.client.listing_patch(offer_id, operations, if_match=version)
        except GameflipAPIError as exc:
            if exc.code != 412:
                raise
            refreshed = await self.client.listing_get(offer_id)
            return await self.client.listing_patch(offer_id, operations, if_match=refreshed.version)

    def _is_onsale_price_lock(self, exc: GameflipAPIError, current_status: str | None) -> bool:
        message = str(exc)
        status = (current_status or "").lower()
        return (
            self.PRICE_LOCK_MESSAGE.lower() in message.lower()
            or (status == GAMEFLIP_DEFAULT_LISTING_STATUS and "cannot change 'price'" in message.lower())
        )
