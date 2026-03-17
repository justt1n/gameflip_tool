import logging

from clients.gameflip_client import GameflipAPIError, GameflipClient
from utils.price_utils import usd_decimal_to_cents

logger = logging.getLogger(__name__)


class GameflipPriceUpdater:
    def __init__(self, client: GameflipClient):
        self.client = client

    async def update_price(self, offer_id: str, new_price: float) -> bool:
        cents = usd_decimal_to_cents(new_price)
        try:
            current = await self.client.listing_get(offer_id)
            await self.client.listing_patch(
                offer_id,
                [{"op": "replace", "path": "/price", "value": cents}],
                if_match=current.version,
            )
            return True
        except GameflipAPIError as exc:
            if exc.code != 412:
                logger.exception("Gameflip price update failed for %s", offer_id)
                return False

            try:
                refreshed = await self.client.listing_get(offer_id)
                await self.client.listing_patch(
                    offer_id,
                    [{"op": "replace", "path": "/price", "value": cents}],
                    if_match=refreshed.version,
                )
                return True
            except GameflipAPIError:
                logger.exception("Gameflip retry after version conflict failed for %s", offer_id)
                return False
