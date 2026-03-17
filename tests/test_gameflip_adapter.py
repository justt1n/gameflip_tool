import pytest

from adapters.gameflip_adapter import GameflipAdapter
from clients.gameflip_client import GameflipAPIError
from core.gameflip_artifact_store import GameflipArtifactStore
from models.gameflip_models import GameflipListing
from models.sheet_models import Payload


class StubGameflipClient:
    def __init__(self):
        self.owner_id = "owner-self"
        self.listings_by_id = {}
        self.search_results = []
        self.patch_calls = []
        self.patch_failures = []

    async def listing_get(self, listing_id: str):
        value = self.listings_by_id[listing_id]
        if isinstance(value, Exception):
            raise value
        return value

    async def listing_search(self, query):
        class Result:
            def __init__(self, listings):
                self.listings = listings

        self.last_query = query
        return Result(self.search_results)

    async def listing_patch(self, listing_id, operations, if_match=None):
        self.patch_calls.append(
            {"listing_id": listing_id, "operations": operations, "if_match": if_match}
        )
        if self.patch_failures:
            failure = self.patch_failures.pop(0)
            if failure:
                raise failure
        listing = self.listings_by_id[listing_id]
        return listing.model_copy(update={"price": operations[0]["value"]})

    async def get_owner_id(self):
        return self.owner_id

    async def close(self):
        return None


def make_listing(**overrides):
    data = {
        "id": "11111111-1111-1111-1111-111111111111",
        "owner": "owner-self",
        "name": "AK-47 | Redline",
        "description": "Great skin",
        "category": "DIGITAL_INGAME",
        "platform": "steam",
        "price": 1234,
        "upc": "094922417596",
        "status": "onsale",
        "version": "7",
        "digital": True,
        "tags": ["Type:Rifle", "Weapon:AK-47"],
    }
    data.update(overrides)
    return GameflipListing.model_validate(data)


def make_payload():
    row = [""] * 28
    row[1] = "1"
    row[2] = "AK-47 | Redline"
    row[6] = "https://gameflip.com/shop/game-items?term=AK-47&platform=steam"
    row[7] = "1"
    row[27] = "10.0"
    payload = Payload.from_row(row, row_index=4)
    payload.fetched_min_price = 10.0
    payload.fetched_max_price = 20.0
    return payload


class TestGameflipAdapter:
    @pytest.mark.asyncio
    async def test_resolve_payload_targets_uses_local_index(self, tmp_path):
        client = StubGameflipClient()
        listing = make_listing()
        store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings_dump.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )
        store.save_owned_listings([listing])
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )

        targets = await adapter.resolve_payload_targets(make_payload())

        assert [item.listing_id for item in targets] == [listing.id]
        assert targets[0].listing_name == listing.name

    @pytest.mark.asyncio
    async def test_prepare_pricing_input_normalizes_current_offer_and_competitors(self, tmp_path):
        client = StubGameflipClient()
        mine = make_listing()
        rival = make_listing(
            id="22222222-2222-2222-2222-222222222222",
            owner="seller-a",
            price=1200,
        )
        client.listings_by_id[mine.id] = mine
        client.search_results = [mine, rival]
        store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings_dump.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )
        store.save_owned_listings([mine])
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )

        target = (await adapter.resolve_payload_targets(make_payload()))[0]
        prepared = await adapter.prepare_pricing_input(target)

        assert prepared.current_offer.price == 12.34
        assert prepared.current_offer.status == "active"
        assert prepared.competition.offers[0].seller_name == "seller-a"
        assert client.last_query["upc"] == mine.upc

    @pytest.mark.asyncio
    async def test_update_price_sends_patch_in_cents(self, tmp_path):
        client = StubGameflipClient()
        listing = make_listing(version="4")
        client.listings_by_id[listing.id] = listing
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )

        ok = await adapter.update_price(listing.id, 12.35)

        assert ok is True
        assert client.patch_calls[0]["operations"] == [
            {"op": "replace", "path": "/price", "value": 1235}
        ]
        assert client.patch_calls[0]["if_match"] == "4"

    @pytest.mark.asyncio
    async def test_update_price_retries_once_on_version_conflict(self, tmp_path):
        client = StubGameflipClient()
        listing = make_listing(version="4")
        refreshed = make_listing(version="5")
        client.listings_by_id[listing.id] = refreshed
        client.patch_failures = [GameflipAPIError("If-Match failed", 412), None]

        async def listing_get_with_refresh(listing_id):
            count = getattr(client, "_get_count", 0) + 1
            client._get_count = count
            return listing if count == 1 else refreshed

        client.listing_get = listing_get_with_refresh
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )

        ok = await adapter.update_price(listing.id, 10.0)

        assert ok is True
        assert len(client.patch_calls) == 2
        assert client.patch_calls[0]["if_match"] == "4"
        assert client.patch_calls[1]["if_match"] == "5"
