import pytest

from adapters.gameflip_adapter import GameflipAdapter
from clients.gameflip_client import GameflipAPIError
from core.gameflip_artifact_store import GameflipArtifactStore
from models.gameflip_models import GameflipListing, GameflipProfile
from models.sheet_models import Payload


class StubGameflipClient:
    def __init__(self):
        self.owner_id = "owner-self"
        self.listings_by_id = {}
        self.search_results = []
        self.search_pages = None
        self.patch_calls = []
        self.patch_failures = []
        self.profile_display_names = {}

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

    async def listing_search_all(self, query):
        class Result:
            def __init__(self, listings):
                self.listings = listings

        self.last_query = query
        if self.search_pages is None:
            return Result(self.search_results)

        merged = []
        for page in self.search_pages:
            merged.extend(page)
        return Result(merged)

    async def listing_patch(self, listing_id, operations, if_match=None):
        self.patch_calls.append(
            {"listing_id": listing_id, "operations": operations, "if_match": if_match}
        )
        if self.patch_failures:
            failure = self.patch_failures.pop(0)
            if failure:
                raise failure
        listing = self.listings_by_id[listing_id]
        updates = {}
        for operation in operations:
            key = operation["path"].lstrip("/")
            updates[key] = operation["value"]
        if "version" in listing.model_fields:
            next_version = str(int(getattr(listing, "version", "0") or "0") + 1)
            updates["version"] = next_version
        updated = listing.model_copy(update=updates)
        self.listings_by_id[listing_id] = updated
        return updated

    async def get_owner_id(self):
        return self.owner_id

    async def profile_get(self, owner_id=None):
        owner = owner_id or self.owner_id
        return GameflipProfile(
            owner=owner,
            display_name=self.profile_display_names.get(owner),
        )

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
        assert prepared.current_offer.raw_status == "onsale"
        assert prepared.current_offer.version == "7"
        assert prepared.competition.offers[0].seller_name == "seller-a"
        assert client.last_query["upc"] == mine.upc

    @pytest.mark.asyncio
    async def test_prepare_pricing_input_uses_profile_display_name_for_seller_log(self, tmp_path):
        client = StubGameflipClient()
        mine = make_listing()
        rival = make_listing(
            id="22222222-2222-2222-2222-222222222222",
            owner="us-east-1:2b359637-0d2c-4345-9e73-20940a63364a",
            price=1200,
        )
        client.profile_display_names[rival.owner] = "Dyuke Store"
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

        assert prepared.competition.offers[0].seller_name == "Dyuke Store"

    @pytest.mark.asyncio
    async def test_prepare_pricing_input_uses_product_compare_query_and_keyword_filters(self, tmp_path):
        client = StubGameflipClient()
        mine = make_listing(
            name="5000 Token | Blade Ball",
            platform="roblox",
            category="DIGITAL_INGAME",
            upc=None,
            tags=[],
        )
        client.listings_by_id[mine.id] = mine
        client.search_results = [
            mine,
            make_listing(
                id="22222222-2222-2222-2222-222222222222",
                owner="seller-a",
                name="5000 Token | Blade Ball",
                platform="roblox",
                category="DIGITAL_INGAME",
                upc=None,
                tags=[],
                price=1200,
            ),
            make_listing(
                id="33333333-3333-3333-3333-333333333333",
                owner="seller-b",
                name="5000 Token | Blade Ball Deluxe",
                platform="roblox",
                category="DIGITAL_INGAME",
                upc=None,
                tags=[],
                price=1190,
            ),
            make_listing(
                id="44444444-4444-4444-4444-444444444444",
                owner="seller-c",
                name="25000 Token | Blade Ball",
                platform="roblox",
                category="DIGITAL_INGAME",
                upc=None,
                tags=[],
                price=1180,
            ),
        ]
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
        payload = make_payload()
        payload.product_compare = (
            "https://gameflip.com/shop/game-items?status=onsale&limit=36"
            "&term=5000%20Token&platform=roblox"
        )
        payload.include_keyword = "5000 Token"
        payload.exclude_keyword = "Deluxe"

        target = (await adapter.resolve_payload_targets(payload))[0]
        prepared = await adapter.prepare_pricing_input(target)

        assert [offer.seller_name for offer in prepared.competition.offers] == ["seller-a"]
        assert client.last_query["term"] == "5000 Token"
        assert client.last_query["platform"] == "roblox"
        assert client.last_query["status"] == "onsale"
        assert client.last_query["limit"] == 36

    @pytest.mark.asyncio
    async def test_prepare_pricing_input_merges_paginated_competitor_results(self, tmp_path):
        client = StubGameflipClient()
        mine = make_listing(upc=None, tags=[], name="1B Gems | PET99", platform="roblox")
        page_one_rival = make_listing(
            id="22222222-2222-2222-2222-222222222222",
            owner="seller-a",
            upc=None,
            tags=[],
            name="1B Gems | PET99",
            platform="roblox",
            price=1200,
        )
        page_two_rival = make_listing(
            id="33333333-3333-3333-3333-333333333333",
            owner="seller-b",
            upc=None,
            tags=[],
            name="1B Gems | PET99",
            platform="roblox",
            price=1190,
        )
        client.listings_by_id[mine.id] = mine
        client.search_pages = [[mine, page_one_rival], [page_two_rival]]
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

        payload = make_payload()
        payload.product_name = "1B Gems | PET99"
        payload.product_link = "1B Gems | PET99"
        payload.product_id = "1B Gems | PET99"
        target = (await adapter.resolve_payload_targets(payload))[0]
        prepared = await adapter.prepare_pricing_input(target)

        assert [offer.seller_name for offer in prepared.competition.offers] == ["seller-b", "seller-a"]

    @pytest.mark.asyncio
    async def test_prepare_pricing_input_filters_competitors_by_feedback_min(self, tmp_path):
        client = StubGameflipClient()
        mine = make_listing(upc=None, tags=[], name="1B Gems | PET99", platform="roblox")
        client.listings_by_id[mine.id] = mine
        client.search_results = [
            mine,
            make_listing(
                id="22222222-2222-2222-2222-222222222222",
                owner="seller-a",
                upc=None,
                tags=[],
                name="1B Gems | PET99",
                platform="roblox",
                price=1200,
                seller_ratings=50,
            ),
            make_listing(
                id="33333333-3333-3333-3333-333333333333",
                owner="seller-b",
                upc=None,
                tags=[],
                name="1B Gems | PET99",
                platform="roblox",
                price=1190,
                seller_ratings=250,
            ),
        ]
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
        payload = make_payload()
        payload.product_name = "1B Gems | PET99"
        payload.product_link = "1B Gems | PET99"
        payload.product_id = "1B Gems | PET99"
        payload.feedback_min = 100

        target = (await adapter.resolve_payload_targets(payload))[0]
        prepared = await adapter.prepare_pricing_input(target)

        assert [offer.seller_name for offer in prepared.competition.offers] == ["seller-b"]

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
    async def test_update_price_reuses_prefetched_version_and_status(self, tmp_path):
        client = StubGameflipClient()
        listing = make_listing(version="4", status="onsale")
        client.listings_by_id[listing.id] = listing

        async def unexpected_listing_get(listing_id):
            raise AssertionError("listing_get should not be called when version is supplied")

        client.listing_get = unexpected_listing_get
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )

        ok = await adapter.update_price(
            listing.id,
            12.35,
            current_version="4",
            current_status="onsale",
        )

        assert ok is True
        assert len(client.patch_calls) == 3
        assert client.patch_calls[0]["operations"] == [
            {"op": "replace", "path": "/status", "value": "draft"}
        ]
        assert client.patch_calls[1]["operations"] == [
            {"op": "replace", "path": "/price", "value": 1235}
        ]
        assert client.patch_calls[2]["operations"] == [
            {"op": "replace", "path": "/status", "value": "onsale"}
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

    @pytest.mark.asyncio
    async def test_update_price_fallback_pause_resume_when_onsale_locked(self, tmp_path):
        client = StubGameflipClient()
        listing = make_listing(version="4", status="onsale")
        client.listings_by_id[listing.id] = listing
        client.patch_failures = [
            GameflipAPIError("Cannot change 'price' when status is onsale", 400),
            None,
            None,
            None,
        ]
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )

        ok = await adapter.update_price(listing.id, 10.0)

        assert ok is True
        assert len(client.patch_calls) == 4
        assert client.patch_calls[0]["operations"] == [
            {"op": "replace", "path": "/price", "value": 1000}
        ]
        assert client.patch_calls[1]["operations"] == [
            {"op": "replace", "path": "/status", "value": "draft"}
        ]
        assert client.patch_calls[2]["operations"] == [
            {"op": "replace", "path": "/price", "value": 1000}
        ]
        assert client.patch_calls[3]["operations"] == [
            {"op": "replace", "path": "/status", "value": "onsale"}
        ]

    @pytest.mark.asyncio
    async def test_update_price_skips_initial_price_patch_when_prefetched_status_is_onsale(self, tmp_path):
        client = StubGameflipClient()
        listing = make_listing(version="4", status="onsale")
        client.listings_by_id[listing.id] = listing

        async def unexpected_listing_get(listing_id):
            raise AssertionError("listing_get should not be called when version/status are supplied")

        client.listing_get = unexpected_listing_get
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )

        ok = await adapter.update_price(
            listing.id,
            10.0,
            current_version="4",
            current_status="onsale",
        )

        assert ok is True
        assert [call["operations"] for call in client.patch_calls] == [
            [{"op": "replace", "path": "/status", "value": "draft"}],
            [{"op": "replace", "path": "/price", "value": 1000}],
            [{"op": "replace", "path": "/status", "value": "onsale"}],
        ]
