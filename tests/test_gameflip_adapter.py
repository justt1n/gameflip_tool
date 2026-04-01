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
        self.search_all_kwargs = None
        self.patch_calls = []
        self.patch_failures = []
        self.post_calls = []
        self.post_failures = []
        self.upload_photo_calls = []
        self.profile_display_names = {}
        self.profile_calls = []
        self._post_counter = 0

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

    async def listing_search_all(self, query, max_pages=None, max_listings=None):
        class Result:
            def __init__(self, listings):
                self.listings = listings

        self.last_query = query
        self.search_all_kwargs = {"max_pages": max_pages, "max_listings": max_listings}
        if self.search_pages is None:
            listings = self.search_results
            if max_listings is not None:
                listings = listings[:max_listings]
            return Result(listings)

        merged = []
        for page in self.search_pages:
            merged.extend(page)
            if max_listings is not None and len(merged) >= max_listings:
                merged = merged[:max_listings]
                break
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

    async def listing_post(self, query):
        self.post_calls.append(query)
        if self.post_failures:
            failure = self.post_failures.pop(0)
            if failure:
                raise failure
        self._post_counter += 1
        listing_id = f"created-{self._post_counter}"
        listing = GameflipListing.model_validate({
            "id": listing_id,
            "owner": self.owner_id,
            "status": "draft",
            "version": "1",
            **query,
        })
        self.listings_by_id[listing_id] = listing
        return listing

    async def upload_photo_from_url(self, listing_id, source_url, display_order=None):
        self.upload_photo_calls.append(
            {"listing_id": listing_id, "source_url": source_url, "display_order": display_order}
        )
        return {"photo_id": f"photo-{len(self.upload_photo_calls)}"}

    async def get_owner_id(self):
        return self.owner_id

    async def profile_get(self, owner_id=None):
        owner = owner_id or self.owner_id
        self.profile_calls.append(owner)
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
        payload.product_name = "1B Gems | PET99"
        payload.product_link = "1B Gems | PET99"
        payload.product_id = "1B Gems | PET99"
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
        assert client.last_query["limit"] == 15

    @pytest.mark.asyncio
    async def test_prepare_pricing_input_applies_configured_competitor_fetch_limit(self, tmp_path):
        client = StubGameflipClient()
        mine = make_listing(upc=None, tags=[], name="1B Gems | PET99", platform="roblox")
        client.listings_by_id[mine.id] = mine
        client.search_results = [mine]
        store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings_dump.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )
        store.save_owned_listings([mine])
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
            competitor_fetch_limit=7,
        )
        payload = make_payload()
        payload.product_name = "1B Gems | PET99"
        payload.product_link = "1B Gems | PET99"
        payload.product_id = "1B Gems | PET99"

        target = (await adapter.resolve_payload_targets(payload))[0]
        await adapter.prepare_pricing_input(target)

        assert client.last_query["limit"] == 7

    @pytest.mark.asyncio
    async def test_prepare_pricing_input_uses_top_competitor_page_only(self, tmp_path):
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
        client.search_results = [mine, page_one_rival]
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

        assert [offer.seller_name for offer in prepared.competition.offers] == ["seller-a"]

    @pytest.mark.asyncio
    async def test_prepare_pricing_input_limits_seller_name_resolution_to_top_subset(self, tmp_path):
        client = StubGameflipClient()
        mine = make_listing(upc=None, tags=[], name="1B Gems | PET99", platform="roblox")
        client.listings_by_id[mine.id] = mine
        client.search_results = [mine] + [
            make_listing(
                id=f"00000000-0000-0000-0000-0000000000{i:02d}",
                owner=f"seller-{i}",
                upc=None,
                tags=[],
                name="1B Gems | PET99",
                platform="roblox",
                price=1100 + i,
            )
            for i in range(8)
        ]
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )
        GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings_dump.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        ).save_owned_listings([mine])

        payload = make_payload()
        payload.product_name = "1B Gems | PET99"
        payload.product_link = "1B Gems | PET99"
        payload.product_id = "1B Gems | PET99"
        target = (await adapter.resolve_payload_targets(payload))[0]
        prepared = await adapter.prepare_pricing_input(target)

        assert len(prepared.competition.offers) == 8
        assert len(client.profile_calls) == 5

    @pytest.mark.asyncio
    async def test_prepare_pricing_input_applies_configured_seller_name_limit(self, tmp_path):
        client = StubGameflipClient()
        mine = make_listing(upc=None, tags=[], name="1B Gems | PET99", platform="roblox")
        client.listings_by_id[mine.id] = mine
        client.search_results = [mine] + [
            make_listing(
                id=f"10000000-0000-0000-0000-0000000000{i:02d}",
                owner=f"seller-{i}",
                upc=None,
                tags=[],
                name="1B Gems | PET99",
                platform="roblox",
                price=1100 + i,
            )
            for i in range(6)
        ]
        GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings_dump.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        ).save_owned_listings([mine])
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
            seller_name_resolve_limit=2,
        )
        payload = make_payload()
        payload.product_name = "1B Gems | PET99"
        payload.product_link = "1B Gems | PET99"
        payload.product_id = "1B Gems | PET99"

        target = (await adapter.resolve_payload_targets(payload))[0]
        await adapter.prepare_pricing_input(target)

        assert len(client.profile_calls) == 2

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
    async def test_prepare_pricing_input_giftcard_url_uses_brand_slug_and_region(self, tmp_path):
        client = StubGameflipClient()
        mine = make_listing(
            category="GIFTCARD",
            platform="xbox_live",
            digital=False,
            digital_region="TR",
            upc=None,
            tags=["type: giftcard", "currency: TRY"],
            name="₺100.00 TRY Xbox Gift Card",
        )
        client.listings_by_id[mine.id] = mine
        client.search_results = [mine]
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
        payload.product_name = "₺100.00 TRY Xbox Gift Card"
        payload.product_link = "₺100.00 TRY Xbox Gift Card"
        payload.product_compare = (
            "https://gameflip.com/shop/gift-cards/xbox-gift-card?status=onsale&limit=36"
            "&sort=price%3Aasc&term=&digital_region=TR%2Ctr&tags=currency%3A%20TRY"
        )
        payload.product_id = payload.product_compare
        payload.category_name = "Gift Card"
        payload.game_name = "Xbox Gift Card"

        target = (await adapter.resolve_payload_targets(payload))[0]
        await adapter.prepare_pricing_input(target)

        assert client.last_query["platform"] == "xbox_live"
        assert client.last_query["digital_region"] == "TR,tr"
        assert client.last_query["term"] == "₺100.00 TRY Xbox Gift Card"

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

    @pytest.mark.asyncio
    async def test_duplicate_listing_creates_missing_non_digital_offers(self, tmp_path):
        client = StubGameflipClient()
        source = make_listing(
            digital=False,
            status="onsale",
            version="7",
            price=1234,
            photo={},
        )
        client.listings_by_id[source.id] = source
        client.search_results = [source]
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )
        payload = make_payload()
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 3

        result = await adapter.ensure_duplicate_listing_quota(payload, 19.5)

        assert "Duplicate created: 2" in result.append_note
        assert len(client.post_calls) == 2
        assert all(call["price"] == 1950 for call in client.post_calls)

    @pytest.mark.asyncio
    async def test_duplicate_listing_retries_publish_once_on_version_conflict(self, tmp_path):
        client = StubGameflipClient()
        source = make_listing(
            digital=False,
            status="onsale",
            version="7",
            price=1234,
            photo={},
        )
        client.listings_by_id[source.id] = source
        client.search_results = [source]
        client.patch_failures = [GameflipAPIError("If-Match failed", 412), None]
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )
        payload = make_payload()
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 2

        result = await adapter.ensure_duplicate_listing_quota(payload, 19.5)

        assert "Duplicate created: 1" in result.append_note
        assert len(client.patch_calls) == 2
        assert client.patch_calls[0]["if_match"] == "1"
        assert client.patch_calls[1]["if_match"] == "1"

    @pytest.mark.asyncio
    async def test_duplicate_listing_skips_when_source_is_digital(self, tmp_path):
        client = StubGameflipClient()
        source = make_listing(digital=True, status="onsale", photo={})
        client.listings_by_id[source.id] = source
        client.search_results = [source]
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )
        payload = make_payload()
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 2

        result = await adapter.ensure_duplicate_listing_quota(payload, 19.5)

        assert "digital listing requires digital_goods_put" in result.append_note
        assert client.post_calls == []

    @pytest.mark.asyncio
    async def test_duplicate_listing_can_attempt_digital_source_when_skip_flag_disabled(self, tmp_path):
        client = StubGameflipClient()
        source = make_listing(digital=True, status="onsale", photo={})
        client.listings_by_id[source.id] = source
        client.search_results = [source]
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
            skip_digital_goods_put=False,
        )
        payload = make_payload()
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 2

        result = await adapter.ensure_duplicate_listing_quota(payload, 19.5)

        assert "digital listing requires digital_goods_put" not in (result.append_note or "")
        assert len(client.post_calls) == 1

    @pytest.mark.asyncio
    async def test_duplicate_listing_overrides_note_when_no_active_offer_remains(self, tmp_path):
        client = StubGameflipClient()
        client.search_results = []
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )
        payload = make_payload()
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 2

        result = await adapter.ensure_duplicate_listing_quota(payload, 19.5)

        assert result.override_note == "\n".join([
            "0 OFFER REMAIN",
            "Duplicate skipped",
            "Target: k=2, active=0",
            "Reason: no active source listing",
        ])

    @pytest.mark.asyncio
    async def test_duplicate_listing_ignores_ready_when_flag_is_false(self, tmp_path):
        client = StubGameflipClient()
        ready_listing = make_listing(digital=False, status="ready", photo={})
        client.listings_by_id[ready_listing.id] = ready_listing
        client.search_results = [ready_listing]
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
            include_ready_products=False,
        )
        payload = make_payload()
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 2

        result = await adapter.ensure_duplicate_listing_quota(payload, 19.5)

        assert "0 OFFER REMAIN" in result.override_note
        assert client.last_query["status"] == "onsale"

    @pytest.mark.asyncio
    async def test_duplicate_listing_includes_ready_when_flag_is_true(self, tmp_path):
        client = StubGameflipClient()
        ready_listing = make_listing(digital=False, status="ready", photo={})
        client.listings_by_id[ready_listing.id] = ready_listing
        client.search_results = [ready_listing]
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
            include_ready_products=True,
        )
        payload = make_payload()
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 2

        result = await adapter.ensure_duplicate_listing_quota(payload, 19.5)

        assert "Duplicate created: 1" in result.append_note
        assert client.last_query["status"] in {"ready,onsale", "onsale,ready"}
        assert client.search_all_kwargs["max_listings"] == 2

    @pytest.mark.asyncio
    async def test_duplicate_listing_skips_invalid_target_value(self, tmp_path):
        client = StubGameflipClient()
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )
        payload = make_payload()
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 0

        result = await adapter.ensure_duplicate_listing_quota(payload, 19.5)

        assert "invalid DUPLICATE_LISTING value" in result.append_note

    @pytest.mark.asyncio
    async def test_duplicate_listing_reports_partial_failure(self, tmp_path):
        client = StubGameflipClient()
        source = make_listing(digital=False, status="onsale", photo={})
        client.listings_by_id[source.id] = source
        client.search_results = [source]
        client.post_failures = [None, RuntimeError("boom")]
        adapter = GameflipAdapter(
            client,
            listings_dump_path=str(tmp_path / "owned_listings_dump.json"),
            listings_index_path=str(tmp_path / "owned_listings_index.json"),
        )
        payload = make_payload()
        payload.check_duplicate_listing_str = "1"
        payload.duplicate_listing = 3

        result = await adapter.ensure_duplicate_listing_quota(payload, 19.5)

        assert "Duplicate partial failure" in result.append_note
        assert "created=1" in result.append_note
