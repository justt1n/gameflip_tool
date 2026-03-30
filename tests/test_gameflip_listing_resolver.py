import pytest

from core.gameflip_artifact_store import GameflipArtifactStore
from core.gameflip_listing_resolver import GameflipListingResolver
from models.gameflip_models import GameflipListing
from tests.conftest import make_payload


def make_owned_listing(**overrides):
    data = {
        "id": "11111111-1111-1111-1111-111111111111",
        "owner": "owner-self",
        "name": "5000 Token | Blade Ball",
        "description": "Best offer",
        "category": "DIGITAL_INGAME",
        "platform": "roblox",
        "price": 1234,
        "upc": "upc-1",
        "status": "onsale",
        "tags": ["roblox_game: Blade Ball"],
    }
    data.update(overrides)
    return GameflipListing.model_validate(data)


class StubResolverClient:
    def __init__(self, listings=None, owner_id="owner-self"):
        self.listings = listings or []
        self.owner_id = owner_id
        self.last_query = None

    async def get_owner_id(self):
        return self.owner_id

    async def listing_search_all(self, query):
        self.last_query = query

        class Result:
            def __init__(self, listings):
                self.listings = listings

        return Result(self.listings)

    async def listing_get(self, listing_id):
        for listing in self.listings:
            if listing.id == listing_id:
                return listing
        raise KeyError(listing_id)


class TestGameflipListingResolver:
    @pytest.mark.asyncio
    async def test_dump_file_contains_normalized_owned_listing_fields(self, tmp_path):
        listings = [make_owned_listing()]
        artifact_store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )

        artifact_store.save_owned_listings(listings)
        index = artifact_store.load_owned_listings_index()

        assert index[0].category == "DIGITAL_INGAME"
        assert index[0].platform == "roblox"
        assert "5000 token" in index[0].search_text

    @pytest.mark.asyncio
    async def test_merge_owned_listings_preserves_existing_ids_not_in_latest_fetch(self, tmp_path):
        existing = make_owned_listing(
            id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            name="Existing Listing",
        )
        updated = make_owned_listing(
            id="11111111-1111-1111-1111-111111111111",
            name="Updated Listing",
        )
        artifact_store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )

        artifact_store.save_owned_listings([existing])
        merged = artifact_store.merge_owned_listings([updated])

        assert {listing.id for listing in merged} == {
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "11111111-1111-1111-1111-111111111111",
        }
        index = artifact_store.load_owned_listings_index()
        assert {entry.id for entry in index} == {
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "11111111-1111-1111-1111-111111111111",
        }

    @pytest.mark.asyncio
    async def test_resolve_payload_uses_local_index(self, tmp_path):
        listings = [make_owned_listing()]
        artifact_store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )
        artifact_store.save_owned_listings(listings)
        resolver = GameflipListingResolver(artifact_store)

        payload = make_payload(product_id="")
        payload.product_compare = (
            "https://gameflip.com/shop/game-items?status=onsale&limit=36"
            "&term=5000%20Token&platform=roblox&tags=roblox_game%3A%20Blade%20Ball"
        )
        payload.include_keyword = "5000 Token"

        matches = await resolver.resolve_payload(payload)

        assert [listing.listing_id for listing in matches] == ["11111111-1111-1111-1111-111111111111"]

    @pytest.mark.asyncio
    async def test_resolve_payload_missing_artifacts_fails_clearly(self, tmp_path):
        resolver = GameflipListingResolver(
            GameflipArtifactStore(
                dump_path=str(tmp_path / "owned_listings.json"),
                index_path=str(tmp_path / "owned_listings_index.json"),
            )
        )

        payload = make_payload(product_id="")
        payload.product_name = "Blade Ball 10000 Token"
        payload.product_link = "Blade Ball 10000 Token"

        with pytest.raises(ValueError) as exc:
            await resolver.resolve_payload(payload)

        assert "build_owned_listings_dump.py" in str(exc.value)

    @pytest.mark.asyncio
    async def test_resolve_payload_applies_exclude_keywords(self, tmp_path):
        listings = [
            make_owned_listing(
                id="33333333-3333-3333-3333-333333333333",
                name="5000 Token | Blade Ball Deluxe",
            ),
            make_owned_listing(
                id="44444444-4444-4444-4444-444444444444",
                name="5000 Token | Blade Ball Basic",
            ),
        ]
        artifact_store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )
        artifact_store.save_owned_listings(listings)
        resolver = GameflipListingResolver(artifact_store)

        payload = make_payload(product_id="")
        payload.product_name = "5000 Token | Blade Ball"
        payload.product_link = "5000 Token | Blade Ball"
        payload.exclude_keyword = "Deluxe"

        matches = await resolver.resolve_payload(payload)

        assert [listing.listing_id for listing in matches] == ["44444444-4444-4444-4444-444444444444"]

    @pytest.mark.asyncio
    async def test_resolve_payload_uses_constants_aliases_for_category(self, tmp_path):
        listings = [make_owned_listing()]
        artifact_store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )
        artifact_store.save_owned_listings(listings)
        resolver = GameflipListingResolver(artifact_store)

        payload = make_payload(product_id="")
        payload.product_compare = (
            "https://gameflip.com/shop/game-items?status=onsale&limit=36"
            "&term=5000%20Token&platform=roblox&tags=roblox_game%3A%20Blade%20Ball"
        )
        payload.category_name = "Game Item"

        matches = await resolver.resolve_payload(payload)

        assert [listing.listing_id for listing in matches] == ["11111111-1111-1111-1111-111111111111"]

    @pytest.mark.asyncio
    async def test_resolve_payload_uses_product_link_search_url_when_compare_is_blank(self, tmp_path):
        listings = [
            make_owned_listing(
                category="GIFTCARD",
                platform="google",
                name="$20.00 Google Play",
                description="$20.00 Google Play card",
                tags=["balance: 2000", "currency: USD", "type: giftcard"],
            )
        ]
        artifact_store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )
        artifact_store.save_owned_listings(listings)
        resolver = GameflipListingResolver(artifact_store)

        payload = make_payload(product_id="")
        payload.product_compare = None
        payload.product_link = (
            "https://gameflip.com/shop/gift-cards?status=onsale&limit=36"
            "&term=Google%20Play&platform=google&tags=type%3A%20giftcard"
        )
        payload.product_id = payload.product_link
        payload.category_name = "Gift Card"

        matches = await resolver.resolve_payload(payload)

        assert [listing.listing_id for listing in matches] == ["11111111-1111-1111-1111-111111111111"]

    @pytest.mark.asyncio
    async def test_resolve_payload_falls_back_to_live_owner_search_when_index_misses(self, tmp_path):
        artifact_store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )
        artifact_store.save_owned_listings([])
        live_listing = make_owned_listing(
            category="GIFTCARD",
            platform="apple",
            name="$50.00 USD Apple",
            description="$50.00 USD Apple",
            tags=["type: giftcard", "balance: 50.00", "currency: USD"],
        )
        client = StubResolverClient([live_listing])
        resolver = GameflipListingResolver(artifact_store, client=client)

        payload = make_payload(product_id="")
        payload.product_compare = None
        payload.product_link = (
            "https://gameflip.com/shop/gift-cards?status=onsale&limit=36"
            "&term=Apple&platform=apple&tags=type%3A%20giftcard"
        )
        payload.product_id = payload.product_link
        payload.category_name = "Gift Card"

        matches = await resolver.resolve_payload(payload)

        assert [listing.listing_id for listing in matches] == [live_listing.id]
        assert client.last_query["owner"] == "owner-self"
        assert client.last_query["category"] == "GIFTCARD"
        assert client.last_query["platform"] == "apple"
        stored = artifact_store.load_owned_listings_index()
        assert [entry.id for entry in stored] == [live_listing.id]

    @pytest.mark.asyncio
    async def test_numeric_term_does_not_match_larger_number_substring(self, tmp_path):
        listings = [
            make_owned_listing(name="5000 Tokens | Blade Ball"),
            make_owned_listing(
                id="22222222-2222-2222-2222-222222222222",
                name="25000 Tokens | Blade Ball",
            ),
        ]
        artifact_store = GameflipArtifactStore(
            dump_path=str(tmp_path / "owned_listings.json"),
            index_path=str(tmp_path / "owned_listings_index.json"),
        )
        artifact_store.save_owned_listings(listings)
        resolver = GameflipListingResolver(artifact_store)

        payload = make_payload(product_id="")
        payload.product_compare = (
            "https://gameflip.com/shop/game-items?status=onsale&limit=36"
            "&term=5000%20Token&platform=roblox&tags=roblox_game%3A%20Blade%20Ball"
        )
        payload.include_keyword = "5000 Token"

        matches = await resolver.resolve_payload(payload)

        assert [listing.listing_name for listing in matches] == ["5000 Tokens | Blade Ball"]
