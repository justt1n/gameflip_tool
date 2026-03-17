import pytest

from clients.gameflip_client import GameflipClient


class DummyAuth:
    async def get_auth_headers(self):
        return {"Authorization": "GFAPI test:123456"}

    async def close(self):
        return None


def make_listing_payload(listing_id: str, name: str):
    return {
        "id": listing_id,
        "owner": "owner-self",
        "name": name,
        "category": "DIGITAL_INGAME",
        "platform": "roblox",
        "price": 1000,
        "status": "onsale",
        "tags": ["roblox_game: Blade Ball"],
    }


class TestGameflipClient:
    @pytest.mark.asyncio
    async def test_list_owned_listings_follows_pagination(self):
        client = GameflipClient(
            base_url="https://production-gameflip.fingershock.com/api/v1",
            auth_handler=DummyAuth(),
            owner_id="owner-self",
        )
        calls = []
        pages = [
            ([make_listing_payload("id-1", "Listing One")], "https://production-gameflip.fingershock.com/api/v1/listing?after=id-1"),
            ([make_listing_payload("id-2", "Listing Two")], None),
        ]

        async def fake_request(method, endpoint, params=None, json_data=None, headers=None):
            calls.append((endpoint, params))
            return pages.pop(0)

        client._request = fake_request

        listings = await client.list_owned_listings(status="onsale", limit=1)

        assert [listing.id for listing in listings] == ["id-1", "id-2"]
        assert calls[0][1]["owner"] == "owner-self"
        assert calls[0][1]["limit"] == 1
        assert calls[1][0].startswith("https://")
        await client.close()
