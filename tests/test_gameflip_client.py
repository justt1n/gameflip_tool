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

    @pytest.mark.asyncio
    async def test_listing_post_wraps_created_listing(self):
        client = GameflipClient(
            base_url="https://production-gameflip.fingershock.com/api/v1",
            auth_handler=DummyAuth(),
            owner_id="owner-self",
        )

        async def fake_request(method, endpoint, params=None, json_data=None, headers=None):
            assert method == "POST"
            assert endpoint == "listing"
            return (
                {
                    "id": "created-1",
                    "name": json_data["name"],
                    "price": json_data["price"],
                    "status": "draft",
                },
                None,
            )

        client._request = fake_request

        listing = await client.listing_post({"name": "New Listing", "price": 1950})

        assert listing.id == "created-1"
        assert listing.name == "New Listing"
        assert listing.price == 1950
        await client.close()

    @pytest.mark.asyncio
    async def test_request_retries_when_gameflip_rate_limits(self, monkeypatch):
        client = GameflipClient(
            base_url="https://production-gameflip.fingershock.com/api/v1",
            auth_handler=DummyAuth(),
            owner_id="owner-self",
        )

        class FakeResponse:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload
                self.text = str(payload)

            def json(self):
                return self._payload

        calls = {"count": 0}

        async def fake_request(method=None, url=None, params=None, json=None, headers=None):
            calls["count"] += 1
            if calls["count"] == 1:
                return FakeResponse(
                    429,
                    {
                        "status": "FAILURE",
                        "data": None,
                        "error": {"message": "Too many attempts - Retry later", "code": 429},
                    },
                )
            return FakeResponse(
                200,
                {
                    "status": "SUCCESS",
                    "data": {"id": "listing-1", "name": "Recovered"},
                },
            )

        async def fake_sleep(_delay):
            return None

        monkeypatch.setattr(client._client, "request", fake_request)
        monkeypatch.setattr("clients.gameflip_client.asyncio.sleep", fake_sleep)

        listing = await client.listing_get("listing-1")

        assert calls["count"] == 2
        assert listing.id == "listing-1"
        await client.close()
