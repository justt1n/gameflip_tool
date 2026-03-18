from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import httpx

from auth.base_auth import IAuthHandler
from models.gameflip_models import (
    GameflipApiError,
    GameflipListing,
    GameflipProfile,
    GameflipSearchResult,
    GameflipWallet,
)


class GameflipAPIError(RuntimeError):
    """Raised when the Gameflip API responds with a failure payload."""

    def __init__(self, message: str, code: int | None = None):
        super().__init__(message)
        self.code = code


class GameflipClient:
    """Minimal async Gameflip client for repricing flows."""

    def __init__(
        self,
        base_url: str,
        auth_handler: IAuthHandler,
        owner_id: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        parsed = urlparse(self.base_url)
        self.origin = f"{parsed.scheme}://{parsed.netloc}/"
        self.auth_handler = auth_handler
        self._owner_id = owner_id
        self._client = httpx.AsyncClient(
            base_url=self.base_url + "/",
            timeout=60,
            headers={
                "Accept": "application/json",
                "User-Agent": "GameflipAutomation/1.0",
            },
        )

    async def profile_get(self, owner_id: Optional[str] = None) -> GameflipProfile:
        suffix = owner_id or "me"
        data, _ = await self._request("GET", f"account/{suffix}/profile")
        profile = GameflipProfile.model_validate(data)
        if owner_id is None:
            self._owner_id = profile.owner
        return profile

    async def get_owner_id(self) -> str:
        if self._owner_id:
            return self._owner_id
        profile = await self.profile_get()
        return profile.owner

    async def wallet_get(
        self, owner_id: Optional[str] = None, balance_only: bool = True
    ) -> GameflipWallet:
        suffix = owner_id or "me"
        params = {"balance_only": True} if balance_only else None
        data, _ = await self._request("GET", f"account/{suffix}/wallet_history", params=params)
        return GameflipWallet.model_validate(data)

    async def listing_get(self, listing_id: str) -> GameflipListing:
        data, _ = await self._request("GET", f"listing/{listing_id}")
        return GameflipListing.model_validate(data)

    async def listing_search(self, query: dict[str, Any]) -> GameflipSearchResult:
        params = dict(query)
        params["v2"] = True
        data, next_page = await self._request("GET", "listing", params=params)
        listings = data if isinstance(data, list) else data.get("listings", [])
        return GameflipSearchResult(
            listings=[GameflipListing.model_validate(item) for item in listings],
            next_page=next_page,
            raw=data,
        )

    async def listing_search_all(
        self,
        query: dict[str, Any],
        max_pages: Optional[int] = None,
    ) -> GameflipSearchResult:
        params = dict(query)
        params["v2"] = True

        listings: list[GameflipListing] = []
        next_page: Optional[str] = None
        page_count = 0
        raw_pages: list[Any] = []

        while True:
            data, next_page = await self._request(
                "GET",
                next_page or "listing",
                params=None if next_page else params,
            )
            raw_pages.append(data)
            page_items = data if isinstance(data, list) else data.get("listings", [])
            listings.extend(GameflipListing.model_validate(item) for item in page_items)
            page_count += 1
            if not next_page:
                break
            if max_pages is not None and page_count >= max_pages:
                break

        return GameflipSearchResult(
            listings=listings,
            next_page=next_page,
            raw=raw_pages,
        )

    async def list_owned_listings(
        self,
        owner_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[GameflipListing]:
        owner = owner_id or await self.get_owner_id()
        query: dict[str, Any] = {
            "owner": owner,
            "limit": limit,
            "v2": True,
        }
        if status:
            query["status"] = status

        listings: list[GameflipListing] = []
        next_page: Optional[str] = None
        while True:
            data, next_page = await self._request(
                "GET",
                next_page or "listing",
                params=None if next_page else query,
            )
            page_items = data if isinstance(data, list) else data.get("listings", [])
            listings.extend(GameflipListing.model_validate(item) for item in page_items)
            if not next_page:
                break
        return listings

    async def listing_patch(
        self,
        listing_id: str,
        operations: list[dict[str, Any]],
        if_match: Optional[str | int] = None,
    ) -> GameflipListing:
        headers = {"Content-Type": "application/json-patch+json"}
        if if_match is not None:
            headers["If-Match"] = str(if_match)
        data, _ = await self._request(
            "PATCH",
            f"listing/{listing_id}",
            json_data=operations,
            headers=headers,
        )
        return GameflipListing.model_validate(data)

    async def close(self):
        await self._client.aclose()
        await self.auth_handler.close()

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        json_data: Optional[Any] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> tuple[Any, Optional[str]]:
        auth_headers = await self.auth_handler.get_auth_headers()
        response = await self._client.request(
            method=method,
            url=self._build_url(endpoint),
            params=params,
            json=json_data,
            headers={**auth_headers, **(headers or {})},
        )

        try:
            payload = response.json()
        except ValueError as exc:
            response.raise_for_status()
            raise RuntimeError(f"Non-JSON response from Gameflip: {response.text}") from exc

        if response.status_code >= 400 or payload.get("status") != "SUCCESS":
            error = GameflipApiError.model_validate(
                payload.get("error") or {"message": response.text, "code": response.status_code}
            )
            raise GameflipAPIError(error.message, error.code)

        return payload.get("data"), payload.get("next_page")

    def _build_url(self, endpoint: str) -> str:
        if endpoint.startswith(("http://", "https://")):
            return endpoint
        if endpoint.startswith("/api/"):
            return urljoin(self.origin, endpoint.lstrip("/"))
        return urljoin(self.base_url + "/", endpoint.lstrip("/"))
