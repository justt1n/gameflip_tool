import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Type

import httpx
from pydantic import BaseModel
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "MarketplaceAutomation/1.0",
}


def _is_retryable_exception(exc: BaseException) -> bool:
    """Determine if an exception should trigger a retry."""
    if isinstance(exc, (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status >= 500:
            return True
        # Check for queue-limit-exceeded in response body
        try:
            body = exc.response.text
            if "queue" in body.lower() and "limit" in body.lower():
                return True
        except Exception:
            pass
        return False
    return False


class BaseRestAPIClient(ABC):
    """
    Async REST client with:
    - Exponential backoff retry (6 attempts)
    - Selective retry (network errors, 5xx, queue-limit-exceeded)
    - Automatic Pydantic model validation on responses
    - Connection pooling
    """

    def __init__(self, base_url: str, headers: Optional[Dict] = None):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers or DEFAULT_HEADERS,
            timeout=60,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
        )

    @retry(
        wait=wait_exponential(multiplier=5, min=1, max=30),
        stop=stop_after_attempt(6),
        retry=retry_if_exception(_is_retryable_exception)
    )
    async def _request(
        self, method: str, endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> httpx.Response:
        """Core request method with retry logic."""
        response = await self._client.request(
            method, endpoint, params=params, json=json_data, headers=headers
        )
        response.raise_for_status()
        return response

    @abstractmethod
    async def _prepare_payload(self, auth_required: bool, **kwargs) -> Dict:
        """Subclasses add auth headers/params here."""
        ...

    async def get(
        self, endpoint: str, response_model: Type[BaseModel],
        auth_required: bool = False, **kwargs
    ) -> BaseModel:
        """GET request with Pydantic response validation."""
        prepared = await self._prepare_payload(auth_required=auth_required, **kwargs)
        headers = prepared.pop("_headers", None)
        response = await self._request('GET', endpoint, params=prepared, headers=headers)
        return response_model.model_validate(response.json())

    async def post(
        self, endpoint: str, response_model: Type[BaseModel],
        auth_required: bool = False, **kwargs
    ) -> BaseModel:
        """POST request with Pydantic response validation."""
        prepared = await self._prepare_payload(auth_required=auth_required, **kwargs)
        headers = prepared.pop("_headers", None)
        response = await self._request('POST', endpoint, json_data=prepared, headers=headers)
        return response_model.model_validate(response.json())

    async def patch(
        self, endpoint: str, response_model: Type[BaseModel],
        auth_required: bool = False, **kwargs
    ) -> BaseModel:
        """PATCH request with Pydantic response validation."""
        prepared = await self._prepare_payload(auth_required=auth_required, **kwargs)
        headers = prepared.pop("_headers", None)
        response = await self._request('PATCH', endpoint, json_data=prepared, headers=headers)
        return response_model.model_validate(response.json())

    async def close(self):
        """Close the underlying httpx client."""
        await self._client.aclose()

