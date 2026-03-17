from abc import ABC, abstractmethod
from typing import Dict


class IAuthHandler(ABC):
    """Abstract base class for platform authentication handlers."""

    @abstractmethod
    async def get_auth_headers(self) -> Dict[str, str]:
        """Return authentication headers, e.g. {"Authorization": "Bearer ..."}"""
        ...

    async def close(self):
        """Optional cleanup. Override to close HTTP clients."""
        pass

