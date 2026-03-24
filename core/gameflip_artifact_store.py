import json
from pathlib import Path
from typing import Optional

from constants.gameflip_constants import normalize_category, normalize_platform, normalize_status
from models.gameflip_models import GameflipListing
from models.runtime_models import OwnedListingIndexEntry


class GameflipArtifactStore:
    def __init__(self, dump_path: str, index_path: str):
        self.dump_path = Path(dump_path)
        self.index_path = Path(index_path)

    def load_owned_listings_dump(self) -> Optional[list[GameflipListing]]:
        if not self.dump_path.exists():
            return None
        data = json.loads(self.dump_path.read_text(encoding="utf-8"))
        return [GameflipListing.model_validate(item) for item in data]

    def load_owned_listings_index(self) -> Optional[list[OwnedListingIndexEntry]]:
        if not self.index_path.exists():
            return None
        data = json.loads(self.index_path.read_text(encoding="utf-8"))
        return [OwnedListingIndexEntry.model_validate(item) for item in data]

    def save_owned_listings(self, listings: list[GameflipListing]):
        self.dump_path.parent.mkdir(parents=True, exist_ok=True)
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        dump_payload = [self._dump_entry(listing) for listing in listings]
        index_payload = [item.model_dump() for item in self.build_owned_listings_index(listings)]
        self.dump_path.write_text(
            json.dumps(dump_payload, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        self.index_path.write_text(
            json.dumps(index_payload, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )

    def merge_owned_listings(self, listings: list[GameflipListing]) -> list[GameflipListing]:
        existing = self.load_owned_listings_dump() or []
        merged = self._merge_listings(existing, listings)
        self.save_owned_listings(merged)
        return merged

    def build_owned_listings_index(self, listings: list[GameflipListing]) -> list[OwnedListingIndexEntry]:
        return [self._index_entry(listing) for listing in listings]

    @staticmethod
    def _merge_listings(
        existing: list[GameflipListing],
        incoming: list[GameflipListing],
    ) -> list[GameflipListing]:
        merged_by_id: dict[str, GameflipListing] = {
            listing.id: listing for listing in existing
        }
        for listing in incoming:
            merged_by_id[listing.id] = listing
        return list(merged_by_id.values())

    @staticmethod
    def _dump_entry(listing: GameflipListing) -> dict:
        return {
            "id": listing.id,
            "owner": listing.owner,
            "name": listing.name,
            "description": listing.description,
            "category": listing.category,
            "platform": listing.platform,
            "upc": listing.upc,
            "status": listing.status,
            "tags": listing.tags or [],
            "price": listing.price,
        }

    @staticmethod
    def _index_entry(listing: GameflipListing) -> OwnedListingIndexEntry:
        parts = [
            listing.name or "",
            listing.description or "",
            normalize_category(listing.category) or "",
            normalize_platform(listing.platform) or "",
            " ".join(listing.tags or []),
        ]
        return OwnedListingIndexEntry(
            id=listing.id,
            owner=listing.owner,
            name=listing.name,
            category=normalize_category(listing.category),
            platform=normalize_platform(listing.platform),
            upc=listing.upc,
            status=normalize_status(listing.status),
            tags=list(listing.tags or []),
            search_text=" ".join(parts).lower(),
        )
