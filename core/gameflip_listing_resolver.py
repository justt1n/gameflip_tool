import re
from typing import Optional
from urllib.parse import parse_qs, urlparse

from pydantic import BaseModel, Field

from clients.gameflip_client import GameflipClient
from constants.gameflip_constants import (
    GAMEFLIP_DEFAULT_LISTING_STATUS,
    normalize_category,
    normalize_giftcard_product_slug_platform,
    normalize_platform,
    normalize_shop_category_slug,
    normalize_status,
)
from core.gameflip_artifact_store import GameflipArtifactStore
from models.gameflip_models import GameflipListing
from models.runtime_models import OwnedListingIndexEntry, ResolvedListingTarget
from models.sheet_models import Payload


class ListingSearchDefinition(BaseModel):
    listing_id: Optional[str] = None
    term: Optional[str] = None
    platform: Optional[str] = None
    category: Optional[str] = None
    digital_region: Optional[str] = None
    status: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    include_keywords: list[str] = Field(default_factory=list)
    exclude_keywords: list[str] = Field(default_factory=list)
    source: str = "fallback"


class GameflipListingResolver:
    LISTING_ID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")

    def __init__(
        self,
        artifact_store: GameflipArtifactStore,
        client: Optional[GameflipClient] = None,
    ):
        self.artifact_store = artifact_store
        self.client = client
        self._owned_index: Optional[list[OwnedListingIndexEntry]] = None

    async def resolve_payload(self, payload: Payload) -> list[ResolvedListingTarget]:
        definition = self.build_search_definition(payload)
        index_entries = self._owned_index
        if index_entries is None:
            index_entries = self.artifact_store.load_owned_listings_index()

        if index_entries is None:
            raise ValueError(
                "Owned listings artifacts not found. Run `python scripts/build_owned_listings_dump.py` first."
            )

        matches = self.match_owned_listings(definition, index_entries)
        if not matches and self.client is not None:
            live_listings = await self._load_live_owned_listings(definition)
            if live_listings:
                merged_listings = self.artifact_store.merge_owned_listings(live_listings)
                index_entries = self.artifact_store.build_owned_listings_index(merged_listings)
                matches = self.match_owned_listings(definition, index_entries)
        if not matches:
            raise ValueError(f"No owned listings matched search definition ({definition.source}) in index")

        self._owned_index = index_entries
        return [
            ResolvedListingTarget(
                payload=payload.model_copy(
                    update={
                        "resolved_listing_id": item.id,
                        "resolved_listing_name": item.name,
                    },
                    deep=True,
                ),
                listing_id=item.id,
                listing_name=item.name,
            )
            for item in matches
        ]

    def build_search_definition(self, payload: Payload) -> ListingSearchDefinition:
        product_compare = (payload.product_compare or "").strip()
        product_id = (payload.product_id or "").strip()
        product_link = (payload.product_link or "").strip()
        include_keywords = self._split_keywords(payload.include_keyword)
        exclude_raw = payload.exclude_keyword or payload.filter_options
        exclude_keywords = self._split_keywords(exclude_raw)

        for value in (product_compare, product_id, product_link):
            if not value:
                continue
            listing_id = self._extract_listing_id(value)
            if listing_id:
                return ListingSearchDefinition(
                    listing_id=listing_id,
                    include_keywords=include_keywords,
                    exclude_keywords=exclude_keywords,
                    source="direct_id",
                )

        parsed_search = self._extract_search_query_payload(
            payload,
            [product_compare, product_id, product_link],
        )
        if parsed_search:
            return ListingSearchDefinition(
                term=parsed_search["term"],
                platform=parsed_search["platform"],
                category=parsed_search["category"],
                digital_region=parsed_search["digital_region"],
                status=parsed_search["status"],
                tags=parsed_search["tags"],
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
                source="search_url",
            )

        fallback_term = payload.product_name or product_link or product_compare or product_id
        fallback_term = (fallback_term or "").strip() or None
        return ListingSearchDefinition(
            term=fallback_term,
            category=normalize_category((payload.category_name or "").strip()),
            status=GAMEFLIP_DEFAULT_LISTING_STATUS,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            source="fallback",
        )

    def match_owned_listings(
        self,
        definition: ListingSearchDefinition,
        listings: list[OwnedListingIndexEntry],
    ) -> list[OwnedListingIndexEntry]:
        candidates = list(listings)

        if definition.listing_id:
            return [listing for listing in candidates if listing.id == definition.listing_id]

        if definition.status:
            candidates = [
                listing for listing in candidates
                if normalize_status(listing.status) == definition.status
            ]
        if definition.platform:
            candidates = [
                listing for listing in candidates
                if normalize_platform(listing.platform) == definition.platform
            ]
        if definition.category:
            candidates = [
                listing for listing in candidates
                if normalize_category(listing.category) == definition.category
            ]
        if definition.digital_region:
            candidates = [
                listing for listing in candidates
                if self._listing_matches_digital_region(listing, definition.digital_region)
            ]
        if definition.tags:
            required_tags = {tag.lower() for tag in definition.tags}
            candidates = [
                listing for listing in candidates
                if required_tags.issubset({tag.lower() for tag in listing.tags or []})
            ]
        if definition.term:
            candidates = [
                listing for listing in candidates
                if self._phrase_matches(listing.search_text, definition.term)
            ]
        if definition.include_keywords:
            candidates = [
                listing for listing in candidates
                if any(self._phrase_matches(listing.name or "", keyword) for keyword in definition.include_keywords)
            ]
        if definition.exclude_keywords:
            candidates = [
                listing for listing in candidates
                if not any(self._phrase_matches(listing.name or "", keyword) for keyword in definition.exclude_keywords)
            ]

        deduped: dict[str, OwnedListingIndexEntry] = {}
        for listing in candidates:
            deduped[listing.id] = listing
        return list(deduped.values())

    @classmethod
    def _extract_listing_id(cls, value: str) -> Optional[str]:
        if cls.LISTING_ID_RE.match(value):
            return value
        parsed = urlparse(value)
        segments = [segment for segment in parsed.path.split("/") if segment]
        for segment in reversed(segments):
            if cls.LISTING_ID_RE.match(segment):
                return segment
        return None

    @staticmethod
    def _first(query: dict[str, list[str]], key: str) -> Optional[str]:
        values = query.get(key) or []
        return values[0] if values else None

    @classmethod
    def _extract_search_query_payload(
        cls,
        payload: Payload,
        candidates: list[str],
    ) -> Optional[dict[str, Optional[str] | list[str]]]:
        for candidate in candidates:
            if not candidate or not candidate.startswith("http"):
                continue
            parsed = urlparse(candidate)
            if "gameflip.com" not in (parsed.netloc or "") or not parsed.path.startswith("/shop/"):
                continue

            query = parse_qs(parsed.query)
            segments = [segment for segment in parsed.path.rstrip("/").split("/") if segment]
            path_slug = segments[-1] if segments else None
            category = normalize_category((payload.category_name or "").strip()) or normalize_shop_category_slug(path_slug)
            platform = normalize_platform(cls._first(query, "platform"))
            if not platform and category == "GIFTCARD":
                platform = normalize_giftcard_product_slug_platform(path_slug)
            term = cls._first(query, "term") or cls._shop_product_term(segments)
            term = cls._effective_sheet_term(payload, category, platform, term)
            return {
                "term": term,
                "platform": platform,
                "category": category,
                "digital_region": cls._first(query, "digital_region"),
                "status": normalize_status(cls._first(query, "status")),
                "tags": cls._split_tags(cls._first(query, "tags")),
            }
        return None

    @staticmethod
    def _shop_product_term(segments: list[str]) -> Optional[str]:
        if len(segments) < 3:
            return None
        slug = segments[-1].strip().lower()
        if not slug:
            return None
        return slug.replace("-", " ")

    @classmethod
    def _effective_sheet_term(
        cls,
        payload: Payload,
        category: Optional[str],
        platform: Optional[str],
        current_term: Optional[str],
    ) -> Optional[str]:
        if category != "GIFTCARD":
            return current_term

        fallback_term = cls._giftcard_name_term(payload)
        if not fallback_term:
            return current_term
        if not current_term:
            return fallback_term
        if cls._term_has_numeric_signal(current_term):
            return current_term
        if cls._phrase_matches(fallback_term, current_term):
            return fallback_term
        if platform and cls._phrase_matches(fallback_term, platform.replace("_", " ")):
            return fallback_term
        return current_term

    @classmethod
    def _giftcard_name_term(cls, payload: Payload) -> Optional[str]:
        for candidate in ((payload.product_link or "").strip(), (payload.product_name or "").strip()):
            if not candidate or candidate.startswith("http"):
                continue
            if cls._term_has_numeric_signal(candidate):
                return candidate
        return None

    @classmethod
    def _term_has_numeric_signal(cls, value: Optional[str]) -> bool:
        tokens = cls._tokenize(value or "")
        return any(token.isdigit() for token in tokens)

    @staticmethod
    def _split_keywords(value: Optional[str]) -> list[str]:
        if not value:
            return []
        normalized = value.replace(";", ",")
        return [item.strip().lower() for item in normalized.split(",") if item.strip()]

    @classmethod
    def _split_tags(cls, value: Optional[str]) -> list[str]:
        if not value:
            return []
        return cls._split_keywords(value.replace("^", ","))

    @classmethod
    def _phrase_matches(cls, text: str, phrase: Optional[str]) -> bool:
        if not phrase:
            return True
        haystack = cls._tokenize(text)
        needle = cls._tokenize(phrase)
        if not needle:
            return False
        if len(needle) > len(haystack):
            return False

        for index in range(len(haystack) - len(needle) + 1):
            window = haystack[index:index + len(needle)]
            if all(cls._tokens_match(query, current) for query, current in zip(needle, window)):
                return True
        return False

    @staticmethod
    def _tokenize(value: str) -> list[str]:
        return re.findall(r"[a-z0-9]+", (value or "").lower())

    @classmethod
    def _listing_matches_digital_region(
        cls,
        listing: OwnedListingIndexEntry,
        digital_region: str,
    ) -> bool:
        regions = [cls._normalize_word(part.strip().lower()) for part in (digital_region or "").split(",") if part.strip()]
        if not regions:
            return True
        listing_tokens = set(cls._tokenize(listing.search_text))
        return any(region in listing_tokens for region in regions)

    @classmethod
    def _tokens_match(cls, query_token: str, text_token: str) -> bool:
        if query_token.isdigit() or text_token.isdigit():
            return query_token == text_token
        return cls._normalize_word(query_token) == cls._normalize_word(text_token)

    async def _load_live_owned_listings(
        self,
        definition: ListingSearchDefinition,
    ) -> list[GameflipListing]:
        if self.client is None:
            return []

        owner_id = await self.client.get_owner_id()
        if definition.listing_id:
            listing = await self.client.listing_get(definition.listing_id)
            if listing.owner == owner_id:
                return [listing]
            return []

        query: dict[str, object] = {
            "owner": owner_id,
            "limit": 100,
        }
        if definition.status:
            query["status"] = definition.status
        if definition.platform:
            query["platform"] = definition.platform
        if definition.category:
            query["category"] = definition.category
        if definition.term:
            query["term"] = definition.term
        if definition.tags:
            query["tags"] = "^".join(definition.tags)

        if hasattr(self.client, "listing_search_all"):
            result = await self.client.listing_search_all(query)
        else:
            result = await self.client.listing_search(query)
        return [listing for listing in result.listings if listing.owner == owner_id]

    @staticmethod
    def _normalize_word(value: str) -> str:
        if value.endswith("ies") and len(value) > 3:
            return value[:-3] + "y"
        if value.endswith("s") and len(value) > 3:
            return value[:-1]
        return value
