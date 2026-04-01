import asyncio
import re
import time
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from clients.gameflip_client import GameflipClient
from constants.gameflip_constants import (
    GAMEFLIP_ACTIVE_STATUSES,
    GAMEFLIP_DEFAULT_SEARCH_SORT,
    GAMEFLIP_PAUSED_STATUSES,
    normalize_category,
    normalize_giftcard_product_slug_platform,
    normalize_platform,
    normalize_shop_category_slug,
    normalize_status,
)
from models.gameflip_models import GameflipListing
from models.runtime_models import (
    PreparedCompetition,
    PreparedCurrentOffer,
    PreparedPricingInput,
    ResolvedListingTarget,
)
from models.standard_models import PlatformIdentifiers, StandardCompetitorOffer
from utils.price_utils import cents_to_usd_decimal


class GameflipPrefetchService:
    def __init__(
        self,
        client: GameflipClient,
        competitor_fetch_limit: int = 15,
        seller_name_resolve_limit: int = 5,
    ):
        self.client = client
        self.competitor_fetch_limit = max(1, competitor_fetch_limit)
        self.seller_name_resolve_limit = max(0, seller_name_resolve_limit)
        self._search_cache: dict[tuple, tuple[float, asyncio.Task]] = {}
        self._search_cache_lock = asyncio.Lock()
        self._search_cache_ttl_seconds = 5.0
        self._seller_name_cache: dict[str, str] = {}
        self._seller_name_lock = asyncio.Lock()

    async def prepare_pricing_input(self, target: ResolvedListingTarget) -> PreparedPricingInput:
        listing = await self.client.listing_get(target.listing_id)
        identifiers = self._build_identifiers(listing)
        current_offer = self._normalize_current_offer(listing, identifiers.product_id)
        competition = PreparedCompetition()

        if target.payload.compare_mode > 0:
            owner_id = listing.owner or await self.client.get_owner_id()
            search_result = await self._listing_search_cached(self._build_search_query(listing, target.payload))
            competitor_listings = self._filter_competitor_listings(
                search_result.listings,
                current_listing=listing,
                owner_id=owner_id,
                min_price=target.payload.fetched_min_price,
                max_price=target.payload.fetched_max_price,
                include_keywords=self._split_keywords(target.payload.include_keyword),
                exclude_keywords=self._split_keywords(
                    target.payload.exclude_keyword or target.payload.filter_options
                ),
                feedback_min=target.payload.feedback_min,
            )
            seller_names = await self._resolve_seller_names(
                competitor_listings[:self.seller_name_resolve_limit]
            )
            normalized_offers = self._normalize_competitors(
                competitor_listings,
                seller_names=seller_names,
                min_price=target.payload.fetched_min_price,
                max_price=target.payload.fetched_max_price,
            )
            competition = PreparedCompetition(
                offers=normalized_offers,
                raw_count=len(search_result.listings),
            )

        prepared_payload = target.payload.model_copy(
            update={
                "offer_id": identifiers.offer_id,
                "real_product_id": identifiers.product_id,
                "current_price": current_offer.price,
            },
            deep=True,
        )
        prepared_target = target.model_copy(update={"payload": prepared_payload}, deep=True)
        return PreparedPricingInput(
            payload=prepared_payload,
            target=prepared_target,
            identifiers=identifiers,
            current_offer=current_offer,
            competition=competition,
        )

    def _build_identifiers(self, listing: GameflipListing) -> PlatformIdentifiers:
        context = self._build_search_context(listing)
        return PlatformIdentifiers(
            offer_id=listing.id,
            product_id=context["product_key"],
            platform="gameflip",
        )

    def _normalize_current_offer(
        self,
        listing: GameflipListing,
        product_id: str,
    ) -> PreparedCurrentOffer:
        return PreparedCurrentOffer(
            offer_id=listing.id,
            product_id=product_id,
            price=cents_to_usd_decimal(listing.price),
            status=self._normalize_status(listing.status),
            raw_status=(listing.status or "").lower() or None,
            offer_type=self._infer_offer_type(listing),
            currency="USD",
            version=listing.version,
        )

    async def _listing_search_cached(self, query: dict[str, Any]):
        key = tuple(sorted((name, self._cacheable_value(value)) for name, value in query.items()))
        now = time.monotonic()

        async with self._search_cache_lock:
            cached = self._search_cache.get(key)
            if cached and now - cached[0] <= self._search_cache_ttl_seconds:
                task = cached[1]
            else:
                task = asyncio.create_task(self.client.listing_search(dict(query)))
                self._search_cache[key] = (now, task)

        try:
            return await task
        except Exception:
            async with self._search_cache_lock:
                cached = self._search_cache.get(key)
                if cached and cached[1] is task:
                    self._search_cache.pop(key, None)
            raise

    def _filter_competitor_listings(
        self,
        listings: list[GameflipListing],
        current_listing: GameflipListing,
        owner_id: str,
        min_price: Optional[float],
        max_price: Optional[float],
        include_keywords: list[str],
        exclude_keywords: list[str],
        feedback_min: Optional[float],
    ) -> list[GameflipListing]:
        filtered: list[GameflipListing] = []
        for listing in listings:
            if listing.id == current_listing.id:
                continue
            if listing.owner == owner_id:
                continue
            if (listing.status or "").lower() != "onsale":
                continue
            if listing.price is None:
                continue
            if include_keywords and not any(
                self._phrase_matches(listing.name or "", keyword) for keyword in include_keywords
            ):
                continue
            if exclude_keywords and any(
                self._phrase_matches(listing.name or "", keyword) for keyword in exclude_keywords
            ):
                continue
            if feedback_min is not None and (listing.seller_ratings or 0) <= feedback_min:
                continue
            filtered.append(listing)

        filtered.sort(key=lambda item: cents_to_usd_decimal(item.price))
        return filtered[:self.competitor_fetch_limit]

    def _normalize_competitors(
        self,
        listings: list[GameflipListing],
        seller_names: dict[str, str],
        min_price: Optional[float],
        max_price: Optional[float],
    ) -> list[StandardCompetitorOffer]:
        offers: list[StandardCompetitorOffer] = []
        for listing in listings:
            price = cents_to_usd_decimal(listing.price)
            is_eligible = True
            note = None
            if min_price is not None and price < min_price:
                is_eligible = False
                note = "Price below min"
            elif max_price is not None and price > max_price:
                is_eligible = False
                note = "Price above max"
            offers.append(
                StandardCompetitorOffer(
                    seller_name=self._seller_name(listing, seller_names),
                    price=price,
                    rating=listing.seller_ratings or 0,
                    is_eligible=is_eligible,
                    note=note,
                )
            )
        return offers

    @staticmethod
    def _build_search_context(listing: GameflipListing) -> dict[str, Any]:
        product_key = "gf:" + "::".join([
            listing.category or "",
            listing.upc or "",
            listing.platform or "",
            "|".join(sorted(listing.tags or [])),
            listing.name or "",
        ])
        return {
            "offer_id": listing.id,
            "owner": listing.owner,
            "category": listing.category,
            "upc": listing.upc,
            "platform": listing.platform,
            "tags": listing.tags or [],
            "name": listing.name,
            "product_key": product_key,
        }

    def _build_search_query(self, listing: GameflipListing, payload) -> dict[str, Any]:
        compare_query = self._parse_compare_query(payload)
        if compare_query:
            return compare_query

        query: dict[str, Any] = {
            "status": "onsale",
            "sort": GAMEFLIP_DEFAULT_SEARCH_SORT,
            "limit": self.competitor_fetch_limit,
        }
        if listing.category:
            query["category"] = listing.category
        if listing.upc:
            query["upc"] = listing.upc
        elif listing.name:
            query["term"] = listing.name
        if listing.platform:
            query["platform"] = listing.platform
        if listing.tags:
            query["tags"] = "^".join(listing.tags)
        elif payload.game_name and (query.get("platform") or "").lower() == "roblox":
            query["tags"] = f"roblox_game: {payload.game_name}"
        return query

    @staticmethod
    def _normalize_status(status: Optional[str]) -> str:
        normalized = (status or "").lower()
        if normalized in GAMEFLIP_ACTIVE_STATUSES:
            return "active"
        if normalized in GAMEFLIP_PAUSED_STATUSES:
            return "paused"
        return "inactive"

    @staticmethod
    def _infer_offer_type(listing: GameflipListing) -> str:
        if listing.digital:
            if (listing.category or "").upper() == "DIGITAL_INGAME":
                return "dropshipping"
            return "key"
        if (listing.category or "").upper() == "ACCOUNT":
            return "account"
        return "gift"

    async def _resolve_seller_names(self, listings: list[GameflipListing]) -> dict[str, str]:
        owner_ids = {
            listing.owner
            for listing in listings
            if listing.owner
        }
        missing_owner_ids: list[str] = []

        async with self._seller_name_lock:
            for owner_id in owner_ids:
                if owner_id not in self._seller_name_cache:
                    missing_owner_ids.append(owner_id)

        if missing_owner_ids:
            resolved = await asyncio.gather(
                *(self._fetch_seller_name(owner_id) for owner_id in missing_owner_ids),
                return_exceptions=True,
            )
            async with self._seller_name_lock:
                for owner_id, value in zip(missing_owner_ids, resolved):
                    if isinstance(value, Exception):
                        self._seller_name_cache[owner_id] = self._fallback_owner_name(owner_id)
                    else:
                        self._seller_name_cache[owner_id] = value

        async with self._seller_name_lock:
            return {
                owner_id: self._seller_name_cache[owner_id]
                for owner_id in owner_ids
                if owner_id in self._seller_name_cache
            }

    async def _fetch_seller_name(self, owner_id: str) -> str:
        profile = await self.client.profile_get(owner_id)
        display_name = (profile.display_name or "").strip()
        if display_name:
            return display_name
        return self._fallback_owner_name(owner_id)

    @staticmethod
    def _fallback_owner_name(owner_id: str) -> str:
        if ":" in owner_id:
            return owner_id.split(":", 1)[1]
        return owner_id

    @classmethod
    def _seller_name(cls, listing: GameflipListing, seller_names: dict[str, str]) -> str:
        if listing.owner and listing.owner in seller_names:
            return seller_names[listing.owner]
        if listing.owner:
            return cls._fallback_owner_name(listing.owner)
        return f"gameflip:{listing.id}"

    @staticmethod
    def _cacheable_value(value: Any):
        if isinstance(value, list):
            return tuple(value)
        return value

    def _parse_compare_query(self, payload) -> dict[str, Any]:
        compare_source = (payload.product_compare or "").strip()
        if not compare_source and getattr(payload, "sheet_schema", "") == "requirement":
            candidates = [
                (payload.product_link or "").strip(),
                (payload.product_id or "").strip(),
            ]
            compare_source = next((candidate for candidate in candidates if candidate), "")
        if not compare_source:
            return {}

        if compare_source.startswith("http"):
            parsed = urlparse(compare_source)
            if "gameflip.com" not in (parsed.netloc or ""):
                return {}

            query = parse_qs(parsed.query)
            segments = [segment for segment in parsed.path.rstrip("/").split("/") if segment]
            path_slug = segments[-1] if segments else None
            result: dict[str, Any] = {
                "status": normalize_status(GameflipPrefetchService._first(query, "status")) or "onsale",
                "sort": GameflipPrefetchService._first(query, "sort") or GAMEFLIP_DEFAULT_SEARCH_SORT,
                "limit": min(
                    int(GameflipPrefetchService._first(query, "limit") or 100),
                    self.competitor_fetch_limit,
                ),
            }

            term = GameflipPrefetchService._first(query, "term")
            category = normalize_category((payload.category_name or "").strip()) or normalize_shop_category_slug(path_slug)
            platform = normalize_platform(GameflipPrefetchService._first(query, "platform"))
            if not platform and category == "GIFTCARD":
                platform = normalize_giftcard_product_slug_platform(path_slug)
            if not term and len(segments) >= 3:
                term = path_slug.replace("-", " ")
            term = GameflipPrefetchService._effective_sheet_term(payload, category, platform, term)
            tags = GameflipPrefetchService._first(query, "tags")
            digital_region = GameflipPrefetchService._first(query, "digital_region")
            if not tags and payload.game_name and (platform or "").lower() == "roblox":
                tags = f"roblox_game: {payload.game_name}"

            if term:
                result["term"] = term
            if platform:
                result["platform"] = platform
            if category:
                result["category"] = category
            if digital_region:
                result["digital_region"] = digital_region
            if tags:
                result["tags"] = tags
            return result

        return {
            "status": "onsale",
            "sort": GAMEFLIP_DEFAULT_SEARCH_SORT,
            "limit": self.competitor_fetch_limit,
            "term": compare_source,
            **(
                {"tags": f"roblox_game: {payload.game_name}"}
                if payload.game_name else {}
            ),
        }

    @staticmethod
    def _first(query: dict[str, list[str]], key: str) -> Optional[str]:
        values = query.get(key) or []
        return values[0] if values else None

    @staticmethod
    def _split_keywords(value: Optional[str]) -> list[str]:
        if not value:
            return []
        normalized = value.replace(";", ",")
        return [item.strip().lower() for item in normalized.split(",") if item.strip()]

    @classmethod
    def _effective_sheet_term(
        cls,
        payload,
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

    @staticmethod
    def _giftcard_name_term(payload) -> Optional[str]:
        for candidate in ((payload.product_link or "").strip(), (payload.product_name or "").strip()):
            if not candidate or candidate.startswith("http"):
                continue
            if GameflipPrefetchService._term_has_numeric_signal(candidate):
                return candidate
        return None

    @staticmethod
    def _term_has_numeric_signal(value: Optional[str]) -> bool:
        return any(part.isdigit() for part in re.findall(r"[a-z0-9]+", (value or "").lower()))

    @classmethod
    def _phrase_matches(cls, text: str, phrase: Optional[str]) -> bool:
        if not phrase:
            return True
        haystack = re.findall(r"[a-z0-9]+", (text or "").lower())
        needle = re.findall(r"[a-z0-9]+", (phrase or "").lower())
        if not needle or len(needle) > len(haystack):
            return False
        for index in range(len(haystack) - len(needle) + 1):
            if haystack[index:index + len(needle)] == needle:
                return True
        return False

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
    def _tokens_match(cls, query_token: str, text_token: str) -> bool:
        if query_token.isdigit() or text_token.isdigit():
            return query_token == text_token
        return cls._normalize_word(query_token) == cls._normalize_word(text_token)

    @staticmethod
    def _normalize_word(value: str) -> str:
        if value.endswith("ies") and len(value) > 3:
            return value[:-3] + "y"
        if value.endswith("s") and len(value) > 3:
            return value[:-1]
        return value
