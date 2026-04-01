import re
from typing import Optional


GAMEFLIP_CATEGORIES: dict[str, str] = {
    "GAMES": "CONSOLE_VIDEO_GAMES",
    "INGAME": "DIGITAL_INGAME",
    "GIFTCARD": "GIFTCARD",
    "CONSOLE": "VIDEO_GAME_HARDWARE",
    "ACCESSORIES": "VIDEO_GAME_ACCESSORIES",
    "TOYS": "TOYS_AND_GAMES",
    "VIDEO": "VIDEO_DVD",
    "OTHER": "UNKNOWN",
}

GAMEFLIP_CATEGORY_VALUES = frozenset(GAMEFLIP_CATEGORIES.values())

GAMEFLIP_CATEGORY_ALIASES: dict[str, str] = {
    "game": GAMEFLIP_CATEGORIES["GAMES"],
    "games": GAMEFLIP_CATEGORIES["GAMES"],
    "console game": GAMEFLIP_CATEGORIES["GAMES"],
    "console games": GAMEFLIP_CATEGORIES["GAMES"],
    "video game": GAMEFLIP_CATEGORIES["GAMES"],
    "video games": GAMEFLIP_CATEGORIES["GAMES"],
    "game item": GAMEFLIP_CATEGORIES["INGAME"],
    "game items": GAMEFLIP_CATEGORIES["INGAME"],
    "ingame": GAMEFLIP_CATEGORIES["INGAME"],
    "in game": GAMEFLIP_CATEGORIES["INGAME"],
    "in-game": GAMEFLIP_CATEGORIES["INGAME"],
    "in game item": GAMEFLIP_CATEGORIES["INGAME"],
    "in-game item": GAMEFLIP_CATEGORIES["INGAME"],
    "gift card": GAMEFLIP_CATEGORIES["GIFTCARD"],
    "gift cards": GAMEFLIP_CATEGORIES["GIFTCARD"],
    "giftcard": GAMEFLIP_CATEGORIES["GIFTCARD"],
    "giftcards": GAMEFLIP_CATEGORIES["GIFTCARD"],
    "hardware": GAMEFLIP_CATEGORIES["CONSOLE"],
    "console": GAMEFLIP_CATEGORIES["CONSOLE"],
    "video game hardware": GAMEFLIP_CATEGORIES["CONSOLE"],
    "accessory": GAMEFLIP_CATEGORIES["ACCESSORIES"],
    "accessories": GAMEFLIP_CATEGORIES["ACCESSORIES"],
    "video game accessories": GAMEFLIP_CATEGORIES["ACCESSORIES"],
    "toy": GAMEFLIP_CATEGORIES["TOYS"],
    "toys": GAMEFLIP_CATEGORIES["TOYS"],
    "toys and games": GAMEFLIP_CATEGORIES["TOYS"],
    "collectible": GAMEFLIP_CATEGORIES["TOYS"],
    "collectibles": GAMEFLIP_CATEGORIES["TOYS"],
    "video": GAMEFLIP_CATEGORIES["VIDEO"],
    "videos": GAMEFLIP_CATEGORIES["VIDEO"],
    "dvd": GAMEFLIP_CATEGORIES["VIDEO"],
    "movie": GAMEFLIP_CATEGORIES["VIDEO"],
    "movies": GAMEFLIP_CATEGORIES["VIDEO"],
    "other": GAMEFLIP_CATEGORIES["OTHER"],
    "unknown": GAMEFLIP_CATEGORIES["OTHER"],
}

GAMEFLIP_SHOP_PATH_CATEGORY_ALIASES: dict[str, str] = {
    "game-items": GAMEFLIP_CATEGORIES["INGAME"],
    "gift-cards": GAMEFLIP_CATEGORIES["GIFTCARD"],
    "video-games": GAMEFLIP_CATEGORIES["GAMES"],
    "consoles": GAMEFLIP_CATEGORIES["CONSOLE"],
    "accessories": GAMEFLIP_CATEGORIES["ACCESSORIES"],
    "toys-games": GAMEFLIP_CATEGORIES["TOYS"],
    "movies": GAMEFLIP_CATEGORIES["VIDEO"],
}

GAMEFLIP_GIFTCARD_PRODUCT_SLUG_PLATFORM_ALIASES: dict[str, str] = {
    "apple-gift-card": "apple",
    "google-play-gift-card": "google",
    "amazon-gift-card": "amazon",
    "xbox-gift-card": "xbox_live",
    "xbox-live-gift-card": "xbox_live",
    "playstation-gift-card": "playstation_network",
    "playstation-network-gift-card": "playstation_network",
    "steam-gift-card": "steam",
    "steam-wallet-gift-card": "steam",
}

GAMEFLIP_PLATFORM_ALIASES: dict[str, str] = {
    "apple": "apple",
    "amazon": "amazon",
    "google": "google",
    "google play": "google",
    "playstation": "playstation",
    "playstation network": "playstation_network",
    "psn": "playstation_network",
    "ps": "playstation",
    "ps1": "playstation",
    "ps2": "playstation_2",
    "ps3": "playstation_3",
    "ps4": "playstation_4",
    "ps5": "playstation_5",
    "xbox": "xbox",
    "xbox live": "xbox_live",
    "xbox gift card": "xbox_live",
    "x360": "xbox_360",
    "xbox 360": "xbox_360",
    "xone": "xbox_one",
    "xbox one": "xbox_one",
    "xseries": "xbox_series",
    "xbox series": "xbox_series",
    "switch": "nintendo_switch",
    "wii": "wii",
    "wii u": "wii_u",
    "steam": "steam",
    "roblox": "roblox",
}

GAMEFLIP_ACTIVE_STATUSES = frozenset({"onsale", "ready"})
GAMEFLIP_PAUSED_STATUSES = frozenset({"draft", "sale_pending"})
GAMEFLIP_DEFAULT_LISTING_STATUS = "onsale"
GAMEFLIP_DEFAULT_SEARCH_SORT = "price:asc"


def normalize_alias_key(value: str) -> str:
    collapsed = value.strip().lower().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", collapsed).strip()


def normalize_category(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = normalize_alias_key(value)
    if not normalized:
        return None
    if normalized.upper() in GAMEFLIP_CATEGORY_VALUES:
        return normalized.upper()
    return GAMEFLIP_CATEGORY_ALIASES.get(normalized, value.strip().upper())


def normalize_shop_category_slug(slug: Optional[str]) -> Optional[str]:
    if not slug:
        return None
    return GAMEFLIP_SHOP_PATH_CATEGORY_ALIASES.get(slug.strip().lower())


def normalize_giftcard_product_slug_platform(slug: Optional[str]) -> Optional[str]:
    if not slug:
        return None
    return GAMEFLIP_GIFTCARD_PRODUCT_SLUG_PLATFORM_ALIASES.get(slug.strip().lower())


def normalize_platform(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = normalize_alias_key(value)
    if not normalized:
        return None
    return GAMEFLIP_PLATFORM_ALIASES.get(normalized, normalized.replace(" ", "_"))


def normalize_status(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.strip().lower()
