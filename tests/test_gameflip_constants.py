from constants.gameflip_constants import (
    GAMEFLIP_CATEGORIES,
    GAMEFLIP_DEFAULT_LISTING_STATUS,
    normalize_category,
    normalize_platform,
    normalize_shop_category_slug,
)


def test_official_category_constants_exist():
    assert GAMEFLIP_CATEGORIES["INGAME"] == "DIGITAL_INGAME"
    assert GAMEFLIP_CATEGORIES["GIFTCARD"] == "GIFTCARD"
    assert GAMEFLIP_CATEGORIES["CONSOLE"] == "VIDEO_GAME_HARDWARE"


def test_category_alias_normalization():
    assert normalize_category("Game Item") == "DIGITAL_INGAME"
    assert normalize_category("gift cards") == "GIFTCARD"
    assert normalize_shop_category_slug("game-items") == "DIGITAL_INGAME"


def test_platform_and_status_defaults():
    assert normalize_platform("PS5") == "playstation_5"
    assert normalize_platform("xbox one") == "xbox_one"
    assert GAMEFLIP_DEFAULT_LISTING_STATUS == "onsale"
