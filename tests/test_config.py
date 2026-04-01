from utils.config import Settings


class TestSettings:
    def test_is_get_ready_product_defaults_false(self):
        settings = Settings(
            MAIN_SHEET_ID="test_sheet_id",
            MAIN_SHEET_NAME="TestSheet",
            GOOGLE_KEY_PATH="creds.json",
        )

        assert settings.IS_GET_READY_PRODUCT is False

    def test_is_get_ready_product_parses_truthy_values(self):
        settings = Settings(
            MAIN_SHEET_ID="test_sheet_id",
            MAIN_SHEET_NAME="TestSheet",
            GOOGLE_KEY_PATH="creds.json",
            IS_GET_READY_PRODUCT="true",
        )

        assert settings.IS_GET_READY_PRODUCT is True

    def test_is_skip_digital_goods_put_defaults_true(self):
        settings = Settings(
            MAIN_SHEET_ID="test_sheet_id",
            MAIN_SHEET_NAME="TestSheet",
            GOOGLE_KEY_PATH="creds.json",
        )

        assert settings.IS_SKIP_DIGITAL_GOODS_PUT is True

    def test_is_skip_digital_goods_put_parses_falsey_values(self):
        settings = Settings(
            MAIN_SHEET_ID="test_sheet_id",
            MAIN_SHEET_NAME="TestSheet",
            GOOGLE_KEY_PATH="creds.json",
            IS_SKIP_DIGITAL_GOODS_PUT="false",
        )

        assert settings.IS_SKIP_DIGITAL_GOODS_PUT is False

    def test_competitor_fetch_limit_defaults_and_parses(self):
        settings = Settings(
            MAIN_SHEET_ID="test_sheet_id",
            MAIN_SHEET_NAME="TestSheet",
            GOOGLE_KEY_PATH="creds.json",
            GAMEFLIP_COMPETITOR_FETCH_LIMIT="12",
        )

        assert settings.GAMEFLIP_COMPETITOR_FETCH_LIMIT == 12

    def test_seller_name_resolve_limit_defaults_and_parses(self):
        settings = Settings(
            MAIN_SHEET_ID="test_sheet_id",
            MAIN_SHEET_NAME="TestSheet",
            GOOGLE_KEY_PATH="creds.json",
            GAMEFLIP_SELLER_NAME_RESOLVE_LIMIT="3",
        )

        assert settings.GAMEFLIP_SELLER_NAME_RESOLVE_LIMIT == 3
