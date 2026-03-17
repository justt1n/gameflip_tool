from utils.price_utils import cents_to_usd_decimal, usd_decimal_to_cents


class TestPriceUtils:
    def test_cents_to_usd_decimal(self):
        assert cents_to_usd_decimal(1234) == 12.34

    def test_usd_decimal_to_cents_rounds_half_up(self):
        assert usd_decimal_to_cents(12.345) == 1235
        assert usd_decimal_to_cents(12.344) == 1234
