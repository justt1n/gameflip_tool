import pytest
from utils.math_utils import round_up_to_n_decimals


class TestRoundUpToNDecimals:
    def test_exact_value(self):
        assert round_up_to_n_decimals(12.50, 2) == 12.50

    def test_round_up_3rd_decimal(self):
        assert round_up_to_n_decimals(12.341, 2) == 12.35

    def test_round_up_already_round(self):
        assert round_up_to_n_decimals(12.30, 2) == 12.30

    def test_round_to_integer(self):
        assert round_up_to_n_decimals(12.341, 0) == 13.0

    def test_round_to_1_decimal(self):
        assert round_up_to_n_decimals(12.34, 1) == 12.4

    def test_round_to_3_decimals(self):
        assert round_up_to_n_decimals(12.3411, 3) == 12.342

    def test_tiny_value(self):
        assert round_up_to_n_decimals(0.001, 2) == 0.01

    def test_large_value(self):
        assert round_up_to_n_decimals(999.991, 2) == 999.10 or round_up_to_n_decimals(999.991, 2) == 1000.00
        # ceil(999.991 * 100) / 100 = ceil(99999.1) / 100 = 100000 / 100 = 1000.00
        assert round_up_to_n_decimals(999.991, 2) == 1000.00

    def test_negative_decimals_raises(self):
        with pytest.raises(ValueError):
            round_up_to_n_decimals(12.50, -1)

    def test_zero_value(self):
        assert round_up_to_n_decimals(0.0, 2) == 0.0

    def test_integer_input(self):
        assert round_up_to_n_decimals(5, 2) == 5.0

    def test_very_small_fraction(self):
        # 0.001 with 3 decimal places → exactly 0.001
        assert round_up_to_n_decimals(0.001, 3) == 0.001

    def test_round_up_not_standard_round(self):
        """Ensure it's ceiling (up), not standard rounding (nearest)."""
        # Standard round(12.344, 2) = 12.34, but ceil should give 12.35
        assert round_up_to_n_decimals(12.344, 2) == 12.35
        # Standard round(12.341, 2) = 12.34, but ceil should give 12.35
        assert round_up_to_n_decimals(12.341, 2) == 12.35

