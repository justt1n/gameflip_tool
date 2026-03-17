import math


def round_up_to_n_decimals(number: float, n: int) -> float:
    """
    Round UP (ceiling) to n decimal places.

    Examples:
      round_up_to_n_decimals(12.341, 2) → 12.35
      round_up_to_n_decimals(12.300, 2) → 12.30
      round_up_to_n_decimals(12.341, 0) → 13.0

    Raises:
        ValueError: If n is negative.
    """
    if n < 0:
        raise ValueError("Decimal places cannot be negative")
    multiplier = 10 ** n
    return math.ceil(number * multiplier) / multiplier

