from decimal import Decimal, ROUND_HALF_UP


USD_CENT = Decimal("0.01")


def cents_to_usd_decimal(cents: int | None) -> float:
    """Convert integer cents from Gameflip into decimal USD."""
    if cents is None:
        return 0.0
    return float((Decimal(cents) / Decimal("100")).quantize(USD_CENT))


def usd_decimal_to_cents(amount: float) -> int:
    """Convert decimal USD into integer cents using half-up rounding."""
    decimal_amount = Decimal(str(amount)).quantize(USD_CENT, rounding=ROUND_HALF_UP)
    return int((decimal_amount * 100).to_integral_value(rounding=ROUND_HALF_UP))
