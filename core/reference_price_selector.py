from dataclasses import dataclass
from typing import Optional

from models.sheet_models import Payload


@dataclass(frozen=True)
class ReferencePriceCandidate:
    source_name: str
    price: float


class ReferencePriceSelector:
    """
    Pick the best base price from marketplace competition plus optional
    spreadsheet-backed comparison sources such as SS1..SS4.
    """

    def select_best_price(
        self,
        payload: Payload,
        competitor_price: Optional[float],
    ) -> Optional[ReferencePriceCandidate]:
        candidates: list[ReferencePriceCandidate] = []

        if competitor_price is not None:
            candidates.append(ReferencePriceCandidate(source_name="Competition", price=competitor_price))

        for source in payload.ss_reference_sources():
            calculated_price = self._calculate_sheet_source_price(source)
            if calculated_price is None:
                continue
            candidates.append(
                ReferencePriceCandidate(
                    source_name=source["label"],
                    price=calculated_price,
                )
            )

        if not candidates:
            return None

        return min(candidates, key=lambda candidate: candidate.price)

    @staticmethod
    def _calculate_sheet_source_price(source: dict) -> Optional[float]:
        if not source["enabled"]:
            return None

        raw_price = source["fetched_price"]
        if raw_price is None:
            return None

        profit = source["profit"] or 0.0
        profit_multiplier = 1 + (profit / 100.0)
        source_multiplier = source["multiplier"] or 1.0
        unit_factor = source["unit_factor"] or 1.0

        calculated = raw_price * profit_multiplier * source_multiplier * unit_factor
        if calculated <= 0:
            return None
        return calculated
