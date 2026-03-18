import logging
import random
from typing import Optional

from core.competition_analyzer import CompetitionAnalyzer
from core.log_formatter import LogFormatter
from core.reference_price_selector import ReferencePriceSelector
from models.processing_models import CompareTarget, AnalysisResult
from models.runtime_models import PreparedPricingInput, PreparedPricingResult, PriceUpdateCommand
from models.sheet_models import Payload
from utils.math_utils import round_up_to_n_decimals

logger = logging.getLogger(__name__)


class PricingEngine:
    """
    Stateless pricing calculator operating on fully prepared inputs.
    """

    def __init__(self, analyzer: CompetitionAnalyzer, log_formatter: LogFormatter):
        self.analyzer = analyzer
        self.log_formatter = log_formatter
        self.reference_price_selector = ReferencePriceSelector()

    async def process(self, prepared: PreparedPricingInput) -> PreparedPricingResult:
        payload = prepared.payload
        payload.offer_id = prepared.identifiers.offer_id
        payload.real_product_id = prepared.identifiers.product_id
        payload.current_price = prepared.current_offer.price

        mode = payload.compare_mode

        if mode == 0:
            return self._handle_mode_0(prepared)

        analysis = None
        if prepared.competition.offers:
            analysis = self.analyzer.analyze(payload, prepared.competition.offers)
        else:
            analysis = AnalysisResult()

        selected_reference = self.reference_price_selector.select_best_price(payload, analysis.competitive_price)
        if selected_reference is not None:
            analysis.selected_reference_name = (
                analysis.competitor_name
                if selected_reference.source_name == "Competition"
                else selected_reference.source_name
            )
            analysis.selected_reference_price = selected_reference.price

        target = self._calc_final_price(
            payload,
            competitor_price=selected_reference.price if selected_reference is not None else None,
        )
        competitor_name = analysis.selected_reference_name or analysis.competitor_name or "No Competition"

        min_price_val = payload.fetched_min_price
        if min_price_val is None:
            min_price_val = payload.get_min_price_value()

        if min_price_val is None:
            return PreparedPricingResult(
                status=0, payload=payload, target=prepared.target,
                log_message=self.log_formatter.format("no_min_price", payload, target, analysis),
                analysis=analysis,
            )

        if payload.current_price < min_price_val:
            target = min_price_val
            payload.applied_adj = 0.0

        if target < min_price_val:
            return PreparedPricingResult(
                status=0, payload=payload, target=prepared.target,
                log_message=self.log_formatter.format("below_min", payload, target, analysis),
                analysis=analysis,
            )

        if mode == 1:
            return self._handle_mode_1(prepared, target, competitor_name, analysis)
        if mode == 2:
            return self._handle_mode_2(prepared, target, competitor_name, analysis)

        return PreparedPricingResult(
            status=0, payload=payload, target=prepared.target,
            log_message=f"Unknown mode: {mode}"
        )

    def _calc_final_price(self, payload: Payload, competitor_price: Optional[float]) -> float:
        """Apply the pricing formula: raw → adjust → clamp → round."""
        payload.applied_adj = 0.0

        # Step 1: Raw target
        price = competitor_price
        if price is None:
            price = payload.fetched_max_price
        if price is None:
            return 0.0  # Cannot determine — will be caught by min price protection

        # Step 2: Random undercut
        if payload.min_price_adjustment is not None and payload.max_price_adjustment is not None:
            min_a = min(payload.min_price_adjustment, payload.max_price_adjustment)
            max_a = max(payload.min_price_adjustment, payload.max_price_adjustment)
            adj = random.uniform(min_a, max_a)
            payload.applied_adj = adj
            price -= adj

        # Step 3: Clamp
        if payload.fetched_min_price is not None:
            price = max(price, payload.fetched_min_price)
        if payload.fetched_max_price is not None:
            price = min(price, payload.fetched_max_price)

        # Step 4: Round up
        rounding = payload.price_rounding if payload.price_rounding is not None else 2
        price = round_up_to_n_decimals(price, rounding)

        return price

    def _handle_mode_0(self, prepared: PreparedPricingInput) -> PreparedPricingResult:
        """No Compare: target = min_price."""
        payload = prepared.payload
        min_price_val = payload.fetched_min_price
        if min_price_val is None:
            min_price_val = payload.get_min_price_value()

        if min_price_val is None:
            return PreparedPricingResult(
                status=0, payload=payload, target=prepared.target,
                log_message="Mode 0: No min price set"
            )

        rounding = payload.price_rounding if payload.price_rounding is not None else 2
        target = round_up_to_n_decimals(min_price_val, rounding)
        payload.applied_adj = 0.0

        if not self._is_significant(payload.current_price, target, payload):
            return PreparedPricingResult(
                status=2, payload=payload, target=prepared.target,
                log_message=self.log_formatter.format("equal", payload, payload.current_price),
            )

        return PreparedPricingResult(
            status=1, payload=payload, target=prepared.target,
            final_price=CompareTarget(name="No Comparison", price=target),
            log_message=self.log_formatter.format("not_compare", payload, target),
            update_command=PriceUpdateCommand(
                offer_id=prepared.current_offer.offer_id,
                new_price=target,
            ),
        )

    def _handle_mode_1(
        self,
        prepared: PreparedPricingInput,
        target: float,
        comp_name: str,
        analysis: Optional[AnalysisResult],
    ) -> PreparedPricingResult:
        """Always Follow: update to target regardless of direction."""
        payload = prepared.payload
        if not self._is_significant(payload.current_price, target, payload):
            return PreparedPricingResult(
                status=2, payload=payload, target=prepared.target,
                log_message=self.log_formatter.format("equal", payload, payload.current_price, analysis),
                analysis=analysis,
            )
        return PreparedPricingResult(
            status=1, payload=payload, target=prepared.target,
            final_price=CompareTarget(name=comp_name, price=target),
            log_message=self.log_formatter.format("compare", payload, target, analysis),
            analysis=analysis,
            update_command=PriceUpdateCommand(
                offer_id=prepared.current_offer.offer_id,
                new_price=target,
            ),
        )

    def _handle_mode_2(
        self,
        prepared: PreparedPricingInput,
        target: float,
        comp_name: str,
        analysis: Optional[AnalysisResult],
    ) -> PreparedPricingResult:
        """Smart Follow: only decrease, never increase."""
        payload = prepared.payload
        if payload.current_price < target and self._is_significant(payload.current_price, target, payload):
            msg = self.log_formatter.format("equal", payload, payload.current_price, analysis)
            msg = msg.replace("matches target", "already below target (Mode 2 — Hold)")
            return PreparedPricingResult(
                status=2, payload=payload, target=prepared.target,
                log_message=msg,
                analysis=analysis,
            )
        if not self._is_significant(payload.current_price, target, payload):
            return PreparedPricingResult(
                status=2, payload=payload, target=prepared.target,
                log_message=self.log_formatter.format("equal", payload, payload.current_price, analysis),
                analysis=analysis,
            )
        return PreparedPricingResult(
            status=1, payload=payload, target=prepared.target,
            final_price=CompareTarget(name=comp_name, price=target),
            log_message=self.log_formatter.format("compare", payload, target, analysis),
            analysis=analysis,
            update_command=PriceUpdateCommand(
                offer_id=prepared.current_offer.offer_id,
                new_price=target,
            ),
        )

    @staticmethod
    def _is_significant(price1: float, price2: float, payload: Payload) -> bool:
        """True if price difference exceeds noise threshold."""
        rounding = payload.price_rounding if payload.price_rounding is not None else 2
        step = 1 / (10 ** rounding)

        noise = 0.0
        if payload.min_price_adjustment is not None and payload.max_price_adjustment is not None:
            noise = abs(payload.max_price_adjustment - payload.min_price_adjustment)

        threshold = max(noise + step * 0.5, step * 1.5)
        return abs(price1 - price2) > threshold
