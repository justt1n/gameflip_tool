from datetime import datetime
from typing import Optional

from models.processing_models import AnalysisResult
from models.sheet_models import Payload


class LogFormatter:
    """
    Generates structured log strings for the sheet note column.

    Output format:
    STATUS_PREFIX
    [DD/MM HH:MM] Main message
    - Targeting: SellerName (price)
    - Range: [min - max]
    - Below Min: seller1=price; seller2=price
    - Top Sellers: seller1=price; seller2=price; ...
    """

    def format(
        self,
        mode: str,
        payload: Payload,
        final_price: float,
        analysis: Optional[AnalysisResult] = None
    ) -> str:
        timestamp = datetime.now().strftime("%d/%m %H:%M")

        min_val = payload.get_min_price_value()
        min_str = f"{min_val:.3f}" if min_val is not None else "None"

        messages = {
            "not_compare": ("UPDATE", f"Updated (no comparison): {final_price:.3f}"),
            "compare":     ("UPDATE", f"Updated successfully: {final_price:.3f}"),
            "below_min":   ("SKIP",   f"Calculated ({final_price:.3f}) below min ({min_str})"),
            "no_min_price":("SKIP",   "Min price not configured."),
            "equal":       ("SKIP",   f"Current price ({final_price:.3f}) matches target"),
        }

        prefix, message = messages.get(mode, ("INFO", f"Mode: {mode}"))
        log = f"{prefix}\n[{timestamp}] {message}\n"

        if analysis:
            log += self._format_analysis(payload, analysis)

        return log

    def _format_analysis(self, payload: Payload, analysis: AnalysisResult) -> str:
        parts = []

        # Target
        comp_price = analysis.competitive_price
        comp_name = analysis.competitor_name
        if comp_price is None or comp_price == float('inf'):
            comp_name = "Max price"
            comp_price = payload.fetched_max_price
        if comp_name and comp_price is not None:
            parts.append(f"- Targeting: {comp_name} ({comp_price:.3f})\n")

        # Range
        min_s = f"{payload.fetched_min_price:.3f}" if payload.fetched_min_price is not None else "None"
        max_s = f"{payload.fetched_max_price:.3f}" if payload.fetched_max_price is not None else "None"
        parts.append(f"- Range: [{min_s} - {max_s}]\n")

        # Sellers below min
        if analysis.sellers_below_min:
            info = "; ".join(
                f"{s.seller_name}={s.price:.3f}"
                for s in analysis.sellers_below_min[:3]
            )
            parts.append(f"- Below Min: {info}\n")

        # Top sellers
        if analysis.top_sellers_for_log:
            sorted_offers = sorted(analysis.top_sellers_for_log, key=lambda o: o.price)
            top = "; ".join(
                f"{o.seller_name}={o.price:.3f}"
                for o in sorted_offers[:4]
            )
            parts.append(f"- Top Sellers: {top}\n")

        return "".join(parts)

