from typing import List

from models.standard_models import StandardCompetitorOffer
from models.processing_models import AnalysisResult
from models.sheet_models import Payload


class CompetitionAnalyzer:
    """
    Analyzes a list of StandardCompetitorOffers and produces an AnalysisResult.

    Stateless service. Filters competitors by blacklist, finds the lowest valid offer.
    """

    def analyze(
        self,
        payload: Payload,
        offers: List[StandardCompetitorOffer]
    ) -> AnalysisResult:
        blacklist = payload.fetched_black_list or []
        blacklist_lower = {s.lower() for s in blacklist}

        # Filter blacklisted sellers
        non_blacklisted = [
            o for o in offers
            if o.seller_name.lower() not in blacklist_lower
        ]

        if not non_blacklisted:
            return AnalysisResult(
                competitor_name=None,
                competitive_price=None,
                top_sellers_for_log=offers,
                sellers_below_min=[]
            )

        # Find lowest eligible
        valid = [o for o in non_blacklisted if o.is_eligible]
        sorted_for_log = sorted(
            non_blacklisted,
            key=lambda x: (not x.is_eligible, x.price)
        )

        competitor_name = None
        competitive_price = None
        if valid:
            lowest = min(valid, key=lambda o: o.price)
            competitor_name = lowest.seller_name
            competitive_price = lowest.price

        # Sellers below min price (for alerting)
        min_price = payload.get_min_price_value()
        sellers_below_min = []
        if min_price is not None:
            sellers_below_min = [
                o for o in non_blacklisted if o.price < min_price
            ]

        return AnalysisResult(
            competitor_name=competitor_name,
            competitive_price=competitive_price,
            top_sellers_for_log=sorted_for_log,
            sellers_below_min=sellers_below_min
        )

