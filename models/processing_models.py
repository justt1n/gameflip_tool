from typing import List, Optional
from pydantic import BaseModel
from models.standard_models import StandardCompetitorOffer
from models.sheet_models import Payload


class CompareTarget(BaseModel):
    """The competitor we're targeting and the final calculated price."""
    name: str       # Competitor name or "No Competition" or "No Comparison"
    price: float    # The calculated final BASE price to set


class AnalysisResult(BaseModel):
    """Output of competition analysis — who are we targeting?"""
    competitor_name: Optional[str] = None
    competitive_price: Optional[float] = None
    top_sellers_for_log: Optional[List[StandardCompetitorOffer]] = None
    sellers_below_min: Optional[List[StandardCompetitorOffer]] = None


class ProcessingResult(BaseModel):
    """
    Output of processing a single row.

    status values:
      0 = Error / Skip (do not update marketplace, just log)
      1 = Update (call adapter.update_price with final_price)
      2 = No Change Needed (price already matches, just log)
    """
    status: int  # 0=error/skip, 1=update, 2=no-change
    payload: Payload
    final_price: Optional[CompareTarget] = None
    log_message: Optional[str] = None
    offer_id: Optional[str] = None
    offer_type: Optional[str] = None
    competition: Optional[List[StandardCompetitorOffer]] = None

