from typing import List, Optional
from pydantic import BaseModel


class PlatformIdentifiers(BaseModel):
    """Resolved IDs for a product on a specific platform."""
    offer_id: str       # YOUR listing/offer ID (for get_my_offer, update_price)
    product_id: str     # The catalog product ID (for get_competitors)
    platform: str       # Platform name string


class StandardCurrentOffer(BaseModel):
    """Your current listing details, normalized to a common format."""
    offer_id: str
    price: float        # BASE price (what you actually receive, after commission)
    status: str         # "active", "inactive", "paused"
    offer_type: str     # "key", "gift", "account", "dropshipping"
    currency: str = "EUR"


class StandardCompetitorOffer(BaseModel):
    """A single competitor's offer, normalized."""
    seller_name: str
    price: float        # BASE price (after commission deduction by adapter)
    rating: int = 0
    is_eligible: bool = True   # False if filtered by price range
    note: Optional[str] = None # Reason for ineligibility


class CompetitionResult(BaseModel):
    """Collection of competitor offers returned by an adapter."""
    offers: List[StandardCompetitorOffer]

