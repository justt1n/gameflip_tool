from typing import Optional

from pydantic import BaseModel, Field

from models.processing_models import AnalysisResult, CompareTarget
from models.sheet_models import Payload
from models.standard_models import PlatformIdentifiers, StandardCompetitorOffer


class OwnedListingIndexEntry(BaseModel):
    id: str
    owner: Optional[str] = None
    name: Optional[str] = None
    category: Optional[str] = None
    platform: Optional[str] = None
    digital_region: Optional[str] = None
    upc: Optional[str] = None
    status: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    search_text: str = ""


class ResolvedListingTarget(BaseModel):
    payload: Payload
    listing_id: str
    listing_name: Optional[str] = None


class PreparedCurrentOffer(BaseModel):
    offer_id: str
    product_id: str
    price: float
    status: str
    raw_status: Optional[str] = None
    offer_type: str
    currency: str = "USD"
    version: Optional[str | int] = None


class PreparedCompetition(BaseModel):
    offers: list[StandardCompetitorOffer] = Field(default_factory=list)
    raw_count: int = 0


class PreparedPricingInput(BaseModel):
    payload: Payload
    target: ResolvedListingTarget
    identifiers: PlatformIdentifiers
    current_offer: PreparedCurrentOffer
    competition: PreparedCompetition = Field(default_factory=PreparedCompetition)


class PriceUpdateCommand(BaseModel):
    offer_id: str
    new_price: float


class PreparedPricingResult(BaseModel):
    status: int
    payload: Payload
    target: ResolvedListingTarget
    final_price: Optional[CompareTarget] = None
    log_message: Optional[str] = None
    update_command: Optional[PriceUpdateCommand] = None
    analysis: Optional[AnalysisResult] = None


class DuplicateListingResult(BaseModel):
    override_note: Optional[str] = None
    append_note: Optional[str] = None
    created_count: int = 0
    active_before: int = 0
    active_after: int = 0


class ProcessedTargetOutcome(BaseModel):
    note: str
    final_price: Optional[float] = None
