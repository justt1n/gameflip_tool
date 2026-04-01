from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class GameflipApiError(BaseModel):
    model_config = ConfigDict(extra="ignore")

    message: str
    code: Optional[int] = None


class GameflipProfile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    owner: str
    display_name: Optional[str] = None
    steam_id: Optional[str] = None


class GameflipWallet(BaseModel):
    model_config = ConfigDict(extra="ignore")

    owner: str
    balance: Optional[int] = None
    cash_balance: Optional[int] = None
    held_cash_balance: Optional[int] = None


class GameflipPhoto(BaseModel):
    model_config = ConfigDict(extra="ignore")

    status: Optional[str] = None
    display_order: Optional[int] = None
    view_url: Optional[str] = None


class GameflipListing(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    owner: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    platform: Optional[str] = None
    accept_currency: Optional[str] = None
    price: Optional[int] = None
    upc: Optional[str] = None
    status: Optional[str] = None
    version: Optional[str | int] = None
    condition: Optional[str] = None
    digital: Optional[bool] = None
    digital_region: Optional[str] = None
    digital_deliverable: Optional[str] = None
    digital_fee: Optional[int] = None
    commission: Optional[int] = None
    expire_in_days: Optional[int] = None
    seller_ratings: Optional[int] = None
    shipping_paid_by: Optional[str] = None
    shipping_fee: Optional[int] = None
    shipping_within_days: Optional[str | int] = None
    shipping_from_state: Optional[str] = None
    shipping_predefined_package: Optional[str] = None
    cover_photo: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    photo: dict[str, GameflipPhoto] = Field(default_factory=dict)


class GameflipSearchResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    listings: List[GameflipListing] = Field(default_factory=list)
    next_page: Optional[str] = None
    raw: Any = None
