import sys
import os
import pytest
from typing import Optional, List

# Add the project root to the path so tests can import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from interfaces.marketplace_adapter import IMarketplaceAdapter
from models.runtime_models import (
    PreparedCompetition,
    PreparedCurrentOffer,
    PreparedPricingInput,
    ResolvedListingTarget,
)
from models.sheet_models import Payload
from models.standard_models import (
    PlatformIdentifiers,
    StandardCompetitorOffer,
)


# ═══════════════════════════════════════════════════════════════
# RAW SHEET ROW DATA — matches Google Sheet columns A–AB (28 cols)
# ═══════════════════════════════════════════════════════════════
# A=is_2lai  B=CHECK  C=product_name  D=parameters  E=note  F=last_update
# G=product_id  H=compare_mode  I=product_compare  J=include_keyword
# K=filter_options  L=min_adj  M=max_adj  N=rounding
# O=idsheet_min  P=sheet_min  Q=cell_min
# R=idsheet_max  S=sheet_max  T=cell_max
# U=idsheet_stock  V=sheet_stock  W=cell_stock
# X=idsheet_blacklist  Y=sheet_blacklist  Z=cell_blacklist
# AA=relax  AB=min_price

HEADER_ROW = [
    "2LAI", "CHECK", "Product_name", "Product_pack", "Note", "Last_update",
    "Product_ID", "Compare", "Product_compare", "Include_keyword",
    "Filter_options", "Min_adj", "Max_adj", "Rounding",
    "IDSheet_min", "Sheet_min", "Cell_min",
    "IDSheet_max", "Sheet_max", "Cell_max",
    "IDSheet_stock", "Sheet_stock", "Cell_stock",
    "IDSheet_blacklist", "Sheet_blacklist", "Cell_blacklist",
    "Relax", "Min_price"
]

ROW_MODE1_FULL = [
    "",                                      # A: is_2lai
    "1",                                     # B: CHECK
    "Game Key Pro Edition",                  # C: product_name
    "",                                      # D: parameters
    "",                                      # E: note (output)
    "",                                      # F: last_update (output)
    "https://mock.com/product/12345",        # G: product_id
    "1",                                     # H: compare_mode (always follow)
    "",                                      # I: product_compare
    "",                                      # J: include_keyword
    "",                                      # K: filter_options
    "0.01",                                  # L: min_adj
    "0.05",                                  # M: max_adj
    "2",                                     # N: rounding
    "spreadsheet_abc",                       # O: idsheet_min
    "Prices",                                # P: sheet_min
    "B5",                                    # Q: cell_min
    "spreadsheet_abc",                       # R: idsheet_max
    "Prices",                                # S: sheet_max
    "C5",                                    # T: cell_max
    "spreadsheet_abc",                       # U: idsheet_stock
    "Inventory",                             # V: sheet_stock
    "D5",                                    # W: cell_stock
    "spreadsheet_abc",                       # X: idsheet_blacklist
    "Blacklist",                             # Y: sheet_blacklist
    "A1:A",                                  # Z: cell_blacklist (unbounded)
    "",                                      # AA: relax
    "",                                      # AB: min_price (inline)
]

ROW_MODE0_INLINE_MIN = [
    "", "1", "Budget Game Key", "", "", "",
    "https://mock.com/product/67890", "0",
    "", "", "", "", "", "2",
    "", "", "", "", "", "",
    "", "", "", "", "", "",
    "", "8.50",
]

ROW_MODE2_SMART = [
    "", "1", "Premium Bundle", "", "", "",
    "https://mock.com/product/11111", "2",
    "", "", "", "0.02", "0.04", "2",
    "spreadsheet_abc", "Prices", "B10",
    "spreadsheet_abc", "Prices", "C10",
    "", "", "", "", "", "",
    "", "",
]

ROW_DISABLED = [
    "", "0", "Disabled Product", "", "", "",
    "https://mock.com/product/99999", "1",
    "", "", "", "", "", "2",
    "", "", "", "", "", "",
    "", "", "", "", "", "",
    "", "",
]

ROW_EMPTY = [""] * 28

ROW_MISSING_NAME = [
    "", "1", "", "", "", "",
    "https://mock.com/product/55555", "1",
    "", "", "", "", "", "2",
    "", "", "", "", "", "",
    "", "", "", "", "", "",
    "", "",
]

ROW_NO_MIN_PRICE = [
    "", "1", "No Min Product", "", "", "",
    "https://mock.com/product/22222", "1",
    "", "", "", "0.01", "0.05", "2",
    "", "", "",
    "spreadsheet_abc", "Prices", "C5",
    "", "", "", "", "", "",
    "", "",
]

ROW_NO_ADJUSTMENTS = [
    "", "1", "No Adj Product", "", "", "",
    "https://mock.com/product/33333", "1",
    "", "", "", "", "", "2",
    "spreadsheet_abc", "Prices", "B5",
    "spreadsheet_abc", "Prices", "C5",
    "", "", "", "", "", "",
    "", "",
]

ROW_WITH_RELAX = [
    "", "1", "Relax Product", "", "", "",
    "https://mock.com/product/44444", "1",
    "", "", "", "0.01", "0.05", "2",
    "", "", "", "", "", "",
    "", "", "", "", "", "",
    "3", "",
]

ROW_BAD_RELAX = [
    "", "1", "Bad Relax Product", "", "", "",
    "https://mock.com/product/55555", "1",
    "", "", "", "0.01", "0.05", "2",
    "", "", "", "", "", "",
    "", "", "", "", "", "",
    "abc", "",
]


# ═══════════════════════════════════════════════════════════════
# FIXTURES — Sheet Data
# ═══════════════════════════════════════════════════════════════

@pytest.fixture
def full_sheet_data():
    """Simulates GoogleSheetsClient.get_data() with junk rows + header + data."""
    return [
        ["NOTES ABOVE THE TABLE", "", "", ""],   # Row 1 (junk)
        ["Last edited: 2026-02-28", "", "", ""],  # Row 2 (junk)
        HEADER_ROW,                                # Row 3 (header)
        ROW_MODE1_FULL,                            # Row 4
        ROW_MODE0_INLINE_MIN,                      # Row 5
        ROW_DISABLED,                              # Row 6 (skipped)
        ROW_MODE2_SMART,                           # Row 7
        ROW_EMPTY,                                 # Row 8 (skipped)
        ROW_NO_MIN_PRICE,                          # Row 9
    ]


@pytest.fixture
def hydration_responses():
    """Simulates batch_get_data() return for external sheet refs."""
    return {
        "'Prices'!B5": [[12.50]],
        "'Prices'!C5": [[18.00]],
        "'Prices'!B10": [[10.00]],
        "'Prices'!C10": [[16.00]],
        "'Inventory'!D5": [[42]],
        "'Blacklist'!A1:A1000": [
            ["CheapKeys"],
            ["ShadySeller"],
            ["BotStore99"],
        ],
    }


# ═══════════════════════════════════════════════════════════════
# FIXTURES — Competitor Offer Sets
# ═══════════════════════════════════════════════════════════════

@pytest.fixture
def competitors_normal():
    return [
        StandardCompetitorOffer(seller_name="AlphaKeys", price=14.20, is_eligible=True),
        StandardCompetitorOffer(seller_name="BetaGames", price=14.80, is_eligible=True),
        StandardCompetitorOffer(seller_name="CheapKeys", price=11.00, is_eligible=True),
        StandardCompetitorOffer(seller_name="DeltaShop", price=19.50, is_eligible=False, note="Base > Max"),
        StandardCompetitorOffer(seller_name="EpsilonStore", price=10.00, is_eligible=False, note="Base < Min"),
    ]

@pytest.fixture
def competitors_all_blacklisted():
    return [
        StandardCompetitorOffer(seller_name="CheapKeys", price=13.00, is_eligible=True),
        StandardCompetitorOffer(seller_name="ShadySeller", price=14.00, is_eligible=True),
    ]

@pytest.fixture
def competitors_empty():
    return []

@pytest.fixture
def competitors_all_ineligible():
    return [
        StandardCompetitorOffer(seller_name="HighSeller", price=25.00, is_eligible=False, note="Base > Max"),
        StandardCompetitorOffer(seller_name="LowSeller", price=5.00, is_eligible=False, note="Base < Min"),
    ]

@pytest.fixture
def competitors_single_rival():
    return [
        StandardCompetitorOffer(seller_name="OnlyRival", price=13.50, is_eligible=True),
    ]


# ═══════════════════════════════════════════════════════════════
# ConfigurableMockAdapter
# ═══════════════════════════════════════════════════════════════

class ConfigurableMockAdapter(IMarketplaceAdapter):
    """Mock adapter with a prepared-input contract."""

    def __init__(
        self,
        platform: str = "mock",
        my_price: float = 15.00,
        my_status: str = "active",
        competitors: List[StandardCompetitorOffer] = None,
        update_succeeds: bool = True,
        resolve_fails: bool = False,
        offer_fails: bool = False,
    ):
        self.platform = platform
        self.my_price = my_price
        self.my_status = my_status
        self.competitors = competitors or []
        self.update_succeeds = update_succeeds
        self.resolve_fails = resolve_fails
        self.offer_fails = offer_fails
        self.updated_prices: List[dict] = []

    def get_platform_name(self) -> str:
        return self.platform

    async def resolve_payload_targets(self, payload: Payload) -> list[ResolvedListingTarget]:
        if self.resolve_fails:
            raise ValueError(f"Cannot resolve: {payload.product_id}")
        payload_copy = payload.model_copy(
            update={
                "resolved_listing_id": "offer_001",
                "resolved_listing_name": payload.product_name,
            },
            deep=True,
        )
        return [
            ResolvedListingTarget(
                payload=payload_copy,
                listing_id="offer_001",
                listing_name=payload.product_name,
            )
        ]

    async def prepare_pricing_input(self, target: ResolvedListingTarget) -> PreparedPricingInput:
        if self.offer_fails:
            raise ValueError(f"Failed to fetch current offer for {target.listing_id}")
        payload = target.payload.model_copy(
            update={
                "offer_id": target.listing_id,
                "real_product_id": "prod_001",
                "current_price": self.my_price,
            },
            deep=True,
        )
        return PreparedPricingInput(
            payload=payload,
            target=ResolvedListingTarget(
                payload=payload,
                listing_id=target.listing_id,
                listing_name=target.listing_name,
            ),
            identifiers=PlatformIdentifiers(
                offer_id=target.listing_id,
                product_id="prod_001",
                platform=self.platform,
            ),
            current_offer=PreparedCurrentOffer(
                offer_id=target.listing_id,
                product_id="prod_001",
                price=self.my_price,
                status=self.my_status,
                raw_status=self.my_status,
                offer_type="key",
                currency="USD",
            ),
            competition=PreparedCompetition(offers=self.competitors),
        )

    async def update_price(
        self,
        offer_id: str,
        new_price: float,
        current_version=None,
        current_status=None,
    ) -> bool:
        self.updated_prices.append({"offer_id": offer_id, "price": new_price})
        return self.update_succeeds

    async def close(self):
        pass


# ═══════════════════════════════════════════════════════════════
# Payload Factory Helper
# ═══════════════════════════════════════════════════════════════

def make_payload(
    product_name: str = "Test Game Key",
    product_id: str = "https://mock.com/product/12345",
    compare_mode: str = "1",
    min_adj: float = None,
    max_adj: float = None,
    rounding: int = 2,
    fetched_min: float = None,
    fetched_max: float = None,
    fetched_stock: int = 999,
    fetched_blacklist: List[str] = None,
    inline_min_price: str = None,
    relax: str = None,
    row_index: int = 5,
) -> Payload:
    """Build a Payload with sensible defaults, pre-hydrated."""
    row = [""] * 28
    row[1] = "1"              # CHECK
    row[2] = product_name     # product_name
    row[6] = product_id       # product_id
    row[7] = compare_mode     # compare mode
    if min_adj is not None:
        row[11] = str(min_adj)
    if max_adj is not None:
        row[12] = str(max_adj)
    row[13] = str(rounding)
    if inline_min_price is not None:
        row[27] = str(inline_min_price)
    if relax is not None:
        row[26] = str(relax)

    p = Payload.from_row(row, row_index=row_index)
    if p:
        p.fetched_min_price = fetched_min
        p.fetched_max_price = fetched_max
        p.fetched_stock = fetched_stock
        p.fetched_black_list = fetched_blacklist
    return p


def make_prepared_input(
    payload: Payload,
    my_price: float = 15.0,
    competitors: Optional[List[StandardCompetitorOffer]] = None,
    offer_id: str = "offer_001",
    product_id: str = "prod_001",
    platform: str = "mock",
    offer_type: str = "key",
    status: str = "active",
) -> PreparedPricingInput:
    payload_copy = payload.model_copy(
        update={
            "offer_id": offer_id,
            "real_product_id": product_id,
            "current_price": my_price,
            "resolved_listing_id": offer_id,
            "resolved_listing_name": payload.product_name,
        },
        deep=True,
    )
    target = ResolvedListingTarget(
        payload=payload_copy,
        listing_id=offer_id,
        listing_name=payload.product_name,
    )
    return PreparedPricingInput(
        payload=payload_copy,
        target=target,
        identifiers=PlatformIdentifiers(
            offer_id=offer_id,
            product_id=product_id,
            platform=platform,
        ),
        current_offer=PreparedCurrentOffer(
            offer_id=offer_id,
            product_id=product_id,
            price=my_price,
            status=status,
            raw_status=status,
            offer_type=offer_type,
            currency="USD",
        ),
        competition=PreparedCompetition(offers=competitors or []),
    )
