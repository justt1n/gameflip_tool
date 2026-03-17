import logging
from typing import Annotated, List, Optional, ClassVar, Dict, Any
from pydantic import BaseModel, ValidationError, computed_field


def _col_letter_to_index(col_name: str) -> int:
    """
    Convert column letter(s) to zero-based index.
    'A' → 0, 'B' → 1, ..., 'Z' → 25, 'AA' → 26, 'AB' → 27, ...
    """
    index = 0
    for char in col_name.upper():
        index = index * 26 + (ord(char) - ord('A') + 1)
    return index - 1


class BaseGSheetModel(BaseModel):
    """
    Base class for any Pydantic model that maps to a Google Sheet row.

    HOW IT WORKS:
    1. Subclass this and annotate fields with column letters:
       product_name: Annotated[str, "C"]
       price: Annotated[Optional[float], "L"] = None

    2. On first use, the class introspects its own fields, extracts the
       column-letter metadata, and builds two lookup maps:
       - _index_map: {"product_name": 2, "price": 11}  (field → column index)
       - _col_map:   {"product_name": "C", "price": "L"} (field → column letter)

    3. from_row(row_data, row_index) uses _index_map to extract values by position.
    4. prepare_update(sheet_name, updates) uses _col_map to build A1-notation ranges.

    This means: to change column layout, just change the annotation letter.
    No other code changes needed.
    """
    row_index: int  # The 1-based row number on the sheet (for write-back)

    _index_map: ClassVar[Optional[Dict[str, int]]] = None
    _col_map: ClassVar[Optional[Dict[str, str]]] = None

    @classmethod
    def _build_maps_if_needed(cls):
        """Lazily build the column mapping from field annotations."""
        if cls._index_map is not None and cls._col_map is not None:
            return

        index_map = {}
        col_map = {}
        for field_name, field_info in cls.model_fields.items():
            if not field_info.metadata:
                continue
            column_letter = field_info.metadata[0]
            if isinstance(column_letter, str):
                index_map[field_name] = _col_letter_to_index(column_letter)
                col_map[field_name] = column_letter

        cls._index_map = index_map
        cls._col_map = col_map

    @classmethod
    def from_row(cls, row_data: List[str], row_index: int) -> Optional['BaseGSheetModel']:
        """
        Parse a raw sheet row (list of strings) into a typed model instance.

        Args:
            row_data: Raw row from Google Sheets API (list of cell values as strings).
            row_index: The 1-based row number on the sheet.

        Returns:
            Model instance, or None if the row is empty or validation fails.

        BEHAVIOR:
        - Empty strings are treated as None.
        - If ALL mapped fields are None/empty, returns None (skip blank rows).
        - Pydantic validation errors are caught and logged, returning None.
        """
        data_dict = cls._row_to_data_dict(row_data)

        # Skip completely empty rows
        if not any(data_dict.values()):
            return None

        data_dict['row_index'] = row_index

        try:
            return cls.model_validate(data_dict)
        except ValidationError as e:
            logging.warning(f"Skipping row {row_index}: validation error: {e}")
            return None

    @classmethod
    def _row_to_data_dict(cls, row_data: List[str]) -> Dict[str, Any]:
        cls._build_maps_if_needed()

        data_dict = {}
        for field_name, col_index in cls._index_map.items():
            if col_index < len(row_data):
                value = row_data[col_index]
                data_dict[field_name] = value if value != '' else None
        return data_dict

    def prepare_update(self, sheet_name: str, updates: Dict[str, Any]) -> List[Dict]:
        """
        Generate batch-update request payloads for Google Sheets API.

        Args:
            sheet_name: The sheet tab name (e.g., 'driffle').
            updates: Dict mapping field names to new values.
                     Example: {'note': 'Updated price', 'last_update': '2026-02-28 10:30:00'}

        Returns:
            List of dicts like: [{'range': 'driffle!E15', 'values': [['Updated price']]}]

        Uses _col_map to translate field names back to column letters.
        """
        self._build_maps_if_needed()
        update_requests = []
        for field_name, new_value in updates.items():
            column_letter = self._col_map.get(field_name)
            if not column_letter:
                logging.warning(f"Field '{field_name}' has no column mapping, skipping.")
                continue
            cell_range = f"{sheet_name}!{column_letter}{self.row_index}"
            update_requests.append({
                'range': cell_range,
                'values': [[str(new_value)]]
            })
        return update_requests


class SheetLocation(BaseModel):
    """Pointer to an external sheet cell."""
    sheet_id: Optional[str] = None
    sheet_name: Optional[str] = None
    cell: Optional[str] = None


class Payload(BaseGSheetModel):
    """
    One row from the control sheet = one product to process.

    COLUMN ANNOTATIONS: Each Annotated[..., "X"] maps the field to column X.
    To rearrange columns in the sheet, just change the letter here. No other code changes.
    """
    # --- Sheet-mapped columns ---
    is_2lai_enabled_str: Annotated[Optional[str], "A"] = None
    is_check_enabled_str: Annotated[Optional[str], "B"] = None
    product_name: Annotated[str, "C"]
    parameters: Annotated[Optional[str], "D"] = None
    note: Annotated[Optional[str], "E"] = None          # OUTPUT: log text
    last_update: Annotated[Optional[str], "F"] = None    # OUTPUT: timestamp
    product_id: Annotated[Optional[str], "G"] = None     # URL or raw ID
    is_compare_enabled_str: Annotated[Optional[str], "H"] = None
    product_compare: Annotated[Optional[str], "I"] = None
    include_keyword: Annotated[Optional[str], "J"] = None
    filter_options: Annotated[Optional[str], "K"] = None
    min_price_adjustment: Annotated[Optional[float], "L"] = None
    max_price_adjustment: Annotated[Optional[float], "M"] = None
    price_rounding: Annotated[Optional[int], "N"] = None
    idsheet_min: Annotated[Optional[str], "O"] = None
    sheet_min: Annotated[Optional[str], "P"] = None
    cell_min: Annotated[Optional[str], "Q"] = None
    idsheet_max: Annotated[Optional[str], "R"] = None
    sheet_max: Annotated[Optional[str], "S"] = None
    cell_max: Annotated[Optional[str], "T"] = None
    idsheet_stock: Annotated[Optional[str], "U"] = None
    sheet_stock: Annotated[Optional[str], "V"] = None
    cell_stock: Annotated[Optional[str], "W"] = None
    idsheet_blacklist: Annotated[Optional[str], "X"] = None
    sheet_blacklist: Annotated[Optional[str], "Y"] = None
    cell_blacklist: Annotated[Optional[str], "Z"] = None
    relax: Annotated[Optional[str], "AA"] = None
    min_price: Annotated[Optional[str], "AB"] = None

    # --- Runtime fields (NOT mapped to sheet, populated during processing) ---
    fetched_min_price: Optional[float] = None
    fetched_max_price: Optional[float] = None
    fetched_stock: Optional[int] = 999
    fetched_black_list: Optional[List[str]] = None
    offer_id: Optional[str] = None         # Resolved by adapter
    real_product_id: Optional[str] = None   # Resolved by adapter
    current_price: Optional[float] = None   # Fetched from marketplace
    applied_adj: Optional[float] = 0.0      # The random adjustment that was applied
    product_link: Optional[str] = None      # Future requirement-sheet compatibility
    exclude_keyword: Optional[str] = None   # Future requirement-sheet compatibility
    category_name: Optional[str] = None     # Future requirement-sheet compatibility
    game_name: Optional[str] = None         # Future requirement-sheet compatibility
    resolved_listing_id: Optional[str] = None
    resolved_listing_name: Optional[str] = None
    sheet_schema: str = "legacy"

    REQUIREMENT_INDEX_MAP: ClassVar[Dict[str, int]] = {
        "is_2lai_enabled_str": 0,
        "is_check_enabled_str": 1,
        "product_name": 2,
        "note": 3,
        "last_update": 4,
        "product_link": 5,
        "product_compare": 6,
        "include_keyword": 7,
        "exclude_keyword": 8,
        "category_name": 9,
        "game_name": 10,
        "min_price_adjustment": 11,
        "max_price_adjustment": 12,
        "price_rounding": 13,
        "idsheet_min": 17,
        "sheet_min": 18,
        "cell_min": 19,
        "idsheet_max": 23,
        "sheet_max": 24,
        "cell_max": 25,
        "idsheet_stock": 29,
        "sheet_stock": 30,
        "cell_stock": 31,
        "idsheet_blacklist": 38,
        "sheet_blacklist": 39,
        "cell_blacklist": 40,
    }
    REQUIREMENT_UPDATE_COLUMNS: ClassVar[Dict[str, str]] = {
        "note": "D",
        "last_update": "E",
    }

    @classmethod
    def _row_to_data_dict(cls, row_data: List[str]) -> Dict[str, Any]:
        if len(row_data) >= 69:
            return cls._requirement_row_to_data_dict(row_data)
        return super()._row_to_data_dict(row_data)

    @classmethod
    def _requirement_row_to_data_dict(cls, row_data: List[str]) -> Dict[str, Any]:
        data_dict: Dict[str, Any] = {}
        for field_name, col_index in cls.REQUIREMENT_INDEX_MAP.items():
            if col_index < len(row_data):
                value = row_data[col_index]
                data_dict[field_name] = value if value != '' else None

        # Synthesize product_id for the legacy pipeline:
        # prefer explicit search URL, then fallback textual search source.
        data_dict["product_id"] = (
            data_dict.get("product_compare")
            or data_dict.get("product_link")
            or data_dict.get("product_name")
        )
        data_dict["filter_options"] = data_dict.get("exclude_keyword")
        data_dict["sheet_schema"] = "requirement"
        return data_dict

    def prepare_update(self, sheet_name: str, updates: Dict[str, Any]) -> List[Dict]:
        if self.sheet_schema != "requirement":
            return super().prepare_update(sheet_name, updates)

        update_requests = []
        for field_name, new_value in updates.items():
            column_letter = self.REQUIREMENT_UPDATE_COLUMNS.get(field_name)
            if not column_letter:
                continue
            cell_range = f"{sheet_name}!{column_letter}{self.row_index}"
            update_requests.append({
                'range': cell_range,
                'values': [[str(new_value)]]
            })
        return update_requests

    # --- Computed locations (group the 3-column pointers into SheetLocation objects) ---
    @computed_field
    @property
    def min_price_location(self) -> SheetLocation:
        return SheetLocation(sheet_id=self.idsheet_min, sheet_name=self.sheet_min, cell=self.cell_min)

    @computed_field
    @property
    def max_price_location(self) -> SheetLocation:
        return SheetLocation(sheet_id=self.idsheet_max, sheet_name=self.sheet_max, cell=self.cell_max)

    @computed_field
    @property
    def stock_location(self) -> SheetLocation:
        return SheetLocation(sheet_id=self.idsheet_stock, sheet_name=self.sheet_stock, cell=self.cell_stock)

    @computed_field
    @property
    def blacklist_location(self) -> SheetLocation:
        return SheetLocation(sheet_id=self.idsheet_blacklist, sheet_name=self.sheet_blacklist, cell=self.cell_blacklist)

    # --- Convenience properties ---
    @property
    def is_check_enabled(self) -> bool:
        return self.is_check_enabled_str == '1'

    @property
    def compare_mode(self) -> int:
        """0 = no compare, 1 = always follow, 2 = smart follow (only decrease)"""
        if self.is_compare_enabled_str == '1':
            return 1
        elif self.is_compare_enabled_str == '2':
            return 2
        return 0

    def get_min_price_value(self) -> Optional[float]:
        """Parse inline min_price (col AB) as float. Returns None if not set."""
        if self.min_price is None:
            return None
        try:
            return float(self.min_price.replace(',', '').strip())
        except (ValueError, TypeError):
            return None
