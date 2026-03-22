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
    def _format_validation_error(cls, error: ValidationError) -> str:
        """Render a compact, row-friendly validation error message."""
        cls._build_maps_if_needed()

        details = []
        for err in error.errors():
            loc = err.get("loc", ())
            field_name = str(loc[-1]) if loc else "unknown"
            column_letter = cls._col_map.get(field_name)
            error_type = err.get("type", "validation_error")
            error_msg = err.get("msg", "invalid value")
            field_info = cls.model_fields.get(field_name)
            input_value = err.get("input")

            is_missing_required = (
                error_type == "missing"
                or (
                    input_value is None
                    and field_info is not None
                    and field_info.is_required()
                    and not cls._is_optional_annotation(field_info.annotation)
                )
            )

            if is_missing_required:
                detail = f"missing required field '{field_name}'"
            else:
                detail = f"invalid field '{field_name}': {error_msg}"

            if column_letter:
                detail += f" (column {column_letter})"
            details.append(detail)

        return "; ".join(details) if details else str(error)

    @staticmethod
    def _is_optional_annotation(annotation: Any) -> bool:
        origin = getattr(annotation, "__origin__", None)
        args = getattr(annotation, "__args__", ())
        return origin is Optional or type(None) in args

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
            logging.warning(
                "Skipping row %s: validation error: %s",
                row_index,
                cls._format_validation_error(e),
            )
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
    ss1_check: Annotated[Optional[str], "AQ"] = None
    ss1_profit: Annotated[Optional[float], "AR"] = None
    ss1_hesonhan: Annotated[Optional[float], "AS"] = None
    ss1_quydoidonvi: Annotated[Optional[float], "AT"] = None
    ss1_idsheet_price: Annotated[Optional[str], "AU"] = None
    ss1_sheet_price: Annotated[Optional[str], "AV"] = None
    ss1_cell_price: Annotated[Optional[str], "AW"] = None
    ss2_check: Annotated[Optional[str], "AX"] = None
    ss2_profit: Annotated[Optional[float], "AY"] = None
    ss2_hesonhan: Annotated[Optional[float], "AZ"] = None
    ss2_quydoidonvi: Annotated[Optional[float], "BA"] = None
    ss2_idsheet_price: Annotated[Optional[str], "BB"] = None
    ss2_sheet_price: Annotated[Optional[str], "BC"] = None
    ss2_cell_price: Annotated[Optional[str], "BD"] = None
    ss3_check: Annotated[Optional[str], "BE"] = None
    ss3_profit: Annotated[Optional[float], "BF"] = None
    ss3_hesonhan: Annotated[Optional[float], "BG"] = None
    ss3_quydoidonvi: Annotated[Optional[float], "BH"] = None
    ss3_idsheet_price: Annotated[Optional[str], "BI"] = None
    ss3_sheet_price: Annotated[Optional[str], "BJ"] = None
    ss3_cell_price: Annotated[Optional[str], "BK"] = None
    ss4_check: Annotated[Optional[str], "BL"] = None
    ss4_profit: Annotated[Optional[float], "BM"] = None
    ss4_hesonhan: Annotated[Optional[float], "BN"] = None
    ss4_quydoidonvi: Annotated[Optional[float], "BO"] = None
    ss4_idsheet_price: Annotated[Optional[str], "BP"] = None
    ss4_sheet_price: Annotated[Optional[str], "BQ"] = None
    ss4_cell_price: Annotated[Optional[str], "BR"] = None

    # --- Runtime fields (NOT mapped to sheet, populated during processing) ---
    fetched_min_price: Optional[float] = None
    fetched_max_price: Optional[float] = None
    fetched_stock: Optional[int] = 999
    fetched_black_list: Optional[List[str]] = None
    fetched_ss1_price: Optional[float] = None
    fetched_ss2_price: Optional[float] = None
    fetched_ss3_price: Optional[float] = None
    fetched_ss4_price: Optional[float] = None
    offer_id: Optional[str] = None         # Resolved by adapter
    real_product_id: Optional[str] = None   # Resolved by adapter
    current_price: Optional[float] = None   # Fetched from marketplace
    applied_adj: Optional[float] = 0.0      # The random adjustment that was applied
    product_link: Optional[str] = None      # Future requirement-sheet compatibility
    exclude_keyword: Optional[str] = None   # Future requirement-sheet compatibility
    category_name: Optional[str] = None     # Future requirement-sheet compatibility
    game_name: Optional[str] = None         # Future requirement-sheet compatibility
    feedback_min: Optional[float] = None
    resolved_listing_id: Optional[str] = None
    resolved_listing_name: Optional[str] = None
    sheet_schema: str = "legacy"

    REQUIREMENT_HEADER_FIELD_ALIASES: ClassVar[Dict[str, str]] = {
        "2lai": "is_2lai_enabled_str",
        "check": "is_check_enabled_str",
        "productname": "product_name",
        "productpack": "parameters",
        "note": "note",
        "lastupdate": "last_update",
        "productid": "product_id",
        "productlink": "product_link",
        "compare": "is_compare_enabled_str",
        "comparemode": "is_compare_enabled_str",
        "productcompare": "product_compare",
        "includekeyword": "include_keyword",
        "excludekeyword": "exclude_keyword",
        "filteroptions": "filter_options",
        "category": "category_name",
        "game": "game_name",
        "dongiagiammin": "min_price_adjustment",
        "dongiagiammax": "max_price_adjustment",
        "dongialamtron": "price_rounding",
        "minadj": "min_price_adjustment",
        "maxadj": "max_price_adjustment",
        "rounding": "price_rounding",
        "feedback": "feedback_min",
        "idsheetmin": "idsheet_min",
        "sheetmin": "sheet_min",
        "cellmin": "cell_min",
        "idsheetmax": "idsheet_max",
        "sheetmax": "sheet_max",
        "cellmax": "cell_max",
        "idsheetstock": "idsheet_stock",
        "sheetstock": "sheet_stock",
        "cellstock": "cell_stock",
        "idsheetblacklist": "idsheet_blacklist",
        "sheetblacklist": "sheet_blacklist",
        "cellblacklist": "cell_blacklist",
        "ss1check": "ss1_check",
        "ss1profit": "ss1_profit",
        "ss1hesonhan": "ss1_hesonhan",
        "ss1quydoidonvi": "ss1_quydoidonvi",
        "ss1idsheetprice": "ss1_idsheet_price",
        "ss1sheetprice": "ss1_sheet_price",
        "ss1cellprice": "ss1_cell_price",
        "ss2check": "ss2_check",
        "ss2profit": "ss2_profit",
        "ss2hesonhan": "ss2_hesonhan",
        "ss2quydoidonvi": "ss2_quydoidonvi",
        "ss2idsheetprice": "ss2_idsheet_price",
        "ss2sheetprice": "ss2_sheet_price",
        "ss2cellprice": "ss2_cell_price",
        "ss3check": "ss3_check",
        "ss3profit": "ss3_profit",
        "ss3hesonhan": "ss3_hesonhan",
        "ss3quydoidonvi": "ss3_quydoidonvi",
        "ss3idsheetprice": "ss3_idsheet_price",
        "ss3sheetprice": "ss3_sheet_price",
        "ss3cellprice": "ss3_cell_price",
        "ss4check": "ss4_check",
        "ss4profit": "ss4_profit",
        "ss4hesonhan": "ss4_hesonhan",
        "ss4quydoidonvi": "ss4_quydoidonvi",
        "ss4idsheetprice": "ss4_idsheet_price",
        "ss4sheetprice": "ss4_sheet_price",
        "ss4cellprice": "ss4_cell_price",
        "relax": "relax",
        "minprice": "min_price",
    }

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
        "ss1_check": 42,
        "ss1_profit": 43,
        "ss1_hesonhan": 44,
        "ss1_quydoidonvi": 45,
        "ss1_idsheet_price": 46,
        "ss1_sheet_price": 47,
        "ss1_cell_price": 48,
        "ss2_check": 49,
        "ss2_profit": 50,
        "ss2_hesonhan": 51,
        "ss2_quydoidonvi": 52,
        "ss2_idsheet_price": 53,
        "ss2_sheet_price": 54,
        "ss2_cell_price": 55,
        "ss3_check": 56,
        "ss3_profit": 57,
        "ss3_hesonhan": 58,
        "ss3_quydoidonvi": 59,
        "ss3_idsheet_price": 60,
        "ss3_sheet_price": 61,
        "ss3_cell_price": 62,
        "ss4_check": 63,
        "ss4_profit": 64,
        "ss4_hesonhan": 65,
        "ss4_quydoidonvi": 66,
        "ss4_idsheet_price": 67,
        "ss4_sheet_price": 68,
        "ss4_cell_price": 69,
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
    def from_row_with_header(
        cls,
        row_data: List[str],
        row_index: int,
        header_row: List[str],
    ) -> Optional['Payload']:
        data_dict = cls._row_to_data_dict_with_header(row_data, header_row)
        if not any(data_dict.values()):
            return None
        data_dict["row_index"] = row_index

        try:
            return cls.model_validate(data_dict)
        except ValidationError as e:
            logging.warning(
                "Skipping row %s: validation error: %s",
                row_index,
                cls._format_validation_error(e),
            )
            return None

    @classmethod
    def _row_to_data_dict_with_header(cls, row_data: List[str], header_row: List[str]) -> Dict[str, Any]:
        header_map = cls._header_map(header_row)
        if not header_map:
            return cls._row_to_data_dict(row_data)

        data_dict: Dict[str, Any] = {}
        for idx, header_value in enumerate(header_row):
            field_name = header_map.get(cls._normalize_header_key(header_value))
            if not field_name or idx >= len(row_data):
                continue
            value = row_data[idx]
            data_dict[field_name] = value if value != '' else None

        if "product_link" in data_dict or "product_compare" in data_dict or "category_name" in data_dict:
            data_dict["product_id"] = (
                data_dict.get("product_compare")
                or data_dict.get("product_link")
                or data_dict.get("product_name")
            )
            data_dict["filter_options"] = data_dict.get("filter_options") or data_dict.get("exclude_keyword")
            data_dict["sheet_schema"] = "requirement"
        return data_dict

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

    @classmethod
    def _header_map(cls, header_row: List[str]) -> Dict[str, str]:
        mapped = {}
        for header_value in header_row:
            normalized = cls._normalize_header_key(header_value)
            field_name = cls.REQUIREMENT_HEADER_FIELD_ALIASES.get(normalized)
            if field_name:
                mapped[normalized] = field_name
        return mapped

    @staticmethod
    def _normalize_header_key(value: Optional[str]) -> str:
        return ''.join(ch for ch in (value or "").lower() if ch.isalnum())

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

    def ss_reference_sources(self) -> List[Dict[str, Any]]:
        sources: List[Dict[str, Any]] = []
        for idx in range(1, 5):
            sources.append({
                "label": f"SS{idx}",
                "enabled": getattr(self, f"ss{idx}_check", None) == "1",
                "profit": getattr(self, f"ss{idx}_profit", None),
                "multiplier": getattr(self, f"ss{idx}_hesonhan", None),
                "unit_factor": getattr(self, f"ss{idx}_quydoidonvi", None),
                "fetched_price": getattr(self, f"fetched_ss{idx}_price", None),
                "location": SheetLocation(
                    sheet_id=getattr(self, f"ss{idx}_idsheet_price", None),
                    sheet_name=getattr(self, f"ss{idx}_sheet_price", None),
                    cell=getattr(self, f"ss{idx}_cell_price", None),
                ),
            })
        return sources

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
