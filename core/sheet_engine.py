import logging
import re
from collections import defaultdict
from typing import List, Optional, Any, Tuple, Dict

from clients.google_sheets_client import GoogleSheetsClient
from models.sheet_models import Payload
from utils.config import Settings

logger = logging.getLogger(__name__)


class SheetEngine:
    """
    Orchestrates all sheet operations:
    1. get_payloads() — Read control sheet, auto-detect header, parse rows
    2. hydrate_payload() — Fetch external values (min, max, stock, blacklist)
    3. batch_write_logs() — Write processing results back to sheet
    """

    def __init__(self, client: GoogleSheetsClient, settings: Settings):
        self.client = client
        self.settings = settings

    # ─────────────────────────────────────────────
    # STEP 1: READ & PARSE
    # ─────────────────────────────────────────────

    def get_payloads(self) -> List[Payload]:
        """
        Read the control sheet and return a list of enabled Payload objects.

        ALGORITHM:
        1. Fetch ALL rows from the sheet tab.
        2. Scan rows top-down to find the header row.
        3. Data rows start at header_row + 1.
        4. Parse each data row into a Payload using from_row().
        5. Filter: only return rows where is_check_enabled == True.
        6. row_index is set to the 1-based sheet row number (for write-back).
        """
        all_rows = self.client.get_data(
            self.settings.MAIN_SHEET_ID,
            self.settings.MAIN_SHEET_NAME
        )
        if not all_rows:
            return []

        # Find header
        header_index = self._find_header_row(all_rows)
        if header_index is None:
            logger.error(
                f"Header row not found. Expected columns: {self.settings.HEADER_KEY_COLUMNS}"
            )
            return []

        # Parse data rows
        data_rows = all_rows[header_index + 1:]
        sheet_start_row = header_index + 2  # +1 for 0→1-based, +1 to skip header
        header_row = all_rows[header_index]

        payloads = []
        for i, row_data in enumerate(data_rows, start=sheet_start_row):
            if len(row_data) < 2 or row_data[1] not in {'0', '1'}:
                continue
            payload = Payload.from_row_with_header(row_data, row_index=i, header_row=header_row)
            if payload and payload.is_check_enabled:
                payloads.append(payload)

        return payloads

    def _find_header_row(self, rows: List[List[str]]) -> Optional[int]:
        """
        Scan rows top-down. Return the index of the first row that contains
        ALL of the HEADER_KEY_COLUMNS strings.
        """
        for i, row in enumerate(rows):
            if all(key in row for key in self.settings.HEADER_KEY_COLUMNS):
                return i
        return None

    # ─────────────────────────────────────────────
    # STEP 2: HYDRATE (fetch external values)
    # ─────────────────────────────────────────────

    def hydrate_payload(self, payload: Payload) -> Payload:
        """
        Fetch min_price, max_price, stock, and blacklist from external sheets.

        Groups requests by spreadsheet_id to minimize API calls.
        """
        locations = {
            "min_price": payload.min_price_location,
            "max_price": payload.max_price_location,
            "stock": payload.stock_location,
            "black_list": payload.blacklist_location,
        }
        for source in payload.ss_reference_sources():
            if source["enabled"]:
                locations[f"{source['label'].lower()}_price"] = source["location"]

        # Group ranges by spreadsheet ID
        requests_by_spreadsheet: Dict[str, List[str]] = defaultdict(list)
        range_to_key_map: Dict[str, str] = {}

        for key, loc in locations.items():
            if loc and loc.sheet_id and loc.sheet_name and loc.cell:
                range_name = f"'{loc.sheet_name}'!{loc.cell}"
                range_name = self._cap_unbounded_range(range_name)
                requests_by_spreadsheet[loc.sheet_id].append(range_name)
                range_to_key_map[range_name] = key

        # Fetch all in batched calls (one call per spreadsheet)
        for sheet_id, ranges in requests_by_spreadsheet.items():
            try:
                fetched = self.client.batch_get_data(sheet_id, ranges)
            except Exception as e:
                logger.error(f"Failed to fetch external data from {sheet_id}: {e}")
                continue

            for response_range, raw_value in fetched.items():
                # Try to match the response range to our request
                key = self._match_range_to_key(response_range, range_to_key_map)
                if not key:
                    continue
                processed = self._process_fetched_value(key, raw_value)
                if processed is not None:
                    setattr(payload, f"fetched_{key}", processed)

        return payload

    def _match_range_to_key(self, response_range: str, range_to_key_map: Dict[str, str]) -> Optional[str]:
        """
        Match a response range back to our request key.
        The API may return ranges in slightly different format than requested.
        """
        # Direct match
        if response_range in range_to_key_map:
            return range_to_key_map[response_range]

        # Try normalized comparison (strip quotes, compare)
        for req_range, key in range_to_key_map.items():
            req_norm = req_range.replace("'", "").upper()
            resp_norm = response_range.replace("'", "").upper()
            if req_norm == resp_norm:
                return key

        return None

    @staticmethod
    def _cap_unbounded_range(range_str: str, limit: int = 1000) -> str:
        """If range ends with a column letter (unbounded), add row limit."""
        if re.search(r":([A-Z]+)$", range_str, re.IGNORECASE):
            return f"{range_str}{limit}"
        return range_str

    @staticmethod
    def _process_fetched_value(key: str, raw_value: Any) -> Optional[Any]:
        """
        Convert raw sheet values into typed Python values.

        Rules:
        - None or '' → None
        - 'black_list' key → flatten into List[str]
        - 'stock' key → int
        - Everything else → float
        """
        if raw_value is None or raw_value == '':
            return None

        if key == 'black_list':
            if isinstance(raw_value, list):
                return [
                    item
                    for sublist in raw_value
                    for item in sublist
                    if item
                ]
            elif isinstance(raw_value, str):
                return [item.strip() for item in raw_value.split(',')]
            return [str(raw_value)]

        # For scalar values, extract from the nested list structure
        final_value = raw_value
        if isinstance(raw_value, list):
            if raw_value and raw_value[0]:
                final_value = raw_value[0][0]
            else:
                return None

        try:
            return int(final_value) if key == 'stock' else float(final_value)
        except (ValueError, TypeError):
            return None

    # ─────────────────────────────────────────────
    # STEP 3: WRITE LOGS
    # ─────────────────────────────────────────────

    def batch_write_logs(self, updates: List[Tuple[Payload, dict]]):
        """
        Write processing results back to the sheet.

        Args:
            updates: List of (payload, log_data) tuples.
                     log_data is a dict like {'note': '...', 'last_update': '...'}.

        All updates are aggregated into a SINGLE batchUpdate API call.
        """
        if not updates:
            return

        all_requests = []
        for payload, log_data in updates:
            reqs = payload.prepare_update(self.settings.MAIN_SHEET_NAME, log_data)
            if reqs:
                all_requests.extend(reqs)

        if all_requests:
            try:
                self.client.batch_update(self.settings.MAIN_SHEET_ID, all_requests)
            except Exception as e:
                logger.error(f"Failed to write logs to sheet: {e}")
