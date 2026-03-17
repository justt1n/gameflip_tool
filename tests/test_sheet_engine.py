import pytest
from core.sheet_engine import SheetEngine
from models.sheet_models import Payload
from utils.config import Settings
from tests.conftest import (
    HEADER_ROW, ROW_MODE1_FULL, ROW_MODE0_INLINE_MIN,
    ROW_DISABLED, ROW_EMPTY, ROW_NO_MIN_PRICE,
)


class MockGoogleSheetsClient:
    """Simulates GoogleSheetsClient with pre-loaded data."""
    def __init__(self, sheet_data=None, batch_data=None):
        self.sheet_data = sheet_data or []
        self.batch_data = batch_data or {}
        self.written_updates = []
        self.batch_get_calls = []

    def get_data(self, spreadsheet_id, range_name):
        return self.sheet_data

    def batch_get_data(self, spreadsheet_id, ranges):
        self.batch_get_calls.append({"spreadsheet_id": spreadsheet_id, "ranges": ranges})
        return self.batch_data

    def batch_update(self, spreadsheet_id, data):
        self.written_updates.extend(data)


@pytest.fixture
def settings():
    return Settings(
        MAIN_SHEET_ID="test_sheet_id",
        MAIN_SHEET_NAME="TestSheet",
        GOOGLE_KEY_PATH="creds.json",
        HEADER_KEY_COLUMNS_JSON='["CHECK", "Product_name", "Product_pack"]',
    )


class TestGetPayloads:
    def test_header_on_row3(self, full_sheet_data, settings):
        """Header at row 3 (after 2 junk rows). Data starts at row 4."""
        client = MockGoogleSheetsClient(sheet_data=full_sheet_data)
        engine = SheetEngine(client, settings)
        payloads = engine.get_payloads()
        # Expect enabled rows only: Mode1(row4), Mode0(row5), Mode2(row7), NoMin(row9)
        assert len(payloads) >= 3
        assert payloads[0].row_index == 4
        assert payloads[0].product_name == "Game Key Pro Edition"

    def test_filters_disabled_rows(self, full_sheet_data, settings):
        """Rows with CHECK=0 should be excluded."""
        client = MockGoogleSheetsClient(sheet_data=full_sheet_data)
        engine = SheetEngine(client, settings)
        payloads = engine.get_payloads()
        names = [p.product_name for p in payloads]
        assert "Disabled Product" not in names

    def test_skips_empty_rows(self, full_sheet_data, settings):
        """Completely empty rows should produce None and be excluded."""
        client = MockGoogleSheetsClient(sheet_data=full_sheet_data)
        engine = SheetEngine(client, settings)
        payloads = engine.get_payloads()
        assert all(p.product_name for p in payloads)

    def test_empty_sheet(self, settings):
        """Empty sheet returns []."""
        client = MockGoogleSheetsClient(sheet_data=[])
        engine = SheetEngine(client, settings)
        assert engine.get_payloads() == []

    def test_no_header_found(self, settings):
        """Sheet with no matching header returns []."""
        client = MockGoogleSheetsClient(sheet_data=[
            ["Random", "Data", "Here"],
            ["More", "Random", "Stuff"],
        ])
        engine = SheetEngine(client, settings)
        assert engine.get_payloads() == []

    def test_skips_comment_rows_with_non_binary_check_value(self, settings):
        settings = Settings(
            MAIN_SHEET_ID="test_sheet_id",
            MAIN_SHEET_NAME="TestSheet",
            GOOGLE_KEY_PATH="creds.json",
            HEADER_KEY_COLUMNS_JSON='["CHECK", "Product_name", "Note"]',
        )
        client = MockGoogleSheetsClient(sheet_data=[
            [" ", "CHECK", "Product_name", "Note"],
            [" ", "Comment about CHECK", "Comment name", ""],
            ["TRUE", "1", "Live Product", ""],
        ])
        engine = SheetEngine(client, settings)

        payloads = engine.get_payloads()

        assert len(payloads) == 1
        assert payloads[0].product_name == "Live Product"


class TestHydrate:
    def test_hydrate_min_max_price(self, settings, hydration_responses):
        """External min/max price refs resolve correctly."""
        client = MockGoogleSheetsClient(batch_data=hydration_responses)
        engine = SheetEngine(client, settings)

        p = Payload.from_row(ROW_MODE1_FULL, row_index=4)
        hydrated = engine.hydrate_payload(p)
        assert hydrated.fetched_min_price == 12.50
        assert hydrated.fetched_max_price == 18.00

    def test_hydrate_stock(self, settings, hydration_responses):
        """External stock ref resolves correctly."""
        client = MockGoogleSheetsClient(batch_data=hydration_responses)
        engine = SheetEngine(client, settings)

        p = Payload.from_row(ROW_MODE1_FULL, row_index=4)
        hydrated = engine.hydrate_payload(p)
        assert hydrated.fetched_stock == 42

    def test_hydrate_blacklist(self, settings, hydration_responses):
        """External blacklist resolves to list of seller names."""
        client = MockGoogleSheetsClient(batch_data=hydration_responses)
        engine = SheetEngine(client, settings)

        p = Payload.from_row(ROW_MODE1_FULL, row_index=4)
        hydrated = engine.hydrate_payload(p)
        assert hydrated.fetched_black_list is not None
        assert "CheapKeys" in hydrated.fetched_black_list
        assert "ShadySeller" in hydrated.fetched_black_list
        assert "BotStore99" in hydrated.fetched_black_list

    def test_hydrate_missing_refs(self, settings):
        """Missing sheet references → fields stay at default."""
        client = MockGoogleSheetsClient(batch_data={})
        engine = SheetEngine(client, settings)

        p = Payload.from_row(ROW_MODE0_INLINE_MIN, row_index=5)
        hydrated = engine.hydrate_payload(p)
        assert hydrated.fetched_min_price is None
        assert hydrated.fetched_max_price is None

    def test_hydrate_caps_unbounded_range(self, settings, hydration_responses):
        """Unbounded range A1:A becomes A1:A1000."""
        client = MockGoogleSheetsClient(batch_data=hydration_responses)
        engine = SheetEngine(client, settings)

        p = Payload.from_row(ROW_MODE1_FULL, row_index=4)
        engine.hydrate_payload(p)

        # Check that the range was capped
        assert len(client.batch_get_calls) > 0
        all_ranges = []
        for call in client.batch_get_calls:
            all_ranges.extend(call["ranges"])
        blacklist_ranges = [r for r in all_ranges if "Blacklist" in r]
        assert any("A1:A1000" in r for r in blacklist_ranges)


class TestBatchWriteLogs:
    def test_batch_write_logs(self, settings):
        """Write note + timestamp back to correct sheet cells."""
        client = MockGoogleSheetsClient()
        engine = SheetEngine(client, settings)

        p = Payload.from_row(ROW_MODE1_FULL, row_index=4)
        log_data = {"note": "Updated price", "last_update": "2026-02-28 10:30:00"}
        engine.batch_write_logs([(p, log_data)])

        assert len(client.written_updates) == 2
        ranges = {u['range'] for u in client.written_updates}
        assert "TestSheet!E4" in ranges  # note column
        assert "TestSheet!F4" in ranges  # last_update column

    def test_batch_write_logs_empty(self, settings):
        """No updates → no API call."""
        client = MockGoogleSheetsClient()
        engine = SheetEngine(client, settings)
        engine.batch_write_logs([])
        assert client.written_updates == []
