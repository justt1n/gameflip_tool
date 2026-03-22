import pytest
from models.sheet_models import _col_letter_to_index, Payload, BaseGSheetModel


class TestColLetterToIndex:
    def test_single_letters(self):
        assert _col_letter_to_index("A") == 0
        assert _col_letter_to_index("B") == 1
        assert _col_letter_to_index("Z") == 25

    def test_double_letters(self):
        assert _col_letter_to_index("AA") == 26
        assert _col_letter_to_index("AB") == 27
        assert _col_letter_to_index("AZ") == 51

    def test_case_insensitive(self):
        assert _col_letter_to_index("a") == 0
        assert _col_letter_to_index("aa") == 26


class TestPayloadFromRow:
    def _make_row(self, **overrides):
        """Build a 28-column row with defaults, applying overrides by column letter."""
        col_map = {
            "A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "F": 5,
            "G": 6, "H": 7, "I": 8, "J": 9, "K": 10, "L": 11,
            "M": 12, "N": 13, "O": 14, "P": 15, "Q": 16,
            "R": 17, "S": 18, "T": 19, "U": 20, "V": 21,
            "W": 22, "X": 23, "Y": 24, "Z": 25, "AA": 26, "AB": 27,
        }
        row = [""] * 28
        # Defaults: CHECK=1, product_name=TestProduct
        row[1] = "1"
        row[2] = "TestProduct"
        for letter, value in overrides.items():
            row[col_map[letter]] = value
        return row

    def test_from_row_parses_correct_columns(self):
        row = self._make_row(
            C="Game Key Pro",
            G="https://example.com/12345",
            H="1",
            L="0.01",
            M="0.05",
            N="2",
        )
        payload = Payload.from_row(row, row_index=5)
        assert payload is not None
        assert payload.product_name == "Game Key Pro"
        assert payload.product_id == "https://example.com/12345"
        assert payload.is_compare_enabled_str == "1"
        assert payload.min_price_adjustment == 0.01
        assert payload.max_price_adjustment == 0.05
        assert payload.price_rounding == 2
        assert payload.row_index == 5

    def test_from_row_empty_string_is_none(self):
        row = self._make_row(G="", H="", L="")
        payload = Payload.from_row(row, row_index=3)
        assert payload is not None
        assert payload.product_id is None
        assert payload.is_compare_enabled_str is None
        assert payload.min_price_adjustment is None

    def test_from_row_blank_row_returns_none(self):
        row = [""] * 28
        payload = Payload.from_row(row, row_index=10)
        assert payload is None

    def test_from_row_missing_required_field(self):
        """product_name (C) is required — blank should fail validation."""
        row = [""] * 28
        row[1] = "1"  # CHECK enabled but no product_name
        payload = Payload.from_row(row, row_index=10)
        assert payload is None  # Validation error → None

    def test_from_row_missing_required_field_logs_clear_message(self, caplog):
        row = [""] * 28
        row[1] = "1"

        with caplog.at_level("WARNING"):
            payload = Payload.from_row(row, row_index=10)

        assert payload is None
        assert "Skipping row 10: validation error:" in caplog.text
        assert "missing required field 'product_name'" in caplog.text
        assert "(column C)" in caplog.text

    def test_from_row_external_sheet_refs(self):
        row = self._make_row(
            O="spreadsheet_id_1",
            P="Prices",
            Q="B5",
            R="spreadsheet_id_2",
            S="MaxPrices",
            T="C5",
        )
        payload = Payload.from_row(row, row_index=4)
        assert payload is not None
        assert payload.min_price_location.sheet_id == "spreadsheet_id_1"
        assert payload.min_price_location.sheet_name == "Prices"
        assert payload.min_price_location.cell == "B5"
        assert payload.max_price_location.sheet_id == "spreadsheet_id_2"
        assert payload.max_price_location.sheet_name == "MaxPrices"
        assert payload.max_price_location.cell == "C5"

    def test_from_requirement_row_maps_gameflip_search_fields(self):
        row = [""] * 69
        row[0] = "TRUE"
        row[1] = "1"
        row[2] = "Blade Ball Row"
        row[5] = "5000 Tokens | Blade Ball"
        row[6] = "https://gameflip.com/shop/game-items?status=onsale&term=5000%20Token"
        row[7] = "5000 Token"
        row[8] = "Deluxe"
        row[9] = "Game Item"
        row[10] = "Blade Ball"
        row[11] = "0.001"
        row[12] = "0.002"
        row[13] = "3"
        row[17] = "sheet_min_id"
        row[18] = "MinSheet"
        row[19] = "A1"
        row[23] = "sheet_max_id"
        row[24] = "MaxSheet"
        row[25] = "B1"
        row[29] = "sheet_stock_id"
        row[30] = "StockSheet"
        row[31] = "C1"
        row[38] = "sheet_blacklist_id"
        row[39] = "BlacklistSheet"
        row[40] = "D1:D"

        payload = Payload.from_row(row, row_index=8)

        assert payload is not None
        assert payload.product_name == "Blade Ball Row"
        assert payload.product_link == "5000 Tokens | Blade Ball"
        assert payload.product_compare == "https://gameflip.com/shop/game-items?status=onsale&term=5000%20Token"
        assert payload.product_id == payload.product_compare
        assert payload.include_keyword == "5000 Token"
        assert payload.exclude_keyword == "Deluxe"
        assert payload.category_name == "Game Item"
        assert payload.game_name == "Blade Ball"
        assert payload.min_price_location.sheet_id == "sheet_min_id"
        assert payload.max_price_location.sheet_id == "sheet_max_id"
        assert payload.stock_location.sheet_id == "sheet_stock_id"
        assert payload.blacklist_location.sheet_id == "sheet_blacklist_id"

    def test_from_row_with_header_maps_new_requirement_layout(self):
        header = [
            "2LAI", "CHECK", "Product_name", "Note", "Last Update", "Product_link",
            "PRODUCT_COMPARE", "Compare mode", "INCLUDE_KEYWORD", "EXCLUDE_KEYWORD",
            "CATEGORY", "GAME", "DONGIAGIAM_MIN", "DONGIAGIAM_MAX", "DONGIA_LAMTRON",
            "FEEDBACK", "IDSheet_min", "Sheet_min", "Cell_min",
        ]
        row = [
            "TRUE", "1", "Blade Ball Row", "", "", "5000 Tokens | Blade Ball",
            "https://gameflip.com/shop/game-items?status=onsale&term=5000%20Token",
            "2", "5000 Token", "Deluxe", "Game Item", "Blade Ball",
            "0.001", "0.002", "3", "100", "sheet_min_id", "MinSheet", "A1",
        ]

        payload = Payload.from_row_with_header(row, row_index=8, header_row=header)

        assert payload is not None
        assert payload.sheet_schema == "requirement"
        assert payload.product_link == "5000 Tokens | Blade Ball"
        assert payload.product_compare == "https://gameflip.com/shop/game-items?status=onsale&term=5000%20Token"
        assert payload.is_compare_enabled_str == "2"
        assert payload.include_keyword == "5000 Token"
        assert payload.exclude_keyword == "Deluxe"
        assert payload.category_name == "Game Item"
        assert payload.game_name == "Blade Ball"
        assert payload.feedback_min == 100
        assert payload.min_price_adjustment == 0.001
        assert payload.max_price_adjustment == 0.002
        assert payload.price_rounding == 3

    def test_from_row_with_header_maps_ss_reference_fields(self):
        header = [
            "CHECK", "Product_name", "SS1_CHECK", "SS1_PROFIT", "SS1_HESONHAN",
            "SS1_QUYDOIDONVI", "SS1_IDSHEET_PRICE", "SS1_SHEET_PRICE", "SS1_CELL_PRICE",
        ]
        row = [
            "1", "Blade Ball Row", "1", "18", "0.85", "0.001",
            "sheet_ss1", "Prices", "A1",
        ]

        payload = Payload.from_row_with_header(row, row_index=8, header_row=header)

        assert payload is not None
        assert payload.ss1_check == "1"
        assert payload.ss1_profit == 18
        assert payload.ss1_hesonhan == 0.85
        assert payload.ss1_quydoidonvi == 0.001
        sources = payload.ss_reference_sources()
        assert sources[0]["location"].sheet_id == "sheet_ss1"
        assert sources[0]["location"].sheet_name == "Prices"
        assert sources[0]["location"].cell == "A1"


class TestPayloadPrepareUpdate:
    def test_prepare_update_generates_correct_ranges(self):
        row = [""] * 28
        row[1] = "1"
        row[2] = "TestProduct"
        payload = Payload.from_row(row, row_index=15)
        assert payload is not None

        updates = payload.prepare_update("MySheet", {
            "note": "Updated price",
            "last_update": "2026-02-28 10:30:00",
        })
        assert len(updates) == 2

        ranges = {u['range'] for u in updates}
        assert "MySheet!E15" in ranges  # note → col E
        assert "MySheet!F15" in ranges  # last_update → col F

        for u in updates:
            if u['range'] == "MySheet!E15":
                assert u['values'] == [["Updated price"]]
            if u['range'] == "MySheet!F15":
                assert u['values'] == [["2026-02-28 10:30:00"]]

    def test_prepare_update_unknown_field_skipped(self):
        row = [""] * 28
        row[1] = "1"
        row[2] = "TestProduct"
        payload = Payload.from_row(row, row_index=5)
        updates = payload.prepare_update("Sheet", {"nonexistent_field": "value"})
        assert updates == []

    def test_prepare_update_requirement_sheet_uses_d_e(self):
        row = [""] * 69
        row[1] = "1"
        row[2] = "TestProduct"
        payload = Payload.from_row(row, row_index=10)
        assert payload is not None
        assert payload.sheet_schema == "requirement"

        updates = payload.prepare_update("MySheet", {
            "note": "Updated price",
            "last_update": "2026-03-17 21:30:00",
        })

        assert updates == [
            {"range": "MySheet!D10", "values": [["Updated price"]]},
            {"range": "MySheet!E10", "values": [["2026-03-17 21:30:00"]]},
        ]


class TestPayloadProperties:
    def _make_payload(self, check="1", compare=None, min_price_str=None):
        row = [""] * 28
        row[1] = check
        row[2] = "TestProduct"
        if compare is not None:
            row[7] = compare
        if min_price_str is not None:
            row[27] = min_price_str
        return Payload.from_row(row, row_index=5)

    def test_is_check_enabled_true(self):
        p = self._make_payload(check="1")
        assert p.is_check_enabled is True

    def test_is_check_enabled_false(self):
        p = self._make_payload(check="0")
        assert p.is_check_enabled is False

    def test_is_check_enabled_none(self):
        p = self._make_payload(check="")
        # Empty string → None, which is not "1"
        assert p is not None
        assert p.is_check_enabled is False

    def test_compare_mode_0(self):
        p = self._make_payload(compare="0")
        assert p.compare_mode == 0

    def test_compare_mode_1(self):
        p = self._make_payload(compare="1")
        assert p.compare_mode == 1

    def test_compare_mode_2(self):
        p = self._make_payload(compare="2")
        assert p.compare_mode == 2

    def test_compare_mode_none(self):
        p = self._make_payload(compare=None)
        assert p.compare_mode == 0

    def test_get_min_price_value(self):
        p = self._make_payload(min_price_str="8.50")
        assert p.get_min_price_value() == 8.50

    def test_get_min_price_value_with_comma(self):
        p = self._make_payload(min_price_str="1,250.00")
        assert p.get_min_price_value() == 1250.00

    def test_get_min_price_value_none(self):
        p = self._make_payload(min_price_str=None)
        assert p.get_min_price_value() is None

    def test_get_min_price_value_invalid(self):
        p = self._make_payload(min_price_str="not_a_number")
        assert p.get_min_price_value() is None
