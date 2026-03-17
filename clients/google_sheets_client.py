import logging
from typing import List, Dict, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


class GoogleSheetsClient:
    """
    Thin wrapper around the Google Sheets API v4.
    Uses a Service Account for authentication.
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def __init__(self, key_path: str):
        creds = service_account.Credentials.from_service_account_file(
            key_path, scopes=self.SCOPES
        )
        self.service = build('sheets', 'v4', credentials=creds)

    def get_data(self, spreadsheet_id: str, range_name: str) -> List[List[str]]:
        """Read all rows from a sheet tab. Returns list of rows."""
        result = self.service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name
        ).execute()
        return result.get('values', [])

    def batch_get_data(self, spreadsheet_id: str, ranges: List[str]) -> Dict[str, Any]:
        """
        Fetch multiple cell ranges from ONE spreadsheet in a single API call.
        Returns: {normalized_range_string: cell_values}

        Uses valueRenderOption='UNFORMATTED_VALUE' to get raw numbers.
        Range normalization: strips quotes from sheet names so
        'Sheet1'!A1 and Sheet1!A1 map to the same key.
        """
        result = self.service.spreadsheets().values().batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=ranges,
            valueRenderOption='UNFORMATTED_VALUE'
        ).execute()

        value_map = {}
        for value_range in result.get('valueRanges', []):
            response_range = value_range.get('range', '')
            normalized_key = self._normalize_range(response_range)
            value_map[normalized_key] = value_range.get('values')
        return value_map

    def batch_update(self, spreadsheet_id: str, data: List[dict]):
        """
        Write multiple cell values in a single API call.
        data format: [{'range': 'Sheet!A1', 'values': [['value']]}]
        """
        if not data:
            return
        body = {'data': data, 'valueInputOption': 'USER_ENTERED'}
        self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()

    @staticmethod
    def _normalize_range(range_str: str) -> str:
        """
        Normalize a range string returned by the API.
        "'Sheet Name'!A1:B5" → "'Sheet Name'!A1:B5"
        Strips outer quotes from sheet name for consistent matching.
        """
        if '!' not in range_str:
            return range_str
        sheet_name, cell_range = range_str.split('!', 1)
        # Strip surrounding single quotes if present
        sheet_name = sheet_name.strip("'")
        return f"'{sheet_name}'!{cell_range}"

