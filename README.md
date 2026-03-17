# Gameflip Automation

Client-specific Gameflip repricing project derived from `marketplace_automation`. It keeps the original sheet-driven pricing engine and swaps in a dedicated Gameflip auth/client/adapter stack.

## Features

- **Python-first skeleton reuse** for sheet parsing, pricing modes, batching, and logging
- **Gameflip-specific integration** for TOTP auth, listing reads, competitor search, and price updates
- **3 compare modes** — No Compare (Mode 0), Always Follow (Mode 1), Smart Follow (Mode 2)
- **Automatic price clamping** with external min/max sheet references
- **Blacklist filtering** using normalized seller identifiers

## Quick Start

1. Create a virtualenv and install dependencies:
   ```bash
   cd gameflip_automation
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Configure `settings.env` with:
   - Google Sheets credentials
   - `GAMEFLIP_API_KEY`
   - `GAMEFLIP_API_SECRET`
   - Optional `GAMEFLIP_BASE_URL`, `GAMEFLIP_OWNER_ID`, `GAMEFLIP_RUNTIME_DATA_DIR`, `GAMEFLIP_LISTINGS_DUMP_PATH`, and `GAMEFLIP_LISTINGS_INDEX_PATH`

3. Build your owned listings dump first:
   ```bash
   python scripts/build_owned_listings_dump.py
   ```

4. Run:
   ```bash
   python main.py
   ```

## Notes

- V1 reprices existing Gameflip listings only.
- Framework prices are USD decimals; Gameflip API prices are integer cents.
- V1 intentionally uses listing price rather than subtracting `commission` or `digital_fee`.
- TODO after fee confirmation: migrate pricing logic from raw listing price to confirmed post-fee net proceeds.
- Runtime resolves your owned listing IDs from local artifacts in `runtime_data/`; refresh them with the build script when your listings change.
- Stable Gameflip taxonomy now lives in Python constants, not JSON runtime files.
- Main flow is now: hydrate sheet -> resolve from local artifacts -> prefetch live listing/competition data -> pricing engine -> update.

## Testing

```bash
python -m pytest tests/ -v
```

## Configuration Reference

| Setting | Required | Description |
|---------|----------|-------------|
| `MAIN_SHEET_ID` | Yes | Google Spreadsheet ID |
| `MAIN_SHEET_NAME` | Yes | Sheet tab name |
| `GOOGLE_KEY_PATH` | Yes | Path to service account JSON key |
| `HEADER_KEY_COLUMNS_JSON` | No | Header row detection keys |
| `WORKERS` | No | Concurrent tasks per batch |
| `SLEEP_TIME` | No | Seconds between rounds |
| `GAMEFLIP_API_KEY` | Yes | Gameflip API key |
| `GAMEFLIP_API_SECRET` | Yes | Gameflip TOTP secret |
| `GAMEFLIP_BASE_URL` | No | Override Gameflip API base URL |
| `GAMEFLIP_OWNER_ID` | No | Cache your owner ID to skip a profile lookup |
| `GAMEFLIP_RUNTIME_DATA_DIR` | No | Override the runtime data directory used for local artifacts |
| `GAMEFLIP_LISTINGS_DUMP_PATH` | No | Override the owned-listings JSON dump path |
| `GAMEFLIP_LISTINGS_INDEX_PATH` | No | Override the owned-listings index JSON path |
