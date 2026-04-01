import json
from pathlib import Path
from typing import List, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).resolve().parents[1] / 'settings.env'


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding='utf-8',
        extra='ignore'
    )

    # Core (required)
    MAIN_SHEET_ID: str
    MAIN_SHEET_NAME: str
    GOOGLE_KEY_PATH: str
    HEADER_KEY_COLUMNS_JSON: str = '["CHECK", "Product_name", "Product_pack"]'
    WORKERS: int = 1
    TARGET_WORKERS: int = 1
    SLEEP_TIME: int = 5
    GAMEFLIP_API_KEY: str = ""
    GAMEFLIP_API_SECRET: str = ""
    GAMEFLIP_BASE_URL: str = "https://production-gameflip.fingershock.com/api/v1"
    GAMEFLIP_OWNER_ID: Optional[str] = None
    IS_GET_READY_PRODUCT: bool = False
    IS_SKIP_DIGITAL_GOODS_PUT: bool = False
    GAMEFLIP_COMPETITOR_FETCH_LIMIT: int = 15
    GAMEFLIP_SELLER_NAME_RESOLVE_LIMIT: int = 5
    GAMEFLIP_RUNTIME_DATA_DIR: Optional[str] = None
    GAMEFLIP_LISTINGS_DUMP_PATH: Optional[str] = None
    GAMEFLIP_LISTINGS_INDEX_PATH: Optional[str] = None

    @property
    def HEADER_KEY_COLUMNS(self) -> List[str]:
        return json.loads(self.HEADER_KEY_COLUMNS_JSON)

    @property
    def GAMEFLIP_RUNTIME_DIR(self) -> Path:
        if self.GAMEFLIP_RUNTIME_DATA_DIR:
            return Path(self.GAMEFLIP_RUNTIME_DATA_DIR)
        return Path(__file__).resolve().parents[1] / 'runtime_data'

    @property
    def GAMEFLIP_LISTINGS_DUMP_FILE(self) -> str:
        if self.GAMEFLIP_LISTINGS_DUMP_PATH:
            return self.GAMEFLIP_LISTINGS_DUMP_PATH
        return str(self.GAMEFLIP_RUNTIME_DIR / 'owned_listings_dump.json')

    @property
    def GAMEFLIP_LISTINGS_INDEX_FILE(self) -> str:
        if self.GAMEFLIP_LISTINGS_INDEX_PATH:
            return self.GAMEFLIP_LISTINGS_INDEX_PATH
        return str(self.GAMEFLIP_RUNTIME_DIR / 'owned_listings_index.json')


settings = Settings()
