import asyncio
import logging
import signal
from typing import Dict

from adapters.gameflip_adapter import GameflipAdapter
from auth.gameflip_auth import GameflipAuth
from clients.gameflip_client import GameflipClient
from clients.google_sheets_client import GoogleSheetsClient
from core.competition_analyzer import CompetitionAnalyzer
from core.log_formatter import LogFormatter
from core.orchestrator import Orchestrator
from core.pricing_engine import PricingEngine
from core.sheet_engine import SheetEngine
from interfaces.marketplace_adapter import IMarketplaceAdapter
from utils.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    # 1. Create Google Sheets client
    sheets_client = GoogleSheetsClient(settings.GOOGLE_KEY_PATH)

    # 2. Create core engines
    sheet_engine = SheetEngine(sheets_client, settings)
    analyzer = CompetitionAnalyzer()
    log_formatter = LogFormatter()
    pricing_engine = PricingEngine(analyzer, log_formatter)

    # 3. Adapter registry
    gameflip_auth = GameflipAuth(
        api_key=settings.GAMEFLIP_API_KEY,
        api_secret=settings.GAMEFLIP_API_SECRET,
    )
    gameflip_client = GameflipClient(
        base_url=settings.GAMEFLIP_BASE_URL,
        auth_handler=gameflip_auth,
        owner_id=settings.GAMEFLIP_OWNER_ID,
    )
    adapter_registry: Dict[str, IMarketplaceAdapter] = {
        "gameflip": GameflipAdapter(
            gameflip_client,
            listings_dump_path=settings.GAMEFLIP_LISTINGS_DUMP_FILE,
            listings_index_path=settings.GAMEFLIP_LISTINGS_INDEX_FILE,
        ),
    }

    # 4. Create orchestrator
    orchestrator = Orchestrator(
        sheet_engine=sheet_engine,
        pricing_engine=pricing_engine,
        adapter_registry=adapter_registry,
        workers=settings.WORKERS,
        sleep_time=settings.SLEEP_TIME
    )

    # 5. Signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, orchestrator.stop)

    # 6. Run
    logger.info("Starting Gameflip automation...")
    try:
        await orchestrator.run_forever()
    finally:
        for adapter in adapter_registry.values():
            await adapter.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
