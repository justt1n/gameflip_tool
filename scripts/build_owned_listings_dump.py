import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from auth.gameflip_auth import GameflipAuth
from clients.gameflip_client import GameflipClient
from core.gameflip_artifact_store import GameflipArtifactStore
from utils.config import settings


async def main():
    parser = argparse.ArgumentParser(description="Build owned Gameflip listings JSON dump.")
    parser.add_argument(
        "--output",
        default=settings.GAMEFLIP_LISTINGS_DUMP_FILE,
        help="Path to the JSON dump file.",
    )
    parser.add_argument(
        "--index-output",
        default=settings.GAMEFLIP_LISTINGS_INDEX_FILE,
        help="Path to the owned-listings index JSON file.",
    )
    args = parser.parse_args()

    auth = GameflipAuth(
        api_key=settings.GAMEFLIP_API_KEY,
        api_secret=settings.GAMEFLIP_API_SECRET,
    )
    client = GameflipClient(
        base_url=settings.GAMEFLIP_BASE_URL,
        auth_handler=auth,
        owner_id=settings.GAMEFLIP_OWNER_ID,
    )
    artifact_store = GameflipArtifactStore(
        dump_path=args.output,
        index_path=args.index_output,
    )
    try:
        listings = await client.list_owned_listings()
        artifact_store.save_owned_listings(listings)
        print(
            f"Wrote {len(listings)} listings to {args.output} "
            f"and {args.index_output}"
        )
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
