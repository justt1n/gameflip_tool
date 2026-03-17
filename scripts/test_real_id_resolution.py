import argparse
import asyncio
import csv
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from auth.gameflip_auth import GameflipAuth
from clients.gameflip_client import GameflipClient
from core.gameflip_artifact_store import GameflipArtifactStore
from core.gameflip_listing_resolver import GameflipListingResolver
from models.sheet_models import Payload
from utils.config import settings


def build_payload_from_requirement_csv(csv_path: Path, row_index: int) -> Payload:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))

    if row_index < 1 or row_index > len(rows):
        raise ValueError(f"Row {row_index} is out of range for {csv_path}")

    row = rows[row_index - 1]
    payload = Payload.from_row(row, row_index=row_index)
    if payload is None:
        raise ValueError(f"Row {row_index} could not be parsed into a payload")
    return payload


def build_payload_from_inputs(
    product_compare: str = "",
    product_link: str = "",
    product_name: str = "",
    include_keyword: str = "",
    exclude_keyword: str = "",
    category_name: str = "",
) -> Payload:
    row = [""] * 28
    row[1] = "1"
    row[2] = product_name or "Manual Resolution Test"
    payload = Payload.from_row(row, row_index=1)
    if payload is None:
        raise ValueError("Failed to create manual payload")

    payload.product_compare = product_compare or None
    payload.product_link = product_link or None
    payload.product_name = product_name or payload.product_name
    payload.include_keyword = include_keyword or None
    payload.exclude_keyword = exclude_keyword or None
    payload.category_name = category_name or None
    payload.product_id = product_compare or product_link or product_name or None
    return payload


async def main():
    parser = argparse.ArgumentParser(
        description="Test real Gameflip ID resolution against live API."
    )
    parser.add_argument(
        "--csv",
        default=str(PROJECT_ROOT / "requireSheet.csv"),
        help="Requirement CSV path.",
    )
    parser.add_argument(
        "--row",
        type=int,
        help="1-based row index inside the requirement CSV to test.",
    )
    parser.add_argument(
        "--product-compare",
        default="",
        help="Direct PRODUCT_COMPARE value to test (for example a Gameflip search URL).",
    )
    parser.add_argument(
        "--product-link",
        default="",
        help="Direct Product_link value to test.",
    )
    parser.add_argument(
        "--product-name",
        default="",
        help="Direct Product_name value to test.",
    )
    parser.add_argument(
        "--include-keyword",
        default="",
        help="Direct INCLUDE_KEYWORD value to test.",
    )
    parser.add_argument(
        "--exclude-keyword",
        default="",
        help="Direct EXCLUDE_KEYWORD value to test.",
    )
    parser.add_argument(
        "--category",
        default="",
        help="Direct CATEGORY value to test.",
    )
    parser.add_argument(
        "--dump-output",
        default=settings.GAMEFLIP_LISTINGS_DUMP_FILE,
        help="Optional path to save the fetched owned listings dump.",
    )
    parser.add_argument(
        "--index-output",
        default=settings.GAMEFLIP_LISTINGS_INDEX_FILE,
        help="Optional path to save the owned listings index.",
    )
    parser.add_argument(
        "--no-save-dump",
        action="store_true",
        help="Do not overwrite the dump file after fetching owned listings.",
    )
    args = parser.parse_args()

    if args.row is None and not any([
        args.product_compare,
        args.product_link,
        args.product_name,
    ]):
        raise ValueError("Provide either --row or one of --product-compare/--product-link/--product-name")

    if args.row is not None:
        payload = build_payload_from_requirement_csv(Path(args.csv), args.row)
    else:
        payload = build_payload_from_inputs(
            product_compare=args.product_compare,
            product_link=args.product_link,
            product_name=args.product_name,
            include_keyword=args.include_keyword,
            exclude_keyword=args.exclude_keyword,
            category_name=args.category,
        )

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
        dump_path=args.dump_output,
        index_path=args.index_output,
    )
    resolver = GameflipListingResolver(
        artifact_store=artifact_store,
    )

    try:
        profile = await client.profile_get()
        listings = await client.list_owned_listings(owner_id=profile.owner)
        if not args.no_save_dump:
            artifact_store.save_owned_listings(listings)
            indexed_listings = artifact_store.load_owned_listings_index() or []
        else:
            indexed_listings = artifact_store.build_owned_listings_index(listings)

        definition = resolver.build_search_definition(payload)
        matches = resolver.match_owned_listings(definition, indexed_listings)

        print("=== OWNER ===")
        print(profile.owner)
        print()
        print("=== SEARCH DEFINITION ===")
        print(definition.model_dump_json(indent=2))
        print()
        print("=== OWNED LISTINGS FETCHED ===")
        print(len(listings))
        print()
        print("=== MATCHED LISTINGS ===")
        print(len(matches))
        for item in matches:
            print(json.dumps({
                "id": item.id,
                "name": item.name,
                "platform": item.platform,
                "category": item.category,
                "status": item.status,
                "tags": item.tags or [],
            }, ensure_ascii=True))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
