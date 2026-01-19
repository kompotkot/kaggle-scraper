import argparse
import csv
import json
import logging
import random
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

from kaggle.api.kaggle_api_extended import KaggleApi  # type: ignore[import-untyped]

from . import config, data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def search_kernels_handler(args: argparse.Namespace) -> None:
    """
    Search Kaggle kernels and save results to CSV file.

    Searches for kernels matching the search term, paginates through results,
    and saves them to a CSV file. Also maintains a memory.json file tracking
    all search operations.
    """
    api = KaggleApi()
    api.authenticate()

    output_file = (
        Path(args.out) / f"kernels-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Parse kernels memory JSON
    memory_json_path = Path(args.out) / "memory.json"

    if memory_json_path.exists():
        try:
            with open(memory_json_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    memory = data.Memory.model_validate(json.loads(content))
                else:
                    # File exists but is empty, create new memory
                    memory = data.Memory()
                    with open(memory_json_path, "w", encoding="utf-8") as f:
                        json.dump(memory.model_dump(), f, indent=4)
                    logger.info("Memory file was empty, created new memory JSON")
        except (json.JSONDecodeError, ValueError) as e:
            raise ValueError(
                f"Invalid JSON in memory file: {e}. Verify memory file integrity."
            )
    else:
        memory = data.Memory()
        with open(memory_json_path, "w", encoding="utf-8") as f:
            json.dump(memory.model_dump(), f, indent=4)
        logger.info("Created new memory JSON")

    page = 1
    page_size = 100
    fields = ["ref", "title", "author", "lastRunTime", "totalVotes"]
    header_written = False
    total_rows = 0

    try:
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            while True:
                results = api.kernels_list(
                    search=args.search,
                    page=page,
                    page_size=page_size,
                    sort_by="dateCreated",
                )

                if not results:
                    break

                # Write header only once at the start
                if not header_written:
                    writer.writerow(fields)
                    header_written = True

                # Write data rows for current page
                for kernel in results:
                    if kernel is None:
                        continue
                    row = []
                    for field in fields:
                        # Convert camelCase to snake_case for attribute access
                        attr_name = api.camel_to_snake(field)
                        value = getattr(kernel, attr_name, "")
                        row.append(api.string(value))
                    writer.writerow(row)
                    total_rows += 1

                logger.info(f"Page {page} done with {len(results)} results")

                # If we got fewer results than page_size, we've reached the last page
                if len(results) < page_size:
                    break

                page += 1

                sleep(random.uniform(0.5, 1.5))

        logger.info(f"Saved {total_rows} results to {output_file}")

        # Save search record to kernels.json
        search_record = data.SearchRecord(
            search_str=args.search,
            datetime=datetime.now().isoformat(),
            file_name=output_file.name,
            amount=total_rows,
        )
        memory.kernels.search.append(search_record)
        with open(memory_json_path, "w", encoding="utf-8") as f:
            json.dump(memory.model_dump(), f, indent=4)
        logger.info("Memory JSON file updated")
    except KeyboardInterrupt:
        logger.info(
            f"\nInterrupted by user. Saved {total_rows} results to {output_file}"
        )

        # Save partial search record to kernels.json
        if total_rows > 0:
            search_record = data.SearchRecord(
                search_str=args.search,
                datetime=datetime.now().isoformat(),
                file_name=output_file.name,
                amount=total_rows,
            )
            memory.kernels.search.append(search_record)
            with open(memory_json_path, "w", encoding="utf-8") as f:
                json.dump(memory.model_dump(), f, indent=4)
            logger.info("Memory JSON file updated")

        sys.exit(0)


def utils_test_handler(args: argparse.Namespace) -> None:
    """Test utility handler for debugging purposes."""
    if args.debug:
        logging.getLogger("src").setLevel(logging.DEBUG)

    logger.debug("Test")


def main() -> None:
    """
    Main entry point for the Kaggle scraper CLI.

    Sets up command line argument parsing and routes to appropriate handlers
    for search and utility commands.
    """
    parser = argparse.ArgumentParser(description="Scalper CLI")
    parser.set_defaults(func=lambda _: parser.print_help())
    subcommands = parser.add_subparsers(description="Scalper commands")

    # Search command parser
    parser_search = subcommands.add_parser("search", description="Scalper search")
    parser_search.set_defaults(func=lambda _: parser_search.print_help())
    subcommands_search = parser_search.add_subparsers(
        description="Scalper search commands"
    )

    parser_search_kernels = subcommands_search.add_parser(
        "kernels", description="Search kernels"
    )
    parser_search_kernels.add_argument(
        "-s",
        "--search",
        required=True,
        help="Term(s) to search for",
    )
    parser_search_kernels.add_argument(
        "-o",
        "--out",
        default=config.DEFAULT_DATA_DIR,
        help="Directory to save the results (default directory: out)",
    )
    parser_search_kernels.set_defaults(func=search_kernels_handler)

    # Util command parser
    parser_utils = subcommands.add_parser("utils", description="Scalper utils")
    parser_utils.set_defaults(func=lambda _: parser_utils.print_help())
    subcommands_utils = parser_utils.add_subparsers(description="Scalper util commands")

    parser_utils_test = subcommands_utils.add_parser(
        "test", description="For test purposes"
    )
    parser_utils_test.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Set this flag for debug",
    )
    parser_utils_test.set_defaults(func=utils_test_handler)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
