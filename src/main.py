import argparse
import csv
import logging
import random
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

from kaggle.api.kaggle_api_extended import KaggleApi

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def search_notebooks_handler(args: argparse.Namespace) -> None:
    api = KaggleApi()
    api.authenticate()

    output_file = (
        Path(args.out) / f"notebooks-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

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

                sleep(random.uniform(5.5, 10.5))

        logger.info(f"Saved {total_rows} results to {output_file}")
    except KeyboardInterrupt:
        logger.info(
            f"\nInterrupted by user. Saved {total_rows} results to {output_file}"
        )
        sys.exit(0)


def utils_test_handler(args: argparse.Namespace) -> None:
    if args.debug:
        logging.getLogger("src").setLevel(logging.DEBUG)

    logger.debug("Test")


def main():
    parser = argparse.ArgumentParser(description="Scalper CLI")
    parser.set_defaults(func=lambda _: parser.print_help())
    subcommands = parser.add_subparsers(description="Scalper commands")

    # Search command parser
    parser_search = subcommands.add_parser("search", description="Scalper search")
    parser_search.set_defaults(func=lambda _: parser_search.print_help())
    subcommands_search = parser_search.add_subparsers(
        description="Scalper search commands"
    )

    parser_search_notebooks = subcommands_search.add_parser(
        "notebooks", description="Search notebooks"
    )
    parser_search_notebooks.add_argument(
        "-s",
        "--search",
        required=True,
        help="Term(s) to search for",
    )
    parser_search_notebooks.add_argument(
        "-o",
        "--out",
        default="out",
        help="Directory to save the results (default directory: out)",
    )
    parser_search_notebooks.set_defaults(func=search_notebooks_handler)

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
