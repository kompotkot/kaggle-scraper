import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def utils_test_handler(args: argparse.Namespace) -> None:
    if args.debug:
        logging.getLogger("src").setLevel(logging.DEBUG)

    logger.debug("Test")


def main():
    parser = argparse.ArgumentParser(description="Scalper CLI")
    parser.set_defaults(func=lambda _: parser.print_help())
    subcommands = parser.add_subparsers(description="Scalper commands")

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
