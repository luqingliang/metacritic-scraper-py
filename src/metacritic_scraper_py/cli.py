from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path
from typing import Sequence

from .client import MetacriticClient
from .config import (
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_DELAY_SECONDS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
)
from .exporter import export_sqlite_to_excel
from .scraper import MetacriticScraper
from .storage import SQLiteStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="metacritic-scraper",
        description="Scrape Metacritic game data into SQLite.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logs.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl = subparsers.add_parser("crawl", help="Crawl games from games sitemap.")
    crawl.add_argument("--db", default="data/metacritic.db", help="SQLite db path.")
    crawl.add_argument("--max-games", type=int, default=None, help="Stop after N games.")
    crawl.add_argument("--start-slug", default=None, help="Start crawling when this slug is reached (sitemap mode).")
    crawl.add_argument("--limit-sitemaps", type=int, default=None, help="Read only first N sitemap files (sitemap mode).")
    crawl.add_argument("--limit-slugs", type=int, default=None, help="Read only first N slugs from sitemap (sitemap mode).")
    crawl.add_argument("--include-reviews", action="store_true", help="Also crawl critic and user reviews.")
    crawl.add_argument("--review-page-size", type=int, default=50, help="Reviews page size.")
    crawl.add_argument("--max-review-pages", type=int, default=None, help="Limit review pages per type.")
    crawl.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Optional concurrent slug workers (default: 1).",
    )
    crawl.add_argument(
        "--incremental-by-date",
        action="store_true",
        help="Enable incremental crawl by releaseDate (finder endpoint).",
    )
    crawl.add_argument(
        "--since-date",
        default=None,
        help="Override incremental start date in YYYY-MM-DD format.",
    )
    crawl.add_argument(
        "--lookback-days",
        type=int,
        default=3,
        help="Re-crawl this many days before checkpoint for safety.",
    )
    crawl.add_argument(
        "--finder-page-size",
        type=int,
        default=24,
        help="Page size when incremental-by-date is enabled.",
    )
    crawl.add_argument(
        "--incremental-state-key",
        default="games_incremental_release_date",
        help="DB state key used to store incremental checkpoint date.",
    )
    crawl.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds.")
    crawl.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Retry attempts.")
    crawl.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_SECONDS, help="Retry backoff base seconds.")
    crawl.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Sleep between requests.")

    crawl_one = subparsers.add_parser("crawl-one", help="Crawl one game by slug.")
    crawl_one.add_argument("slug", help="Game slug.")
    crawl_one.add_argument("--db", default="data/metacritic.db", help="SQLite db path.")
    crawl_one.add_argument("--include-reviews", action="store_true", help="Also crawl critic and user reviews.")
    crawl_one.add_argument("--review-page-size", type=int, default=50, help="Reviews page size.")
    crawl_one.add_argument("--max-review-pages", type=int, default=None, help="Limit review pages per type.")
    crawl_one.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds.")
    crawl_one.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Retry attempts.")
    crawl_one.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_SECONDS, help="Retry backoff base seconds.")
    crawl_one.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Sleep between requests.")

    slugs = subparsers.add_parser("slugs", help="Extract game slugs from games sitemap.")
    slugs.add_argument("--limit-sitemaps", type=int, default=None, help="Read only first N sitemap files.")
    slugs.add_argument("--limit-slugs", type=int, default=None, help="Output only first N slugs.")
    slugs.add_argument("--output", default=None, help="Write slugs to file instead of stdout.")
    slugs.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds.")
    slugs.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Retry attempts.")
    slugs.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_SECONDS, help="Retry backoff base seconds.")
    slugs.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Sleep between requests.")

    export_excel = subparsers.add_parser(
        "export-excel",
        help="Export crawled SQLite data to an Excel file.",
    )
    export_excel.add_argument("--db", default="data/metacritic.db", help="SQLite db path.")
    export_excel.add_argument(
        "--output",
        default="data/metacritic_export.xlsx",
        help="Output Excel file path.",
    )
    export_excel.add_argument(
        "--slug",
        default=None,
        help="Optional slug filter to export one game and its reviews.",
    )
    export_excel.add_argument(
        "--include-raw-json",
        action="store_true",
        help="Include raw JSON columns in Excel sheets.",
    )

    return parser


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    if not verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)


def _build_client(args: argparse.Namespace) -> MetacriticClient:
    return MetacriticClient(
        timeout_seconds=args.timeout,
        max_retries=args.max_retries,
        backoff_seconds=args.backoff,
        delay_seconds=args.delay,
    )


def run_crawl(args: argparse.Namespace) -> int:
    if args.since_date:
        try:
            date.fromisoformat(args.since_date)
        except ValueError as exc:
            raise SystemExit("--since-date must be in YYYY-MM-DD format") from exc
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1")

    storage = SQLiteStorage(args.db)
    try:
        with _build_client(args) as client:
            scraper = MetacriticScraper(client, storage)
            if args.incremental_by_date:
                result = scraper.crawl_incremental_by_date(
                    include_reviews=args.include_reviews,
                    review_page_size=args.review_page_size,
                    max_review_pages=args.max_review_pages,
                    max_games=args.max_games,
                    since_date=args.since_date,
                    lookback_days=args.lookback_days,
                    finder_page_size=args.finder_page_size,
                    state_key=args.incremental_state_key,
                    concurrency=args.concurrency,
                )
            else:
                result = scraper.crawl_from_sitemaps(
                    include_reviews=args.include_reviews,
                    review_page_size=args.review_page_size,
                    max_review_pages=args.max_review_pages,
                    max_games=args.max_games,
                    start_slug=args.start_slug,
                    limit_sitemaps=args.limit_sitemaps,
                    limit_slugs=args.limit_slugs,
                    concurrency=args.concurrency,
                )
        logging.info(
            "crawl finished games=%d critic_reviews=%d user_reviews=%d failed=%d",
            result.games_crawled,
            result.critic_reviews_saved,
            result.user_reviews_saved,
            len(result.failed_slugs),
        )
        if result.failed_slugs:
            logging.warning("failed slugs: %s", ",".join(result.failed_slugs))
        return 0
    finally:
        storage.close()


def run_crawl_one(args: argparse.Namespace) -> int:
    storage = SQLiteStorage(args.db)
    try:
        with _build_client(args) as client:
            scraper = MetacriticScraper(client, storage)
            result = scraper.crawl_slug(
                args.slug,
                include_reviews=args.include_reviews,
                review_page_size=args.review_page_size,
                max_review_pages=args.max_review_pages,
            )
        logging.info(
            "crawl-one finished games=%d critic_reviews=%d user_reviews=%d failed=%d",
            result.games_crawled,
            result.critic_reviews_saved,
            result.user_reviews_saved,
            len(result.failed_slugs),
        )
        return 0 if not result.failed_slugs else 2
    finally:
        storage.close()


def run_slugs(args: argparse.Namespace) -> int:
    with _build_client(args) as client:
        slugs = client.iter_game_slugs(
            limit_sitemaps=args.limit_sitemaps,
            limit_slugs=args.limit_slugs,
        )
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("w", encoding="utf-8") as f:
                for slug in slugs:
                    f.write(slug + "\n")
            logging.info("wrote slugs to %s", output_path)
        else:
            for slug in slugs:
                print(slug)
    return 0


def run_export_excel(args: argparse.Namespace) -> int:
    counts = export_sqlite_to_excel(
        db_path=args.db,
        output_path=args.output,
        slug=args.slug,
        include_raw_json=args.include_raw_json,
    )
    logging.info(
        "excel exported to %s games=%d critic_reviews=%d user_reviews=%d",
        args.output,
        counts["games_rows"],
        counts["critic_reviews_rows"],
        counts["user_reviews_rows"],
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    if args.command == "crawl":
        return run_crawl(args)
    if args.command == "crawl-one":
        return run_crawl_one(args)
    if args.command == "slugs":
        return run_slugs(args)
    if args.command == "export-excel":
        return run_export_excel(args)
    parser.error(f"unknown command: {args.command}")
    return 2
