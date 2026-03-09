from __future__ import annotations

import argparse
import io
import logging
import os
import re
import shlex
import sys
import threading
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path
from typing import Callable, Sequence

from .client import MetacriticClient
from .config import (
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_DELAY_SECONDS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
)
from .cover_downloader import CoverImageDownloader
from .exporter import export_sqlite_to_excel
from .scraper import MetacriticScraper
from .storage import SQLiteStorage

DEFAULT_QUICKSTART_MAX_GAMES = 50
DEFAULT_QUICKSTART_MAX_REVIEW_PAGES = 1
INTERACTIVE_COMPOSER_MIN_LINES = 3
INTERACTIVE_COMPOSER_MAX_LINES = 8
INTERACTIVE_WELCOME_CONTENT_WIDTH = 74
INTERACTIVE_WELCOME_LABEL_WIDTH = 28
INTERACTIVE_WELCOME_TITLE = "METACRITIC SCRAPER"
LOG_BULLET = "●"
LOG_FORMAT = f"{LOG_BULLET} %(levelname)s %(name)s - %(message)s"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="metacritic-scraper",
        description="Scrape Metacritic game data into SQLite.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logs.")

    subparsers = parser.add_subparsers(dest="command", required=False)

    crawl = subparsers.add_parser("crawl", help="Crawl games from games sitemap.")
    crawl.add_argument("--db", default="data/metacritic.db", help="SQLite db path.")
    crawl.add_argument(
        "--max-games",
        type=int,
        default=DEFAULT_QUICKSTART_MAX_GAMES,
        help=f"Stop after N games (default: {DEFAULT_QUICKSTART_MAX_GAMES}).",
    )
    crawl.add_argument("--start-slug", default=None, help="Start crawling when this slug is reached (sitemap mode).")
    crawl.add_argument("--limit-sitemaps", type=int, default=None, help="Read only first N sitemap files (sitemap mode).")
    crawl.add_argument("--limit-slugs", type=int, default=None, help="Read only first N slugs from sitemap (sitemap mode).")
    crawl.add_argument(
        "--include-reviews",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also crawl critic and user reviews (default: true).",
    )
    crawl.add_argument("--review-page-size", type=int, default=50, help="Reviews page size.")
    crawl.add_argument(
        "--max-review-pages",
        type=int,
        default=DEFAULT_QUICKSTART_MAX_REVIEW_PAGES,
        help=f"Limit review pages per type (default: {DEFAULT_QUICKSTART_MAX_REVIEW_PAGES}).",
    )
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
    crawl.add_argument(
        "--download-covers",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Download cover image files while crawling games (default: false).",
    )
    crawl.add_argument(
        "--covers-dir",
        default="data/covers",
        help="Output directory for downloaded cover image files.",
    )
    crawl.add_argument(
        "--overwrite-covers",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Overwrite existing cover files when downloading (default: false).",
    )

    crawl_one = subparsers.add_parser("crawl-one", help="Crawl one game by slug.")
    crawl_one.add_argument("slug", help="Game slug.")
    crawl_one.add_argument("--db", default="data/metacritic.db", help="SQLite db path.")
    crawl_one.add_argument(
        "--include-reviews",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also crawl critic and user reviews (default: true).",
    )
    crawl_one.add_argument("--review-page-size", type=int, default=50, help="Reviews page size.")
    crawl_one.add_argument(
        "--max-review-pages",
        type=int,
        default=DEFAULT_QUICKSTART_MAX_REVIEW_PAGES,
        help=f"Limit review pages per type (default: {DEFAULT_QUICKSTART_MAX_REVIEW_PAGES}).",
    )
    crawl_one.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds.")
    crawl_one.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Retry attempts.")
    crawl_one.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_SECONDS, help="Retry backoff base seconds.")
    crawl_one.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Sleep between requests.")
    crawl_one.add_argument(
        "--download-covers",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Download cover image files while crawling this game (default: false).",
    )
    crawl_one.add_argument(
        "--covers-dir",
        default="data/covers",
        help="Output directory for downloaded cover image files.",
    )
    crawl_one.add_argument(
        "--overwrite-covers",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Overwrite existing cover files when downloading (default: false).",
    )

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

    download_covers = subparsers.add_parser(
        "download-covers",
        help="Download cover image files from existing games in SQLite.",
    )
    download_covers.add_argument("--db", default="data/metacritic.db", help="SQLite db path.")
    download_covers.add_argument(
        "--output-dir",
        default="data/covers",
        help="Output directory for downloaded cover image files.",
    )
    download_covers.add_argument(
        "--slug",
        default=None,
        help="Optional slug filter to download only one game's cover.",
    )
    download_covers.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Download at most N covers.",
    )
    download_covers.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Overwrite existing cover files (default: false).",
    )
    download_covers.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds.")
    download_covers.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Retry attempts.")
    download_covers.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_SECONDS, help="Retry backoff base seconds.")
    download_covers.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Sleep between requests.")

    subparsers.add_parser(
        "interactive",
        help="Run interactive shell (persistent session).",
    )

    return parser


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
    )
    for handler in logging.getLogger().handlers:
        handler.terminator = "\n\n"
    if not verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)


def _get_stop_event(args: argparse.Namespace) -> threading.Event | None:
    stop_event = getattr(args, "stop_event", None)
    return stop_event if isinstance(stop_event, threading.Event) else None


def _check_stop_requested(stop_event: threading.Event | None) -> None:
    if stop_event is not None and stop_event.is_set():
        raise InterruptedError("stopped by user")


def _build_client(args: argparse.Namespace) -> MetacriticClient:
    return MetacriticClient(
        timeout_seconds=args.timeout,
        max_retries=args.max_retries,
        backoff_seconds=args.backoff,
        delay_seconds=args.delay,
        stop_event=_get_stop_event(args),
    )


def run_crawl(args: argparse.Namespace) -> int:
    if args.since_date:
        try:
            date.fromisoformat(args.since_date)
        except ValueError as exc:
            raise SystemExit("--since-date must be in YYYY-MM-DD format") from exc
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1")
    download_covers = bool(getattr(args, "download_covers", False))
    covers_dir = str(getattr(args, "covers_dir", "data/covers"))
    overwrite_covers = bool(getattr(args, "overwrite_covers", False))
    stop_event = _get_stop_event(args)

    storage = SQLiteStorage(args.db)
    try:
        with _build_client(args) as client:
            scraper = MetacriticScraper(client, storage, stop_event=stop_event)
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
                    download_covers=download_covers,
                    covers_dir=covers_dir,
                    overwrite_covers=overwrite_covers,
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
                    download_covers=download_covers,
                    covers_dir=covers_dir,
                    overwrite_covers=overwrite_covers,
                )
        logging.info(
            (
                "crawl %s games=%d critic_reviews=%d user_reviews=%d "
                "covers_downloaded=%d covers_skipped=%d covers_failed=%d failed=%d"
            ),
            "stopped" if result.stopped else "finished",
            result.games_crawled,
            result.critic_reviews_saved,
            result.user_reviews_saved,
            result.covers_downloaded,
            result.covers_skipped,
            result.covers_failed,
            len(result.failed_slugs),
        )
        if bool(getattr(args, "print_summary", False)):
            summary = (
                (
                    "crawl summary: games=%d critic_reviews=%d user_reviews=%d "
                    "covers_downloaded=%d covers_skipped=%d covers_failed=%d failed=%d"
                )
                % (
                    result.games_crawled,
                    result.critic_reviews_saved,
                    result.user_reviews_saved,
                    result.covers_downloaded,
                    result.covers_skipped,
                    result.covers_failed,
                    len(result.failed_slugs),
                )
            )
            if result.stopped:
                summary = f"{summary} stopped=1"
            print(summary)
        if result.failed_slugs:
            logging.warning("failed slugs: %s", ",".join(result.failed_slugs))
        return 130 if result.stopped else 0
    finally:
        storage.close()


def run_crawl_one(args: argparse.Namespace) -> int:
    download_covers = bool(getattr(args, "download_covers", False))
    covers_dir = str(getattr(args, "covers_dir", "data/covers"))
    overwrite_covers = bool(getattr(args, "overwrite_covers", False))
    stop_event = _get_stop_event(args)

    storage = SQLiteStorage(args.db)
    try:
        with _build_client(args) as client:
            scraper = MetacriticScraper(client, storage, stop_event=stop_event)
            cover_downloader = (
                CoverImageDownloader(
                    fetch_binary=client.fetch_binary,
                    output_dir=covers_dir,
                    overwrite=overwrite_covers,
                )
                if download_covers
                else None
            )
            result = scraper.crawl_slug(
                args.slug,
                include_reviews=args.include_reviews,
                review_page_size=args.review_page_size,
                max_review_pages=args.max_review_pages,
                cover_downloader=cover_downloader,
            )
        logging.info(
            (
                "crawl-one %s games=%d critic_reviews=%d user_reviews=%d "
                "covers_downloaded=%d covers_skipped=%d covers_failed=%d failed=%d"
            ),
            "stopped" if result.stopped else "finished",
            result.games_crawled,
            result.critic_reviews_saved,
            result.user_reviews_saved,
            result.covers_downloaded,
            result.covers_skipped,
            result.covers_failed,
            len(result.failed_slugs),
        )
        if bool(getattr(args, "print_summary", False)):
            summary = (
                (
                    "crawl-one summary: games=%d critic_reviews=%d user_reviews=%d "
                    "covers_downloaded=%d covers_skipped=%d covers_failed=%d failed=%d"
                )
                % (
                    result.games_crawled,
                    result.critic_reviews_saved,
                    result.user_reviews_saved,
                    result.covers_downloaded,
                    result.covers_skipped,
                    result.covers_failed,
                    len(result.failed_slugs),
                )
            )
            if result.stopped:
                summary = f"{summary} stopped=1"
            print(summary)
        if result.stopped:
            return 130
        return 0 if not result.failed_slugs else 2
    finally:
        storage.close()


def run_slugs(args: argparse.Namespace) -> int:
    stop_event = _get_stop_event(args)
    with _build_client(args) as client:
        try:
            slugs = client.iter_game_slugs(
                limit_sitemaps=args.limit_sitemaps,
                limit_slugs=args.limit_slugs,
            )
            if args.output:
                output_path = Path(args.output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("w", encoding="utf-8") as f:
                    for slug in slugs:
                        _check_stop_requested(stop_event)
                        f.write(slug + "\n")
                logging.info("wrote slugs to %s", output_path)
            else:
                for slug in slugs:
                    _check_stop_requested(stop_event)
                    print(slug)
        except InterruptedError:
            logging.info("slugs stopped by user")
            return 130
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


def run_download_covers(args: argparse.Namespace) -> int:
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be >= 1")

    stop_event = _get_stop_event(args)
    storage = SQLiteStorage(args.db)
    try:
        rows = storage.list_game_cover_urls(slug=args.slug, limit=args.limit)
        with _build_client(args) as client:
            downloader = CoverImageDownloader(
                fetch_binary=client.fetch_binary,
                output_dir=args.output_dir,
                overwrite=args.overwrite,
            )
            downloaded = 0
            skipped = 0
            failed = 0
            try:
                for slug, cover_url in rows:
                    _check_stop_requested(stop_event)
                    status = downloader.download(slug=slug, cover_url=cover_url)
                    if status == "downloaded":
                        downloaded += 1
                    elif status == "skipped":
                        skipped += 1
                    else:
                        failed += 1
            except InterruptedError:
                logging.info(
                    "download-covers stopped total=%d downloaded=%d skipped=%d failed=%d output_dir=%s",
                    len(rows),
                    downloaded,
                    skipped,
                    failed,
                    args.output_dir,
                )
                return 130

        logging.info(
            "download-covers finished total=%d downloaded=%d skipped=%d failed=%d output_dir=%s",
            len(rows),
            downloaded,
            skipped,
            failed,
            args.output_dir,
        )
        return 0 if failed == 0 else 2
    finally:
        storage.close()


def _interactive_defaults() -> dict[str, object]:
    return {
        "db": "data/metacritic.db",
        "max_games": DEFAULT_QUICKSTART_MAX_GAMES,
        "start_slug": None,
        "limit_sitemaps": None,
        "limit_slugs": None,
        "include_reviews": True,
        "review_page_size": 50,
        "max_review_pages": DEFAULT_QUICKSTART_MAX_REVIEW_PAGES,
        "concurrency": 1,
        "incremental_by_date": False,
        "since_date": None,
        "lookback_days": 3,
        "finder_page_size": 24,
        "incremental_state_key": "games_incremental_release_date",
        "timeout": DEFAULT_TIMEOUT_SECONDS,
        "max_retries": DEFAULT_MAX_RETRIES,
        "backoff": DEFAULT_BACKOFF_SECONDS,
        "delay": DEFAULT_DELAY_SECONDS,
        "download_covers": False,
        "covers_dir": "data/covers",
        "overwrite_covers": False,
        "export_output": "data/metacritic_export.xlsx",
        "slug_filter": None,
        "include_raw_json": False,
    }


def _parse_bool(raw: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError("expected boolean value (true/false)")


def _convert_setting_value(key: str, raw_value: str) -> object:
    bool_keys = {"include_reviews", "incremental_by_date", "include_raw_json", "download_covers", "overwrite_covers"}
    int_keys = {
        "review_page_size",
        "concurrency",
        "lookback_days",
        "finder_page_size",
        "max_retries",
    }
    float_keys = {"timeout", "backoff", "delay"}
    optional_int_keys = {"max_games", "max_review_pages", "limit_sitemaps", "limit_slugs"}
    optional_str_keys = {"start_slug", "since_date", "slug_filter"}

    value = raw_value.strip()
    if key in bool_keys:
        return _parse_bool(value)
    if key in int_keys:
        return int(value)
    if key in float_keys:
        return float(value)
    if key in optional_int_keys:
        return None if value.lower() in {"none", "null", ""} else int(value)
    if key in optional_str_keys:
        return None if value.lower() in {"none", "null", ""} else value
    if key in {"db", "incremental_state_key", "export_output", "covers_dir"}:
        return value
    raise KeyError(f"unknown setting key: {key}")


def _print_interactive_help() -> str:
    lines = [
        "Interactive commands:",
        "  help                              Show help",
        "  help-zh | 帮助                    Show Chinese annotated help",
        "  show-zh | 配置                    Show settings with Chinese explanations",
        "  show                              Show settings with English explanations",
        "  set <key> <value>                 Update setting (use 'none' for null)",
        "  reset                             Reset settings to defaults",
        "  stop                              Request stop for the current background crawl/download task",
        "  crawl                             Run crawl with current settings",
        "  crawl-one <slug>                  Crawl one game with current settings",
        "  download-covers [output_dir]      Download cover image files from DB",
        "  slugs [output_path]               Print slugs or save to a file",
        "  export-excel [output_path]        Export DB data to Excel",
        "  exit | quit                       Exit interactive shell",
        "",
        "Examples:",
        "  set db data/metacritic.db",
        "  set include_reviews true",
        "  set concurrency 4",
        "  set download_covers true",
        "  set incremental_by_date true",
        "  crawl",
        "  download-covers",
        "  crawl-one the-legend-of-zelda-breath-of-the-wild",
    ]
    return "\n".join(lines)


def _print_interactive_help_zh() -> str:
    lines = [
        "交互命令（中文释义）:",
        "  help                              显示英文帮助",
        "  help-zh | 帮助                    显示中文释义帮助",
        "  show-zh | 配置                    显示带中文说明的参数列表",
        "  show                              显示带英文说明的参数列表",
        "  set <key> <value>                 修改配置（null/none 表示空值）",
        "  reset                             重置为默认配置",
        "  stop                              请求停止当前后台抓取/下载任务",
        "  crawl                             用当前配置执行批量抓取",
        "  crawl-one <slug>                  抓取单个游戏",
        "  download-covers [output_dir]      基于已抓取数据下载封面图片实体",
        "  slugs [output_path]               打印 slug 或写入文件",
        "  export-excel [output_path]        导出 SQLite 数据到 Excel",
        "  exit | quit                       退出交互模式",
        "",
        "示例:",
        "  help-zh",
        "  set include_reviews true",
        "  set concurrency 4",
        "  crawl",
        "  download-covers",
        "  export-excel data/metacritic_export.xlsx",
    ]
    return "\n".join(lines)


def _setting_explanations_en() -> dict[str, str]:
    return {
        "backoff": "Retry backoff base; larger values increase wait time growth on retries",
        "concurrency": "Number of concurrent crawl workers (1 means serial)",
        "covers_dir": "Output directory for downloaded cover image files",
        "db": "Path to the SQLite database file",
        "delay": "Fixed delay in seconds between requests",
        "download_covers": "Whether to download cover image files during crawl",
        "export_output": "Default output path for Excel export",
        "finder_page_size": "Page size for incremental-by-date finder mode",
        "include_raw_json": "Whether to include raw JSON columns when exporting",
        "include_reviews": "Whether to crawl critic and user reviews",
        "incremental_by_date": "Enable incremental crawling ordered by release date",
        "incremental_state_key": "DB state key used to persist incremental checkpoint",
        "limit_sitemaps": "Maximum number of sitemap files to read in full mode",
        "limit_slugs": "Maximum number of slugs to process in full mode",
        "lookback_days": "Safety lookback window in days for incremental mode",
        "max_games": "Maximum number of games to crawl in this run",
        "max_retries": "Maximum retry attempts for failed requests",
        "max_review_pages": "Maximum review pages per review type",
        "overwrite_covers": "Whether to overwrite existing cover image files",
        "review_page_size": "Review API page size",
        "since_date": "Incremental start date in YYYY-MM-DD format",
        "slug_filter": "Optional slug filter when exporting",
        "start_slug": "Start crawling from this slug in full sitemap mode",
        "timeout": "HTTP timeout in seconds per request",
    }


def _format_settings(settings: dict[str, object]) -> str:
    explanations = _setting_explanations_en()
    return "\n".join(
        f"{key} = {settings[key]}  # {explanations.get(key, 'Explanation pending')}"
        for key in sorted(settings)
    )


def _setting_explanations_zh() -> dict[str, str]:
    return {
        "backoff": "重试退避系数，越大表示失败后等待增长更快",
        "concurrency": "并发抓取 worker 数量，1 表示串行",
        "covers_dir": "封面图片实体下载目录",
        "db": "SQLite 数据库文件路径",
        "delay": "每次请求之间的固定等待秒数",
        "download_covers": "抓取游戏时是否同时下载封面图片实体",
        "export_output": "导出 Excel 的默认输出路径",
        "finder_page_size": "增量模式每页抓取数量",
        "include_raw_json": "导出时是否包含原始 JSON 字段",
        "include_reviews": "是否抓取媒体评论和用户评论",
        "incremental_by_date": "是否启用按发布日期的增量抓取",
        "incremental_state_key": "数据库中保存增量检查点的键名",
        "limit_sitemaps": "全量模式最多读取的 sitemap 文件数",
        "limit_slugs": "全量模式最多处理的 slug 数",
        "lookback_days": "增量模式回看天数，降低漏抓风险",
        "max_games": "本次最多抓取的游戏数量",
        "max_retries": "请求失败后的最大重试次数",
        "max_review_pages": "每类评论最多翻页数",
        "overwrite_covers": "下载封面时是否覆盖本地已有文件",
        "review_page_size": "评论接口每页抓取条数",
        "since_date": "增量抓取起始日期（YYYY-MM-DD）",
        "slug_filter": "导出时只过滤某个 slug",
        "start_slug": "全量模式从指定 slug 开始抓取",
        "timeout": "单次 HTTP 请求超时秒数",
    }


def _format_settings_zh(settings: dict[str, object]) -> str:
    explanations = _setting_explanations_zh()
    return "\n".join(
        f"{key} = {settings[key]}  # {explanations.get(key, '参数说明待补充')}"
        for key in sorted(settings)
    )


def _style_output_line(line: str) -> list[tuple[str, str]]:
    if line.startswith("metacritic>"):
        return [("class:prompt", line)]

    if line.startswith("crawl summary:") or line.startswith("crawl-one summary:"):
        match = re.match(r"^(crawl(?:-one)? summary:)\s*(.*)$", line)
        if match:
            label = match.group(1)
            rest = match.group(2)
            fragments: list[tuple[str, str]] = [("class:summary.label", label)]
            if rest:
                fragments.append(("", " "))
                parts = [p for p in rest.split(" ") if p]
                for idx, part in enumerate(parts):
                    kv = part.split("=", 1)
                    if len(kv) == 2:
                        key, value = kv
                        fragments.extend(
                            [
                                ("class:summary.key", key),
                                ("", "="),
                                ("class:summary.value", value),
                            ]
                        )
                    else:
                        fragments.append(("class:summary.value", part))
                    if idx < len(parts) - 1:
                        fragments.append(("", " "))
                return fragments

    log_match = re.match(
        rf"^(?P<bullet>{re.escape(LOG_BULLET)}\s+)?(?P<header>(?:(?:\S+\s+\S+\s+))?(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+\S+\s+-\s+)(?P<message>.*)$",
        line,
    )
    if log_match:
        bullet = log_match.group("bullet") or ""
        level = log_match.group("level")
        header = log_match.group("header")
        message = log_match.group("message")
        fragments: list[tuple[str, str]] = []
        if bullet:
            fragments.append(("class:log.bullet", bullet))
        if "download-covers finished" in message or "cover download" in message:
            fragments.append(("class:log.cover", f"{header}{message}"))
            return fragments
        if level == "WARNING":
            fragments.extend([("class:log.warning", header), ("", message)])
            return fragments
        if level in {"ERROR", "CRITICAL"}:
            fragments.extend([("class:log.error", header), ("", message)])
            return fragments
        fragments.append(("", f"{header}{message}"))
        return fragments

    match = re.match(r"^([A-Za-z0-9_]+)\s=\s(.*)$", line)
    if not match:
        return [("", line)]

    key, rest = match.group(1), match.group(2)

    if "  # " in rest:
        value, comment = rest.split("  # ", 1)
        return [
            ("class:settings.key", key),
            ("", " = "),
            ("class:settings.value", value),
            ("class:settings.comment_prefix", "  # "),
            ("class:settings.comment", comment),
        ]

    return [
        ("class:settings.key", key),
        ("", " = "),
        ("class:settings.value", rest),
    ]


def _style_output_text(text: str) -> list[tuple[str, str]]:
    lines = str(text).split("\n")
    fragments: list[tuple[str, str]] = []
    for idx, line in enumerate(lines):
        fragments.extend(_style_output_line(line))
        if idx < len(lines) - 1:
            fragments.append(("", "\n"))
    return fragments


def _interactive_welcome_rows() -> list[tuple[str, str, str | None]]:
    return [
        ("title", INTERACTIVE_WELCOME_TITLE, None),
        ("subtitle", "Crawl games, export Excel, and sync cover assets from one shell.", None),
        ("blank", "", None),
        ("section", "Quick Start", None),
        ("item", "help or help-zh", "Show English or Chinese help and usage examples"),
        ("item", "show", "Inspect the active configuration"),
        ("item", "stop", "Request stop for the current background crawl/download task"),
        ("item", "crawl", "Run a crawl with the current settings"),
        ("item", "crawl-one <slug>", "Fetch one game immediately"),
        ("blank", "", None),
        ("section", "Input Tips", None),
        ("item", "Enter", "Submit the current command"),
        ("item", "Up / Down", "Navigate command history"),
        ("item", "Ctrl-C / Ctrl-D", "Exit the interactive shell"),
    ]


def _interactive_welcome_frame_line(text: str) -> str:
    return text.ljust(INTERACTIVE_WELCOME_CONTENT_WIDTH)


def _interactive_title_art_word_lines(word: str) -> list[str]:
    return [word.center(INTERACTIVE_WELCOME_CONTENT_WIDTH)]


def _interactive_title_art_lines() -> list[str]:
    return _interactive_title_art_word_lines(INTERACTIVE_WELCOME_TITLE)


def _interactive_welcome_lines() -> list[str]:
    lines: list[str] = []

    for kind, label, detail in _interactive_welcome_rows():
        if kind == "blank":
            content = ""
        elif kind == "title":
            lines.extend(_interactive_title_art_lines())
            continue
        elif kind == "item":
            detail_text = detail or ""
            content = f"  {label:<{INTERACTIVE_WELCOME_LABEL_WIDTH}} {detail_text}"
        else:
            content = label
        lines.append(_interactive_welcome_frame_line(content))

    return lines


def _interactive_banner_lines() -> list[str]:
    return _interactive_welcome_lines()


def _interactive_welcome_fragments() -> list[tuple[str, str]]:
    fragments: list[tuple[str, str]] = []
    row_width = INTERACTIVE_WELCOME_CONTENT_WIDTH

    def _append_line(parts: list[tuple[str, str]]) -> None:
        if fragments:
            fragments.append(("", "\n"))
        fragments.extend(parts)

    for kind, label, detail in _interactive_welcome_rows():
        if kind == "blank":
            _append_line([("", " " * row_width)])
            continue

        if kind == "title":
            content = label.center(row_width)
            _append_line([("class:welcome.title", content)])
            continue

        if kind == "subtitle":
            content = label.center(row_width)
            _append_line([("class:welcome.subtitle", content)])
            continue

        if kind == "section":
            padding = max(0, row_width - len(label))
            _append_line([("class:welcome.section", label), ("", " " * padding)])
            continue

        label_text = f"  {label:<{INTERACTIVE_WELCOME_LABEL_WIDTH}}"
        detail_text = detail or ""
        padding = max(0, row_width - len(label_text) - len(detail_text) - 1)
        _append_line(
            [
                ("class:welcome.command", label_text),
                ("", " "),
                ("class:welcome.text", detail_text),
                ("", " " * padding),
            ]
        )
    return fragments


def _interactive_composer_height(text: str) -> int:
    logical_lines = max(1, str(text).count("\n") + 1)
    return max(
        INTERACTIVE_COMPOSER_MIN_LINES,
        min(INTERACTIVE_COMPOSER_MAX_LINES, logical_lines),
    )


def _interactive_help_hint_text() -> str:
    return "Type 'help' or 'help-zh'"


def _interactive_command_is_running(running_command: dict[str, object | None]) -> bool:
    thread = running_command.get("thread")
    if not isinstance(thread, threading.Thread):
        return False
    if thread.is_alive():
        return True
    running_command["thread"] = None
    running_command["name"] = None
    return False


def _refresh_interactive_cursor_blink(app: object) -> None:
    """
    Force prompt_toolkit to re-send the blinking cursor shape on the next render.

    prompt_toolkit's VT100 output toggles blinking off when it shows the cursor
    during a repaint, but the renderer caches the last cursor shape and won't
    emit the blinking shape again unless the cached value changes.
    """

    renderer = getattr(app, "renderer", None)
    if renderer is not None and hasattr(renderer, "_last_cursor_shape"):
        renderer._last_cursor_shape = None

    invalidate = getattr(app, "invalidate", None)
    if callable(invalidate):
        invalidate()


def _format_interactive_command_echo(text: str) -> str:
    stripped = str(text).strip()
    if not stripped:
        return "metacritic>"

    lines = stripped.splitlines()
    prefix = "metacritic> "
    continuation = " " * len(prefix)
    rendered = [f"{prefix}{lines[0]}"]
    for line in lines[1:]:
        rendered.append(f"{continuation}{line}")
    return "\n".join(rendered)


def _run_with_captured_stdout(
    func: Callable[[argparse.Namespace], int],
    namespace: argparse.Namespace,
    emit: Callable[[str], None],
) -> None:
    class _StreamingStdout(io.TextIOBase):
        def __init__(self, emit_func: Callable[[str], None]) -> None:
            self._emit = emit_func
            self._pending = ""

        def write(self, text: str) -> int:  # type: ignore[override]
            if not text:
                return 0
            self._pending += text
            while "\n" in self._pending:
                line, self._pending = self._pending.split("\n", 1)
                self._emit(line)
            return len(text)

        def flush(self) -> None:  # type: ignore[override]
            if self._pending:
                self._emit(self._pending)
                self._pending = ""

    stream = _StreamingStdout(emit)
    with redirect_stdout(stream):
        exit_code = func(namespace)
        stream.flush()
    emit(f"[done] exit_code={exit_code}")


def _run_interactive_command(
    tokens: list[str],
    settings: dict[str, object],
    emit: Callable[[str], None],
    *,
    request_stop: Callable[[], str] | None = None,
    stop_event: threading.Event | None = None,
) -> bool:
    cmd = tokens[0].lower()
    args = tokens[1:]

    if cmd in {"exit", "quit"}:
        return False
    if cmd in {"help", "h", "?"}:
        if args and args[0].lower() in {"zh", "cn"}:
            emit(_print_interactive_help_zh())
        else:
            emit(_print_interactive_help())
        return True
    if cmd in {"help-zh", "help_cn", "help-cn", "帮助"}:
        emit(_print_interactive_help_zh())
        return True
    if cmd in {"show-zh", "show_cn", "show-cn", "配置"}:
        emit(_format_settings_zh(settings))
        return True
    if cmd in {"show", "config"}:
        if args and args[0].lower() in {"zh", "cn"}:
            emit(_format_settings_zh(settings))
        else:
            emit(_format_settings(settings))
        return True
    if cmd == "reset":
        settings.clear()
        settings.update(_interactive_defaults())
        emit("Settings reset.")
        return True
    if cmd == "stop":
        if request_stop is None:
            emit("No background command is running.")
        else:
            emit(request_stop())
        return True
    if cmd == "set":
        if len(args) < 2:
            emit("Usage: set <key> <value>")
            return True
        key = args[0]
        raw_value = " ".join(args[1:])
        try:
            value = _convert_setting_value(key, raw_value)
            if key == "since_date" and value is not None:
                date.fromisoformat(str(value))
            if key == "concurrency" and int(value) < 1:
                raise ValueError("concurrency must be >= 1")
        except (KeyError, ValueError) as exc:
            emit(f"Cannot set value: {exc}")
            return True
        settings[key] = value
        emit(f"Updated: {key}={value}")
        return True

    try:
        if cmd == "crawl":
            ns = argparse.Namespace(
                db=settings["db"],
                max_games=settings["max_games"],
                start_slug=settings["start_slug"],
                limit_sitemaps=settings["limit_sitemaps"],
                limit_slugs=settings["limit_slugs"],
                include_reviews=settings["include_reviews"],
                review_page_size=settings["review_page_size"],
                max_review_pages=settings["max_review_pages"],
                concurrency=settings["concurrency"],
                incremental_by_date=settings["incremental_by_date"],
                since_date=settings["since_date"],
                lookback_days=settings["lookback_days"],
                finder_page_size=settings["finder_page_size"],
                incremental_state_key=settings["incremental_state_key"],
                timeout=settings["timeout"],
                max_retries=settings["max_retries"],
                backoff=settings["backoff"],
                delay=settings["delay"],
                download_covers=settings["download_covers"],
                covers_dir=settings["covers_dir"],
                overwrite_covers=settings["overwrite_covers"],
                print_summary=True,
                stop_event=stop_event,
            )
            _run_with_captured_stdout(run_crawl, ns, emit)
            return True

        if cmd == "crawl-one":
            if not args:
                emit("Usage: crawl-one <slug>")
                return True
            slug = args[0]
            ns = argparse.Namespace(
                slug=slug,
                db=settings["db"],
                include_reviews=settings["include_reviews"],
                review_page_size=settings["review_page_size"],
                max_review_pages=settings["max_review_pages"],
                timeout=settings["timeout"],
                max_retries=settings["max_retries"],
                backoff=settings["backoff"],
                delay=settings["delay"],
                download_covers=settings["download_covers"],
                covers_dir=settings["covers_dir"],
                overwrite_covers=settings["overwrite_covers"],
                print_summary=True,
                stop_event=stop_event,
            )
            _run_with_captured_stdout(run_crawl_one, ns, emit)
            return True

        if cmd == "download-covers":
            output_dir = args[0] if args else settings["covers_dir"]
            ns = argparse.Namespace(
                db=settings["db"],
                output_dir=output_dir,
                slug=settings["slug_filter"],
                limit=None,
                overwrite=settings["overwrite_covers"],
                timeout=settings["timeout"],
                max_retries=settings["max_retries"],
                backoff=settings["backoff"],
                delay=settings["delay"],
                stop_event=stop_event,
            )
            _run_with_captured_stdout(run_download_covers, ns, emit)
            return True

        if cmd == "slugs":
            output = args[0] if args else None
            ns = argparse.Namespace(
                limit_sitemaps=settings["limit_sitemaps"],
                limit_slugs=settings["limit_slugs"],
                output=output,
                timeout=settings["timeout"],
                max_retries=settings["max_retries"],
                backoff=settings["backoff"],
                delay=settings["delay"],
                stop_event=stop_event,
            )
            _run_with_captured_stdout(run_slugs, ns, emit)
            return True

        if cmd == "export-excel":
            output = args[0] if args else settings["export_output"]
            ns = argparse.Namespace(
                db=settings["db"],
                output=output,
                slug=settings["slug_filter"],
                include_raw_json=settings["include_raw_json"],
            )
            _run_with_captured_stdout(run_export_excel, ns, emit)
            return True

        emit(f"Unknown command: {cmd}. Type 'help' or 'help-zh' for available commands.")
        return True
    except Exception as exc:  # pragma: no cover
        emit(f"Command failed: {exc}")
        return True


class _InteractiveLogHandler(logging.Handler):
    def __init__(self, emit: Callable[[str], None]) -> None:
        super().__init__()
        self._emit = emit
        self.setFormatter(logging.Formatter(LOG_FORMAT))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._emit(f"{self.format(record)}\n")
        except Exception:  # pragma: no cover
            return


def _run_interactive_plain(settings: dict[str, object]) -> int:
    for line in _interactive_banner_lines():
        print(line)
    while True:
        try:
            line = input("metacritic> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        try:
            tokens = shlex.split(line)
        except ValueError as exc:
            print(f"Invalid input: {exc}")
            continue
        if not _run_interactive_command(tokens, settings, print):
            return 0


def run_interactive() -> int:
    settings = _interactive_defaults()
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return _run_interactive_plain(settings)

    try:
        from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
        from prompt_toolkit import PromptSession, print_formatted_text
        from prompt_toolkit.cursor_shapes import CursorShape
        from prompt_toolkit.formatted_text import FormattedText
        from prompt_toolkit.history import InMemoryHistory
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.patch_stdout import patch_stdout
        from prompt_toolkit.styles import Style
    except Exception:
        return _run_interactive_plain(settings)

    output_lock = threading.Lock()
    running_command: dict[str, object | None] = {"thread": None, "name": None}
    stop_event = threading.Event()

    output_style = Style.from_dict(
        {
            "prompt": "bold ansicyan",
            "summary.label": "bold ansibrightgreen",
            "summary.key": "ansibrightgreen",
            "summary.value": "bold ansiwhite",
            "log.bullet": "ansibrightblack",
            "log.warning": "bold ansibrightyellow",
            "log.error": "bold ansibrightred",
            "log.cover": "bold ansibrightcyan",
            "settings.key": "ansibrightblue",
            "settings.value": "bold ansiyellow",
            "settings.comment_prefix": "ansibrightblack",
            "settings.comment": "ansibrightblack",
            "welcome.border": "ansibrightblack",
            "welcome.title": "bold ansibrightcyan",
            "welcome.subtitle": "ansiwhite",
            "welcome.section": "bold ansibrightgreen",
            "welcome.command": "bold ansiyellow",
            "welcome.text": "ansiwhite",
        }
    )
    prompt_style = Style.from_dict(
        {
            "prompt": "bold ansicyan",
            "placeholder": "ansibrightblack",
        }
    )

    previous_no_cpr = os.environ.get("PROMPT_TOOLKIT_NO_CPR")
    os.environ["PROMPT_TOOLKIT_NO_CPR"] = "1"

    session = PromptSession(history=InMemoryHistory())

    kb = KeyBindings()

    def emit_output(message: str) -> None:
        with output_lock:
            print_formatted_text(
                FormattedText(_style_output_text(str(message))),
                style=output_style,
            )

    stoppable_commands = {"crawl", "crawl-one", "slugs", "download-covers"}

    def _request_stop_current_command() -> str:
        if not _interactive_command_is_running(running_command):
            return "No background command is running."

        command_name = str(running_command.get("name") or "")
        if command_name not in stoppable_commands:
            return f"[stop] command cannot be interrupted: {command_name}"
        if stop_event.is_set():
            return f"[stopping] stop already requested for {command_name}"
        stop_event.set()
        return f"[stopping] requested stop for {command_name}"

    def _run_command_in_background(tokens: list[str]) -> None:
        if _interactive_command_is_running(running_command):
            emit_output(f"[busy] command is still running: {running_command.get('name')}")
            return

        command_name = str(tokens[0]).lower()
        stop_event.clear()

        def _worker() -> None:
            try:
                _run_interactive_command(
                    tokens,
                    settings,
                    emit_output,
                    stop_event=stop_event,
                )
            finally:
                stop_event.clear()
                running_command["thread"] = None
                running_command["name"] = None

        running_command["name"] = command_name
        worker = threading.Thread(target=_worker, name=f"interactive-{command_name}", daemon=True)
        running_command["thread"] = worker
        emit_output(f"[running] {command_name} (prompt remains responsive)")
        worker.start()

    @kb.add("enter")
    def _submit_current_input(event) -> None:
        event.current_buffer.validate_and_handle()

    @kb.add("up")
    def _history_previous(event) -> None:
        event.current_buffer.history_backward()
        _refresh_interactive_cursor_blink(event.app)

    @kb.add("down")
    def _history_next(event) -> None:
        event.current_buffer.history_forward()
        _refresh_interactive_cursor_blink(event.app)

    @kb.add("c-c")
    def _abort_prompt(event) -> None:
        event.app.exit(exception=KeyboardInterrupt(), style="class:aborting")

    @kb.add("c-d")
    def _end_prompt(event) -> None:
        event.app.exit(exception=EOFError(), style="class:exiting")

    root_logger = logging.getLogger()
    previous_handlers = list(root_logger.handlers)
    previous_level = root_logger.level
    for handler in previous_handlers:
        root_logger.removeHandler(handler)
    ui_handler = _InteractiveLogHandler(emit_output)
    root_logger.addHandler(ui_handler)
    root_logger.setLevel(previous_level)

    background_commands = {"crawl", "crawl-one", "slugs", "download-covers", "export-excel"}
    with output_lock:
        print_formatted_text(
            FormattedText(_interactive_welcome_fragments()),
            style=output_style,
        )

    try:
        with patch_stdout():
            while True:
                try:
                    line = session.prompt(
                        [("class:prompt", "metacritic> ")],
                        key_bindings=kb,
                        style=prompt_style,
                        placeholder=lambda: [("class:placeholder", _interactive_help_hint_text())],
                        cursor=CursorShape.BLINKING_BEAM,
                        multiline=False,
                        show_frame=True,
                        auto_suggest=AutoSuggestFromHistory(),
                        mouse_support=False,
                    ).strip()
                except (EOFError, KeyboardInterrupt):
                    print()
                    return 0

                if not line:
                    continue

                try:
                    tokens = shlex.split(line)
                except ValueError as exc:
                    emit_output(f"Invalid input: {exc}")
                    continue

                cmd = tokens[0].lower()
                if cmd in background_commands:
                    _run_command_in_background(tokens)
                    continue

                if not _run_interactive_command(
                    tokens,
                    settings,
                    emit_output,
                    request_stop=_request_stop_current_command,
                ):
                    return 0
    finally:
        root_logger.removeHandler(ui_handler)
        for handler in previous_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(previous_level)
        if previous_no_cpr is None:
            os.environ.pop("PROMPT_TOOLKIT_NO_CPR", None)
        else:
            os.environ["PROMPT_TOOLKIT_NO_CPR"] = previous_no_cpr


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    if args.command is None or args.command == "interactive":
        return run_interactive()
    if args.command == "crawl":
        return run_crawl(args)
    if args.command == "crawl-one":
        return run_crawl_one(args)
    if args.command == "slugs":
        return run_slugs(args)
    if args.command == "export-excel":
        return run_export_excel(args)
    if args.command == "download-covers":
        return run_download_covers(args)
    parser.error(f"unknown command: {args.command}")
    return 2
