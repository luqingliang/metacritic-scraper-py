from __future__ import annotations

import argparse
import io
import logging
import queue
import re
import shlex
import sys
import threading
import time
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
    download_covers = bool(getattr(args, "download_covers", False))
    covers_dir = str(getattr(args, "covers_dir", "data/covers"))
    overwrite_covers = bool(getattr(args, "overwrite_covers", False))

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
                "crawl finished games=%d critic_reviews=%d user_reviews=%d "
                "covers_downloaded=%d covers_skipped=%d covers_failed=%d failed=%d"
            ),
            result.games_crawled,
            result.critic_reviews_saved,
            result.user_reviews_saved,
            result.covers_downloaded,
            result.covers_skipped,
            result.covers_failed,
            len(result.failed_slugs),
        )
        if bool(getattr(args, "print_summary", False)):
            print(
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
        if result.failed_slugs:
            logging.warning("failed slugs: %s", ",".join(result.failed_slugs))
        return 0
    finally:
        storage.close()


def run_crawl_one(args: argparse.Namespace) -> int:
    download_covers = bool(getattr(args, "download_covers", False))
    covers_dir = str(getattr(args, "covers_dir", "data/covers"))
    overwrite_covers = bool(getattr(args, "overwrite_covers", False))

    storage = SQLiteStorage(args.db)
    try:
        with _build_client(args) as client:
            scraper = MetacriticScraper(client, storage)
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
                "crawl-one finished games=%d critic_reviews=%d user_reviews=%d "
                "covers_downloaded=%d covers_skipped=%d covers_failed=%d failed=%d"
            ),
            result.games_crawled,
            result.critic_reviews_saved,
            result.user_reviews_saved,
            result.covers_downloaded,
            result.covers_skipped,
            result.covers_failed,
            len(result.failed_slugs),
        )
        if bool(getattr(args, "print_summary", False)):
            print(
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


def run_download_covers(args: argparse.Namespace) -> int:
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be >= 1")

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
            for slug, cover_url in rows:
                status = downloader.download(slug=slug, cover_url=cover_url)
                if status == "downloaded":
                    downloaded += 1
                elif status == "skipped":
                    skipped += 1
                else:
                    failed += 1

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


def _print_interactive_help(include_clear: bool = False) -> str:
    lines = [
        "Interactive commands:",
        "  help                              Show help",
        "  help-zh | 帮助                    Show Chinese annotated help",
        "  show-zh | 配置                    Show settings with Chinese explanations",
        "  show                              Show settings with English explanations",
        "  set <key> <value>                 Update setting (use 'none' for null)",
        "  reset                             Reset settings to defaults",
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
    if include_clear:
        lines.insert(3, "  clear                             Clear screen output")
    return "\n".join(lines)


def _print_interactive_help_zh(include_clear: bool = False) -> str:
    lines = [
        "交互命令（中文释义）:",
        "  help                              显示英文帮助",
        "  help-zh | 帮助                    显示中文释义帮助",
        "  show-zh | 配置                    显示带中文说明的参数列表",
        "  show                              显示带英文说明的参数列表",
        "  set <key> <value>                 修改配置（null/none 表示空值）",
        "  reset                             重置为默认配置",
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
    if include_clear:
        lines.insert(3, "  clear                             清屏")
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

    if re.search(r"\bWARNING\b", line):
        return [("class:log.warning", line)]

    if "download-covers finished" in line or "cover download" in line:
        return [("class:log.cover", line)]

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


def _interactive_banner_lines() -> list[str]:
    return [
        "Metacritic Scraper Interactive Shell",
        (
            "Type 'help' to see commands. PgUp/PgDn and Ctrl+Up/Down scroll logs. "
            "Mouse drag selects text. Cursor stays in input. Ctrl-C/Ctrl-D exits."
        ),
    ]


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
    include_clear: bool = False,
    clear_output: Callable[[], None] | None = None,
) -> bool:
    cmd = tokens[0].lower()
    args = tokens[1:]

    if cmd in {"exit", "quit"}:
        return False
    if cmd in {"help", "h", "?"}:
        if args and args[0].lower() in {"zh", "cn"}:
            emit(_print_interactive_help_zh(include_clear=include_clear))
        else:
            emit(_print_interactive_help(include_clear=include_clear))
        return True
    if cmd in {"help-zh", "help_cn", "help-cn", "帮助"}:
        emit(_print_interactive_help_zh(include_clear=include_clear))
        return True
    if include_clear and cmd in {"clear", "cls"}:
        if clear_output is not None:
            clear_output()
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

        emit(f"Unknown command: {cmd}. Type 'help' for available commands.")
        return True
    except Exception as exc:  # pragma: no cover
        emit(f"Command failed: {exc}")
        return True


class _InteractiveLogHandler(logging.Handler):
    def __init__(self, emit: Callable[[str], None]) -> None:
        super().__init__()
        self._emit = emit
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._emit(self.format(record))
        except Exception:  # pragma: no cover
            return


def _run_interactive_plain(settings: dict[str, object]) -> int:
    print("Metacritic Scraper Interactive Shell")
    print("Type 'help' to see commands.")
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
        from prompt_toolkit import Application
        from prompt_toolkit.cursor_shapes import CursorShape
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.layout import HSplit, Layout, Window
        from prompt_toolkit.lexers import Lexer
        from prompt_toolkit.styles import Style
        from prompt_toolkit.widgets import Frame, TextArea
    except Exception:
        return _run_interactive_plain(settings)

    class _InteractiveOutputLexer(Lexer):
        def lex_document(self, document):
            lines = document.lines

            def get_line(lineno: int):
                if lineno < 0 or lineno >= len(lines):
                    return []
                return _style_output_line(lines[lineno])

            return get_line

    output_lines: list[str] = []
    max_lines = 5000
    app_holder: dict[str, object | None] = {"app": None}
    running_command: dict[str, object | None] = {"thread": None, "name": None}
    pending_messages: queue.Queue[str] = queue.Queue()
    pending_exit: dict[str, bool] = {"requested": False}
    follow_output: dict[str, bool] = {"value": True}
    last_invalidate_at: dict[str, float] = {"value": 0.0}
    min_invalidate_interval = 0.08

    output_box = TextArea(
        text="",
        focusable=False,
        read_only=True,
        scrollbar=True,
        wrap_lines=True,
        lexer=_InteractiveOutputLexer(),
    )
    input_box = TextArea(
        height=1,
        prompt="metacritic> ",
        multiline=False,
        wrap_lines=False,
        focus_on_click=True,
    )

    def _sync_follow_output_state() -> None:
        doc = output_box.buffer.document
        if not doc.lines:
            follow_output["value"] = True
            return
        follow_output["value"] = doc.cursor_position_row >= len(doc.lines) - 1

    def _append_output_lines(lines: list[str]) -> None:
        if not lines:
            return
        _sync_follow_output_state()
        previous_cursor_position = output_box.buffer.cursor_position
        output_lines.extend(lines)
        if len(output_lines) > max_lines:
            del output_lines[: len(output_lines) - max_lines]
        output_box.text = "\n".join(output_lines)
        if follow_output["value"]:
            output_box.buffer.cursor_position = len(output_box.text)
        else:
            output_box.buffer.cursor_position = min(previous_cursor_position, len(output_box.text))

    def append_output(message: str) -> None:
        _append_output_lines(str(message).splitlines() or [""])

    def _invalidate_app(force: bool = False) -> None:
        app_obj = app_holder.get("app")
        if app_obj is None:
            return
        now = time.monotonic()
        if not force and (now - last_invalidate_at["value"]) < min_invalidate_interval:
            return
        last_invalidate_at["value"] = now
        try:
            app_obj.invalidate()  # type: ignore[union-attr]
        except Exception:
            return

    def append_output_threadsafe(message: str) -> None:
        pending_messages.put(str(message))
        _invalidate_app()

    def _drain_pending_messages(app=None) -> None:
        drained_lines: list[str] = []
        drained_messages = 0
        max_messages_per_frame = 500
        while True:
            if drained_messages >= max_messages_per_frame:
                break
            try:
                message = pending_messages.get_nowait()
            except queue.Empty:
                break
            drained_lines.extend(str(message).splitlines() or [""])
            drained_messages += 1
        _append_output_lines(drained_lines)
        if app is not None and not pending_messages.empty():
            _invalidate_app(force=True)
        if pending_exit["requested"] and app is not None:
            pending_exit["requested"] = False
            try:
                app.exit(result=0)
            except Exception:
                pass

    def _enforce_blinking_cursor(app=None) -> None:
        if app is None:
            return
        try:
            app.output.set_cursor_shape(CursorShape.BLINKING_BEAM)
            app.output.flush()
        except Exception:
            return

    def clear_output() -> None:
        output_lines.clear()
        output_lines.extend(_interactive_banner_lines())
        output_box.text = "\n".join(output_lines)
        output_box.buffer.cursor_position = len(output_box.text)
        follow_output["value"] = True

    def _move_output_cursor(row_delta: int) -> None:
        doc = output_box.buffer.document
        if not doc.lines:
            return
        current_row = doc.cursor_position_row
        max_row = len(doc.lines) - 1
        target_row = max(0, min(max_row, current_row + row_delta))
        output_box.buffer.cursor_position = doc.translate_row_col_to_index(target_row, 0)
        follow_output["value"] = target_row >= max_row

    def _jump_output_end() -> None:
        output_box.buffer.cursor_position = len(output_box.text)
        follow_output["value"] = True

    def _command_is_running() -> bool:
        thread = running_command.get("thread")
        if thread is None:
            return False
        return bool(getattr(thread, "is_alive", lambda: False)())

    def _run_command_in_background(tokens: list[str]) -> None:
        if _command_is_running():
            append_output(f"[busy] command is still running: {running_command.get('name')}")
            return

        command_name = str(tokens[0]).lower()

        def _worker() -> None:
            try:
                keep_running = _run_interactive_command(
                    tokens,
                    settings,
                    append_output_threadsafe,
                    include_clear=True,
                    clear_output=clear_output,
                )
                if not keep_running:
                    pending_exit["requested"] = True
                    app_obj = app_holder.get("app")
                    if app_obj is not None:
                        try:
                            app_obj.invalidate()  # type: ignore[union-attr]
                        except Exception:
                            pass
            finally:
                running_command["thread"] = None
                running_command["name"] = None

        running_command["name"] = command_name
        worker = threading.Thread(target=_worker, name=f"interactive-{command_name}", daemon=True)
        running_command["thread"] = worker
        append_output(f"[running] {command_name} (UI remains responsive)")
        worker.start()

    kb = KeyBindings()

    @kb.add("enter")
    def _(event) -> None:
        line = input_box.text.strip()
        input_box.text = ""
        if not line:
            return
        _jump_output_end()
        append_output(f"metacritic> {line}")
        try:
            tokens = shlex.split(line)
        except ValueError as exc:
            append_output(f"Invalid input: {exc}")
            return

        cmd = tokens[0].lower()
        background_commands = {"crawl", "crawl-one", "slugs", "download-covers", "export-excel"}
        if cmd in background_commands:
            _run_command_in_background(tokens)
            return

        if not _run_interactive_command(
            tokens,
            settings,
            append_output,
            include_clear=True,
            clear_output=clear_output,
        ):
            event.app.exit(result=0)

    @kb.add("c-c")
    @kb.add("c-d")
    def _(event) -> None:
        event.app.exit(result=0)

    @kb.add("pageup", eager=True)
    def _(event) -> None:
        _move_output_cursor(-12)

    @kb.add("pagedown", eager=True)
    def _(event) -> None:
        _move_output_cursor(12)

    @kb.add("c-pageup", eager=True)
    def _(event) -> None:
        _move_output_cursor(-12)

    @kb.add("c-pagedown", eager=True)
    def _(event) -> None:
        _move_output_cursor(12)

    @kb.add("c-up", eager=True)
    def _(event) -> None:
        _move_output_cursor(-3)

    @kb.add("c-down", eager=True)
    def _(event) -> None:
        _move_output_cursor(3)

    @kb.add("c-end", eager=True)
    def _(event) -> None:
        _jump_output_end()

    app = Application(
        layout=Layout(
            HSplit(
                [
                    Frame(output_box, title="Metacritic Scraper"),
                    Window(height=1, char="-"),
                    input_box,
                ]
            ),
            focused_element=input_box,
        ),
        key_bindings=kb,
        full_screen=True,
        mouse_support=False,
        before_render=lambda app_obj: _drain_pending_messages(app_obj),
        after_render=lambda app_obj: _enforce_blinking_cursor(app_obj),
        cursor=CursorShape.BLINKING_BEAM,
        style=Style.from_dict(
            {
                "frame.label": "bold",
                "prompt": "bold ansicyan",
                "summary.label": "bold ansibrightgreen",
                "summary.key": "ansibrightgreen",
                "summary.value": "bold ansiwhite",
                "log.warning": "bold ansibrightyellow",
                "log.cover": "bold ansibrightcyan",
                "settings.key": "ansibrightblue",
                "settings.value": "bold ansiyellow",
                "settings.comment_prefix": "ansibrightblack",
                "settings.comment": "ansibrightblack",
            }
        ),
    )
    app_holder["app"] = app

    root_logger = logging.getLogger()
    previous_handlers = list(root_logger.handlers)
    previous_level = root_logger.level
    for handler in previous_handlers:
        root_logger.removeHandler(handler)
    ui_handler = _InteractiveLogHandler(append_output_threadsafe)
    root_logger.addHandler(ui_handler)
    root_logger.setLevel(previous_level)

    for banner_line in _interactive_banner_lines():
        append_output(banner_line)

    try:
        result = app.run()
        return int(result or 0)
    finally:
        app_holder["app"] = None
        root_logger.removeHandler(ui_handler)
        for handler in previous_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(previous_level)


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
