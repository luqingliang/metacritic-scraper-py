from __future__ import annotations

import argparse
import io
import json
import logging
import os
import random
import re
import shlex
import sqlite3
import sys
import threading
from contextlib import contextmanager, redirect_stdout
from contextvars import ContextVar, copy_context
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
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
from .storage import APP_TABLE_NAMES, SQLiteStorage, load_slug_search_candidates_from_db

DEFAULT_QUICKSTART_MAX_REVIEW_PAGES = 1
DEFAULT_CONCURRENCY = 4
DEFAULT_SLUG_SYNC_BATCH_SIZE = 500
GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY = "game_slugs_last_successful_full_sync_at"
GAME_SLUGS_FULL_SYNC_MAX_AGE = timedelta(days=7)
SHARED_SETTINGS_PATH = "config/cli_settings.json"
INTERACTIVE_WELCOME_CONTENT_WIDTH = 74
INTERACTIVE_WELCOME_LABEL_WIDTH = 28
INTERACTIVE_WELCOME_TITLE = "GAMECRITIC"
INTERACTIVE_BACKGROUND_COMMANDS = {
    "crawl",
    "crawl-one",
    "search-slug",
    "crawl-reviews",
    "sync-slugs",
    "download-covers",
    "export-excel",
    "clear-db",
}
INTERACTIVE_STOPPABLE_COMMANDS = {"crawl", "crawl-one", "crawl-reviews", "sync-slugs", "download-covers"}
INTERACTIVE_HELP_LABEL_WIDTH = 34
INTERACTIVE_HELP_TITLE_EN = "Interactive Help"
INTERACTIVE_HELP_TITLE_ZH = "交互帮助"
INTERACTIVE_HELP_SUBTITLE_EN = "Run commands directly at the gamecritic> prompt."
INTERACTIVE_HELP_SUBTITLE_ZH = "在当前 gamecritic> 提示符里直接输入命令。"
INTERACTIVE_HELP_SECTIONS = (
    (
        "Core Workflow",
        "主流程",
        (
            ("crawl", "Run crawl with current settings", "用当前配置执行批量抓取"),
            (
                "search-slug <game_name>",
                "Search the best-matching local slug by game name",
                "根据游戏名搜索本地最匹配的 slug",
            ),
            ("crawl-one <slug>", "Crawl one game with current settings", "抓取单个游戏"),
            (
                "crawl-reviews [slug]",
                "Backfill both critic and user reviews for games in SQLite",
                "为 SQLite 中已有游戏补抓媒体和用户评论",
            ),
            ("sync-slugs", "Sync sitemap slugs into SQLite", "将 sitemap 中的 slug 同步到 SQLite"),
            (
                "download-covers [slug]",
                "Download cover image files from DB",
                "基于已抓取数据下载封面图片实体",
            ),
            ("export-excel [output_path]", "Export DB data to Excel", "导出 SQLite 数据到 Excel"),
        ),
    ),
    (
        "Session & Config",
        "会话与配置",
        (
            (
                "show | show-zh",
                "Inspect settings with English or Chinese explanations",
                "查看带英文或中文说明的参数列表",
            ),
            ("set <key> <value>", "Update setting (use 'none' for null)", "修改配置（null/none 表示空值）"),
            ("reset", "Reset settings to defaults", "重置为默认配置"),
            (
                "stop",
                "Request stop for the current background crawl/download task",
                "请求停止当前后台抓取/下载任务",
            ),
        ),
    ),
    (
        "Safety & Exit",
        "帮助与退出",
        (
            ("help | help-zh", "Show help in English or Chinese", "显示英文或中文释义帮助"),
            ("clear-db", "Delete all rows from all SQLite tables", "清空所有 SQLite 业务表中的数据并保留表结构"),
            ("exit | quit", "Exit interactive shell", "退出交互模式"),
        ),
    ),
)
INTERACTIVE_HELP_EXAMPLES_LABEL_EN = "Examples"
INTERACTIVE_HELP_EXAMPLES_LABEL_ZH = "示例"
INTERACTIVE_HELP_EXAMPLES_EN = (
    "crawl",
    "search-slug Elden Ring",
    "crawl-one the-legend-of-zelda-breath-of-the-wild",
    "crawl-reviews",
    "show",
    "set concurrency 4",
    "sync-slugs",
    "download-covers",
    "export-excel data/excel/gamecritic_export.xlsx",
)
INTERACTIVE_HELP_EXAMPLES_ZH = (
    "crawl",
    "search-slug Elden Ring",
    "crawl-one the-legend-of-zelda-breath-of-the-wild",
    "crawl-reviews",
    "show-zh",
    "set concurrency 4",
    "sync-slugs",
    "download-covers",
    "export-excel data/excel/gamecritic_export.xlsx",
)
INTERACTIVE_SETTINGS_DISPLAY_ORDER = (
    "db",
    "concurrency",
    "include_critic_reviews",
    "include_user_reviews",
    "review_page_size",
    "max_review_pages",
    "download_covers",
    "covers_dir",
    "overwrite_covers",
    "timeout",
    "max_retries",
    "backoff",
    "delay",
    "export_output",
)
INTERACTIVE_HELP_SAMPLE_SIZE = 3
LOG_BULLET = "●"
LOG_FORMAT = f"{LOG_BULLET} %(log_header)s%(progress_display)s - %(message)s"
_LOG_COMMAND_CONTEXT: ContextVar[str] = ContextVar("log_command_context", default="command")
_BOOL_SETTING_KEYS = {"include_critic_reviews", "include_user_reviews", "download_covers", "overwrite_covers"}
_INT_SETTING_KEYS = {"review_page_size", "concurrency", "max_retries"}
_FLOAT_SETTING_KEYS = {"timeout", "backoff", "delay"}
_OPTIONAL_INT_SETTING_KEYS = {"max_review_pages"}
_STRING_SETTING_KEYS = {"db", "export_output", "covers_dir"}
_SEARCH_SLUG_AUTO_ACCEPT_SCORE = 0.92
_SEARCH_SLUG_AMBIGUITY_MARGIN = 0.03
_SEARCH_SLUG_MIN_SCORE = 0.55
_SEARCH_SLUG_MAX_CANDIDATES = 5


@dataclass(frozen=True)
class _SlugSearchMatch:
    slug: str
    title: str | None
    score: float
    matched_by: str


def _current_log_command_name() -> str:
    current = str(_LOG_COMMAND_CONTEXT.get() or "").strip().lower()
    return current or "command"


def _normalize_log_command_name(command: object | None) -> str:
    normalized = str(command or "").strip().lower()
    return normalized or _current_log_command_name()


def _format_log_header(command_name: str, level_name: str) -> str:
    normalized_level = str(level_name or "INFO").upper()
    if normalized_level == "INFO":
        return command_name
    return f"{command_name}-{normalized_level}"


@contextmanager
def _logging_command_context(command: object | None):
    token = _LOG_COMMAND_CONTEXT.set(_normalize_log_command_name(command))
    try:
        yield
    finally:
        _LOG_COMMAND_CONTEXT.reset(token)


class _ProgressAwareFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        command_name = _normalize_log_command_name(getattr(record, "command_name", None))
        record.log_header = _format_log_header(command_name, record.levelname)
        progress = str(getattr(record, "progress", "") or "").strip()
        record.progress_display = f" {progress}" if progress else ""
        return super().format(record)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gamecritic",
        description="Scrape Metacritic game data into SQLite.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logs.")

    subparsers = parser.add_subparsers(dest="command", required=False)

    subparsers.add_parser(
        "crawl",
        help="Crawl games using the shared settings profile.",
    )
    crawl_one = subparsers.add_parser(
        "crawl-one",
        help="Crawl one game by slug using the shared settings profile.",
    )
    crawl_one.add_argument("slug", help="Game slug.")
    search_slug = subparsers.add_parser(
        "search-slug",
        help="Search for the best-matching slug by game name using the local SQLite index.",
    )
    search_slug.add_argument("query", nargs="+", help="Game name or partial title.")
    crawl_reviews = subparsers.add_parser(
        "crawl-reviews",
        help="Backfill critic and user reviews using the shared settings profile.",
    )
    crawl_reviews.add_argument(
        "slug",
        nargs="?",
        help="Game slug. When omitted, backfill reviews for all crawled games.",
    )
    subparsers.add_parser(
        "sync-slugs",
        help="Sync game slugs using the shared settings profile.",
    )
    subparsers.add_parser(
        "export-excel",
        help="Export crawled SQLite data using the shared settings profile.",
    )
    download_covers = subparsers.add_parser(
        "download-covers",
        help="Download cover image files using the shared settings profile.",
    )
    download_covers.add_argument(
        "slug",
        nargs="?",
        help="Game slug. When omitted, download covers for all crawled games.",
    )
    subparsers.add_parser(
        "clear-db",
        help="Delete all rows from all project SQLite tables while keeping the schema.",
    )

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
    formatter = _ProgressAwareFormatter(LOG_FORMAT)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)
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


def _resolve_review_selection(
    args: argparse.Namespace,
    *,
    default_to_both: bool = False,
) -> tuple[bool, bool]:
    include_critic_reviews = bool(getattr(args, "include_critic_reviews", False))
    include_user_reviews = bool(getattr(args, "include_user_reviews", False))
    if default_to_both and not include_critic_reviews and not include_user_reviews:
        return True, True
    return include_critic_reviews, include_user_reviews


def _parse_checkpoint_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _should_auto_sync_game_slugs_before_crawl(storage: SQLiteStorage) -> bool:
    state_value = storage.get_state(GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY)
    if state_value is None:
        logging.info("game_slugs checkpoint missing; running sync-slugs before crawl")
        return True

    checkpoint = _parse_checkpoint_datetime(state_value)
    if checkpoint is None:
        if isinstance(state_value, str):
            logging.warning(
                "game_slugs checkpoint is invalid state_value=%s; running sync-slugs before crawl",
                state_value,
            )
            return True
        return False

    now = datetime.now(timezone.utc)
    age = now - checkpoint
    if age >= GAME_SLUGS_FULL_SYNC_MAX_AGE:
        logging.info(
            "game_slugs checkpoint stale state_value=%s age_seconds=%d; running sync-slugs before crawl",
            checkpoint.isoformat(),
            int(age.total_seconds()),
        )
        return True

    logging.info(
        "game_slugs checkpoint fresh state_value=%s age_seconds=%d; skipping sync-slugs before crawl",
        checkpoint.isoformat(),
        max(0, int(age.total_seconds())),
    )
    return False


def _build_auto_sync_slugs_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        db=args.db,
        timeout=args.timeout,
        max_retries=args.max_retries,
        backoff=args.backoff,
        delay=args.delay,
        print_summary=False,
        stop_event=_get_stop_event(args),
    )


def _maybe_run_auto_sync_slugs_before_crawl(args: argparse.Namespace, storage: SQLiteStorage) -> int | None:
    if not _should_auto_sync_game_slugs_before_crawl(storage):
        return None

    logging.info("auto sync-slugs before crawl db=%s", args.db)
    with _logging_command_context("sync-slugs"):
        exit_code = run_sync_slugs(_build_auto_sync_slugs_args(args))
    if exit_code != 0:
        logging.warning("sync-slugs before crawl exited with code=%d; crawl aborted", exit_code)
        return exit_code
    return None


def _normalize_search_text(value: object) -> str:
    text = str(value or "").casefold()
    text = re.sub(r"[_\W]+", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def _slug_text(slug: str) -> str:
    return slug.replace("-", " ").replace("_", " ").strip()


def _search_tokens(text: str) -> set[str]:
    normalized = _normalize_search_text(text)
    return {token for token in normalized.split(" ") if token}


def _text_match_score(query_text: str, candidate_text: str) -> float:
    normalized_query = _normalize_search_text(query_text)
    normalized_candidate = _normalize_search_text(candidate_text)
    if not normalized_query or not normalized_candidate:
        return 0.0
    if normalized_query == normalized_candidate:
        return 1.0

    query_tokens = _search_tokens(normalized_query)
    candidate_tokens = _search_tokens(normalized_candidate)
    shared_tokens = query_tokens & candidate_tokens
    union_tokens = query_tokens | candidate_tokens
    jaccard = len(shared_tokens) / len(union_tokens) if union_tokens else 0.0
    coverage = len(shared_tokens) / len(query_tokens) if query_tokens else 0.0
    sequence_ratio = SequenceMatcher(None, normalized_query, normalized_candidate).ratio()

    score = max(sequence_ratio, jaccard * 0.9, coverage * 0.88)
    if normalized_candidate.startswith(normalized_query) or normalized_query.startswith(normalized_candidate):
        score = max(score, 0.9 + (coverage * 0.08))
    elif normalized_query in normalized_candidate or normalized_candidate in normalized_query:
        score = max(score, 0.84 + (coverage * 0.08))
    return min(score, 0.999)


def _score_slug_search_candidate(
    *,
    query: str,
    slug: str,
    title: str | None,
) -> _SlugSearchMatch | None:
    normalized_slug = str(slug).strip()
    if not normalized_slug:
        return None

    normalized_query = _normalize_search_text(query)
    slug_query = normalized_query.replace(" ", "-")
    slug_text = _slug_text(normalized_slug)

    if normalized_query and slug_query == normalized_slug.casefold():
        return _SlugSearchMatch(slug=normalized_slug, title=title, score=1.0, matched_by="slug")

    title_score = _text_match_score(query, title or "")
    slug_score = _text_match_score(query, slug_text)

    matched_by = "title"
    best_score = title_score
    if slug_score > best_score:
        matched_by = "slug"
        best_score = slug_score
    elif title_score > 0 and slug_score == title_score:
        matched_by = "title"

    if matched_by == "title" and title:
        best_score = min(0.999, best_score + 0.01)

    if best_score < _SEARCH_SLUG_MIN_SCORE:
        return None

    return _SlugSearchMatch(
        slug=normalized_slug,
        title=title,
        score=best_score,
        matched_by=matched_by,
    )


def _find_slug_search_matches(
    candidates: Sequence[tuple[str, str | None]],
    query: str,
    *,
    limit: int = _SEARCH_SLUG_MAX_CANDIDATES,
) -> tuple[list[_SlugSearchMatch], int]:
    matches: list[_SlugSearchMatch] = []
    for slug, title in candidates:
        match = _score_slug_search_candidate(query=query, slug=slug, title=title)
        if match is not None:
            matches.append(match)

    matches.sort(
        key=lambda item: (
            -item.score,
            item.title is None,
            len(item.slug),
            item.slug,
        )
    )
    total_matches = len(matches)
    return matches[:max(1, limit)], total_matches


def _select_slug_search_match(matches: Sequence[_SlugSearchMatch]) -> _SlugSearchMatch | None:
    if not matches:
        return None

    best = matches[0]
    second = matches[1] if len(matches) > 1 else None
    if best.score < _SEARCH_SLUG_AUTO_ACCEPT_SCORE:
        return None
    if second is None:
        return best
    if (best.score - second.score) >= _SEARCH_SLUG_AMBIGUITY_MARGIN:
        return best
    return None


def _format_search_slug_match(match: _SlugSearchMatch) -> str:
    details = [f"score={match.score:.3f}", f"matched_by={match.matched_by}"]
    if match.title:
        details.insert(0, f"title={match.title}")
    return f"{match.slug}  # " + " ".join(details)


def run_search_slug(args: argparse.Namespace) -> int:
    query = str(getattr(args, "query", "") or "").strip()
    if not query:
        raise SystemExit("search-slug requires a non-empty query")

    logging.info("search-slug querying local database")
    candidates = load_slug_search_candidates_from_db(args.db)

    logging.info("search-slug matching candidates")
    matches, total_matches = _find_slug_search_matches(candidates, query)

    logging.info("search-slug selecting result")
    selected = _select_slug_search_match(matches)
    if selected is not None:
        logging.info("search-slug matched slug=%s", selected.slug)
        print(selected.slug)
        return 0

    if not matches:
        logging.info("search-slug no match found")
        print(
            f"No slug matched query: {query}\n"
            "Tip: run 'sync-slugs' first to build the local slug index."
        )
        return 2

    if total_matches == 1:
        logging.info("search-slug no confident match found")
        print(f"No confident slug match found for query: {query}")
        print(_format_search_slug_match(matches[0]))
        return 2

    logging.info("search-slug multiple matches found count=%d", total_matches)
    print(f"Multiple possible slugs matched query: {query}")
    for match in matches:
        print(_format_search_slug_match(match))
    return 2


def run_crawl(args: argparse.Namespace) -> int:
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1")
    download_covers = bool(getattr(args, "download_covers", False))
    covers_dir = str(getattr(args, "covers_dir", "data/covers"))
    overwrite_covers = bool(getattr(args, "overwrite_covers", False))
    stop_event = _get_stop_event(args)
    include_critic_reviews, include_user_reviews = _resolve_review_selection(args)

    storage = SQLiteStorage(args.db)
    try:
        auto_sync_exit_code = _maybe_run_auto_sync_slugs_before_crawl(args, storage)
        if auto_sync_exit_code is not None:
            return auto_sync_exit_code

        with _build_client(args) as client:
            scraper = MetacriticScraper(client, storage, stop_event=stop_event)
            result = scraper.crawl_from_sitemaps(
                include_critic_reviews=include_critic_reviews,
                include_user_reviews=include_user_reviews,
                review_page_size=args.review_page_size,
                max_review_pages=args.max_review_pages,
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
        return 130 if result.stopped else 0
    finally:
        storage.close()


def run_crawl_one(args: argparse.Namespace) -> int:
    download_covers = bool(getattr(args, "download_covers", False))
    covers_dir = str(getattr(args, "covers_dir", "data/covers"))
    overwrite_covers = bool(getattr(args, "overwrite_covers", False))
    stop_event = _get_stop_event(args)
    include_critic_reviews, include_user_reviews = _resolve_review_selection(args)

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
                include_critic_reviews=include_critic_reviews,
                include_user_reviews=include_user_reviews,
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


def run_crawl_reviews(args: argparse.Namespace) -> int:
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1")
    stop_event = _get_stop_event(args)
    include_critic_reviews, include_user_reviews = _resolve_review_selection(
        args,
        default_to_both=bool(getattr(args, "default_to_both_reviews", False)),
    )

    storage = SQLiteStorage(args.db)
    try:
        with _build_client(args) as client:
            scraper = MetacriticScraper(client, storage, stop_event=stop_event)
            result = scraper.crawl_reviews_from_games(
                slug=getattr(args, "slug", None),
                include_critic_reviews=include_critic_reviews,
                include_user_reviews=include_user_reviews,
                review_page_size=args.review_page_size,
                max_review_pages=args.max_review_pages,
                concurrency=args.concurrency,
            )
        logging.info(
            "crawl-reviews %s processed=%d critic_reviews=%d user_reviews=%d failed=%d",
            "stopped" if result.stopped else "finished",
            result.slugs_processed,
            result.critic_reviews_saved,
            result.user_reviews_saved,
            len(result.failed_slugs),
        )
        if bool(getattr(args, "print_summary", False)):
            summary = (
                "crawl-reviews summary: processed=%d critic_reviews=%d user_reviews=%d failed=%d"
                % (
                    result.slugs_processed,
                    result.critic_reviews_saved,
                    result.user_reviews_saved,
                    len(result.failed_slugs),
                )
            )
            if result.stopped:
                summary = f"{summary} stopped=1"
            print(summary)
        return 130 if result.stopped else 0
    finally:
        storage.close()


def _sync_slugs_summary_text(
    *,
    processed: int,
    inserted: int,
    updated: int,
    total: int,
    stopped: bool = False,
) -> str:
    summary = (
        "sync-slugs summary: processed=%d inserted=%d updated=%d total=%d"
        % (processed, inserted, updated, total)
    )
    if stopped:
        summary = f"{summary} stopped=1"
    return summary


def run_sync_slugs(args: argparse.Namespace) -> int:
    stop_event = _get_stop_event(args)
    storage = SQLiteStorage(args.db)
    processed = 0
    inserted = 0
    updated = 0
    current_sitemap_url: str | None = None
    current_batch: list[tuple[str, str, str]] = []
    current_seen_slugs: set[str] = set()
    current_total_games = 0
    current_saved_games = 0
    current_inserted_games = 0
    current_updated_games = 0

    def _flush_current_batch() -> None:
        nonlocal processed, inserted, updated
        nonlocal current_saved_games, current_inserted_games, current_updated_games
        if not current_batch:
            return
        one_processed, one_inserted, one_updated = storage.upsert_game_slugs(current_batch)
        processed += one_processed
        inserted += one_inserted
        updated += one_updated
        current_saved_games += one_processed
        current_inserted_games += one_inserted
        current_updated_games += one_updated
        current_batch.clear()

    def _log_current_sitemap(*, stopped: bool = False) -> None:
        if current_sitemap_url is None:
            return
        message = (
            "sync-slugs sitemap=%s total_games=%d saved_games=%d inserted=%d updated=%d"
            if not stopped
            else "sync-slugs sitemap=%s total_games=%d saved_games=%d inserted=%d updated=%d stopped=1"
        )
        logging.info(
            message,
            current_sitemap_url,
            current_total_games,
            current_saved_games,
            current_inserted_games,
            current_updated_games,
        )

    try:
        with _build_client(args) as client:
            try:
                for sitemap_url in client.iter_game_sitemap_urls():
                    _check_stop_requested(stop_event)
                    current_sitemap_url = sitemap_url
                    current_batch = []
                    current_seen_slugs = set()
                    current_total_games = 0
                    current_saved_games = 0
                    current_inserted_games = 0
                    current_updated_games = 0

                    for record in client.iter_game_slug_records_for_sitemap(sitemap_url):
                        _check_stop_requested(stop_event)
                        current_total_games += 1
                        if record.slug in current_seen_slugs:
                            continue
                        current_seen_slugs.add(record.slug)
                        current_batch.append((record.slug, record.game_url, record.sitemap_url))

                        if len(current_batch) >= DEFAULT_SLUG_SYNC_BATCH_SIZE:
                            _flush_current_batch()

                    _flush_current_batch()
                    _log_current_sitemap()
                    current_sitemap_url = None
            except InterruptedError:
                _flush_current_batch()
                _log_current_sitemap(stopped=True)
                total = storage.count_rows("game_slugs")
                logging.info(
                    "sync-slugs stopped processed=%d inserted=%d updated=%d total=%d db=%s",
                    processed,
                    inserted,
                    updated,
                    total,
                    args.db,
                )
                if bool(getattr(args, "print_summary", False)):
                    print(
                        _sync_slugs_summary_text(
                            processed=processed,
                            inserted=inserted,
                            updated=updated,
                            total=total,
                            stopped=True,
                        )
                    )
                return 130
            finally:
                current_sitemap_url = None

        total = storage.count_rows("game_slugs")
        state_value = datetime.now(timezone.utc).isoformat(timespec="seconds")
        storage.set_state(GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY, state_value)
        logging.info(
            "sync-slugs checkpoint state_key=%s state_value=%s",
            GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY,
            state_value,
        )
        logging.info(
            "sync-slugs finished processed=%d inserted=%d updated=%d total=%d db=%s",
            processed,
            inserted,
            updated,
            total,
            args.db,
        )
        if bool(getattr(args, "print_summary", False)):
            print(
                _sync_slugs_summary_text(
                    processed=processed,
                    inserted=inserted,
                    updated=updated,
                    total=total,
                )
            )
        return 0
    finally:
        storage.close()


def run_export_excel(args: argparse.Namespace) -> int:
    counts = export_sqlite_to_excel(
        db_path=args.db,
        output_path=args.output,
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
    stop_event = _get_stop_event(args)
    storage = SQLiteStorage(args.db)
    try:
        rows = storage.list_game_cover_urls(slug=getattr(args, "slug", None))
        with _build_client(args) as client:
            downloader = CoverImageDownloader(
                fetch_binary=client.fetch_binary,
                output_dir=args.covers_dir,
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
                    "download-covers stopped total=%d downloaded=%d skipped=%d failed=%d covers_dir=%s",
                    len(rows),
                    downloaded,
                    skipped,
                    failed,
                    args.covers_dir,
                )
                return 130

        logging.info(
            "download-covers finished total=%d downloaded=%d skipped=%d failed=%d covers_dir=%s",
            len(rows),
            downloaded,
            skipped,
            failed,
            args.covers_dir,
        )
        return 0 if failed == 0 else 2
    finally:
        storage.close()


def _clear_db_summary_text(counts: dict[str, int]) -> str:
    total = sum(counts.values())
    return (
        "clear-db summary: critic_reviews=%d user_reviews=%d games=%d game_slugs=%d sync_state=%d total=%d"
        % (
            counts.get("critic_reviews", 0),
            counts.get("user_reviews", 0),
            counts.get("games", 0),
            counts.get("game_slugs", 0),
            counts.get("sync_state", 0),
            total,
        )
    )


def _validate_existing_project_db_for_clear(db_path: str) -> str | None:
    normalized_db_path = str(db_path).strip()
    if not normalized_db_path:
        return "clear-db requires a non-empty database path."
    if not os.path.isfile(normalized_db_path):
        return f"clear-db requires an existing project database file: {normalized_db_path}"

    try:
        conn = sqlite3.connect(normalized_db_path)
        try:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        return f"clear-db cannot open project database {normalized_db_path}: {exc}"

    existing_tables = {str(row[0]) for row in rows}
    missing_tables = [table_name for table_name in APP_TABLE_NAMES if table_name not in existing_tables]
    if missing_tables:
        return (
            "clear-db requires an initialized project database; missing tables: "
            + ", ".join(missing_tables)
        )

    return None


def run_clear_db(args: argparse.Namespace) -> int:
    validation_error = _validate_existing_project_db_for_clear(args.db)
    if validation_error is not None:
        logging.error(validation_error)
        return 2

    storage = SQLiteStorage(args.db)
    try:
        counts = storage.clear_all_tables()
        logging.info("%s db=%s", _clear_db_summary_text(counts), args.db)
        if bool(getattr(args, "print_summary", False)):
            print(_clear_db_summary_text(counts))
        return 0
    finally:
        storage.close()


def _interactive_defaults() -> dict[str, object]:
    return {
        "db": "data/gamecritic.db",
        "include_critic_reviews": False,
        "include_user_reviews": False,
        "review_page_size": 50,
        "max_review_pages": DEFAULT_QUICKSTART_MAX_REVIEW_PAGES,
        "concurrency": DEFAULT_CONCURRENCY,
        "timeout": DEFAULT_TIMEOUT_SECONDS,
        "max_retries": DEFAULT_MAX_RETRIES,
        "backoff": DEFAULT_BACKOFF_SECONDS,
        "delay": DEFAULT_DELAY_SECONDS,
        "download_covers": False,
        "covers_dir": "data/covers",
        "overwrite_covers": False,
        "export_output": "data/excel/gamecritic_export.xlsx",
    }


def _coerce_loaded_setting_value(key: str, value: object) -> object:
    if key in _BOOL_SETTING_KEYS:
        if isinstance(value, bool):
            return value
        raise ValueError(f"invalid boolean setting: {key}")
    if key in _INT_SETTING_KEYS:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        raise ValueError(f"invalid integer setting: {key}")
    if key in _FLOAT_SETTING_KEYS:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        raise ValueError(f"invalid float setting: {key}")
    if key in _OPTIONAL_INT_SETTING_KEYS:
        if value is None:
            return None
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        raise ValueError(f"invalid optional integer setting: {key}")
    if key in _STRING_SETTING_KEYS:
        if isinstance(value, str):
            return value
        raise ValueError(f"invalid string setting: {key}")
    raise KeyError(f"unknown setting key: {key}")


def _load_shared_settings() -> dict[str, object]:
    settings = _interactive_defaults()
    if not os.path.isfile(SHARED_SETTINGS_PATH):
        return settings

    try:
        with open(SHARED_SETTINGS_PATH, "r", encoding="utf-8") as fp:
            raw_settings = json.load(fp)
    except (OSError, ValueError, TypeError):
        return settings

    if not isinstance(raw_settings, dict):
        return settings

    for key, value in raw_settings.items():
        if key not in settings:
            continue
        try:
            settings[key] = _coerce_loaded_setting_value(str(key), value)
        except (KeyError, ValueError):
            continue
    return settings


def _save_shared_settings(settings: dict[str, object]) -> None:
    directory = os.path.dirname(SHARED_SETTINGS_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(SHARED_SETTINGS_PATH, "w", encoding="utf-8") as fp:
        json.dump(settings, fp, ensure_ascii=True, indent=2, sort_keys=True)
        fp.write("\n")


def _build_crawl_namespace(
    settings: dict[str, object],
    *,
    print_summary: bool = False,
    stop_event: threading.Event | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        command="crawl",
        db=str(settings["db"]),
        include_critic_reviews=bool(settings["include_critic_reviews"]),
        include_user_reviews=bool(settings["include_user_reviews"]),
        review_page_size=int(settings["review_page_size"]),
        max_review_pages=settings["max_review_pages"],
        concurrency=int(settings["concurrency"]),
        timeout=float(settings["timeout"]),
        max_retries=int(settings["max_retries"]),
        backoff=float(settings["backoff"]),
        delay=float(settings["delay"]),
        download_covers=bool(settings["download_covers"]),
        covers_dir=str(settings["covers_dir"]),
        overwrite_covers=bool(settings["overwrite_covers"]),
        print_summary=print_summary,
        stop_event=stop_event,
    )


def _build_crawl_one_namespace(
    settings: dict[str, object],
    *,
    slug: str,
    print_summary: bool = False,
    stop_event: threading.Event | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        command="crawl-one",
        slug=slug,
        db=str(settings["db"]),
        include_critic_reviews=bool(settings["include_critic_reviews"]),
        include_user_reviews=bool(settings["include_user_reviews"]),
        review_page_size=int(settings["review_page_size"]),
        max_review_pages=settings["max_review_pages"],
        timeout=float(settings["timeout"]),
        max_retries=int(settings["max_retries"]),
        backoff=float(settings["backoff"]),
        delay=float(settings["delay"]),
        download_covers=bool(settings["download_covers"]),
        covers_dir=str(settings["covers_dir"]),
        overwrite_covers=bool(settings["overwrite_covers"]),
        print_summary=print_summary,
        stop_event=stop_event,
    )


def _build_search_slug_namespace(
    settings: dict[str, object],
    *,
    query: str,
) -> argparse.Namespace:
    return argparse.Namespace(
        command="search-slug",
        db=str(settings["db"]),
        query=query,
    )


def _build_crawl_reviews_namespace(
    settings: dict[str, object],
    *,
    slug: str | None = None,
    print_summary: bool = False,
    stop_event: threading.Event | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        command="crawl-reviews",
        slug=slug,
        db=str(settings["db"]),
        include_critic_reviews=True,
        include_user_reviews=True,
        review_page_size=int(settings["review_page_size"]),
        max_review_pages=settings["max_review_pages"],
        concurrency=int(settings["concurrency"]),
        timeout=float(settings["timeout"]),
        max_retries=int(settings["max_retries"]),
        backoff=float(settings["backoff"]),
        delay=float(settings["delay"]),
        print_summary=print_summary,
        stop_event=stop_event,
    )


def _build_sync_slugs_namespace(
    settings: dict[str, object],
    *,
    print_summary: bool = False,
    stop_event: threading.Event | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        command="sync-slugs",
        db=str(settings["db"]),
        timeout=float(settings["timeout"]),
        max_retries=int(settings["max_retries"]),
        backoff=float(settings["backoff"]),
        delay=float(settings["delay"]),
        print_summary=print_summary,
        stop_event=stop_event,
    )


def _build_export_excel_namespace(
    settings: dict[str, object],
    *,
    output: str | None = None,
) -> argparse.Namespace:
    resolved_output = output if output is not None else str(settings["export_output"])
    return argparse.Namespace(
        command="export-excel",
        db=str(settings["db"]),
        output=resolved_output,
    )


def _build_download_covers_namespace(
    settings: dict[str, object],
    *,
    slug: str | None = None,
    stop_event: threading.Event | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        command="download-covers",
        slug=slug,
        db=str(settings["db"]),
        covers_dir=str(settings["covers_dir"]),
        overwrite=bool(settings["overwrite_covers"]),
        timeout=float(settings["timeout"]),
        max_retries=int(settings["max_retries"]),
        backoff=float(settings["backoff"]),
        delay=float(settings["delay"]),
        stop_event=stop_event,
    )


def _build_clear_db_namespace(
    settings: dict[str, object],
    *,
    print_summary: bool = False,
) -> argparse.Namespace:
    return argparse.Namespace(
        command="clear-db",
        db=str(settings["db"]),
        print_summary=print_summary,
    )


def _parse_bool(raw: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError("expected boolean value (true/false)")


def _convert_setting_value(key: str, raw_value: str) -> object:
    value = raw_value.strip()
    if key in _BOOL_SETTING_KEYS:
        return _parse_bool(value)
    if key in _INT_SETTING_KEYS:
        return int(value)
    if key in _FLOAT_SETTING_KEYS:
        return float(value)
    if key in _OPTIONAL_INT_SETTING_KEYS:
        return None if value.lower() in {"none", "null", ""} else int(value)
    if key in _STRING_SETTING_KEYS:
        return value
    raise KeyError(f"unknown setting key: {key}")


def _build_interactive_help_lines(language: str = "en") -> list[str]:
    heading = INTERACTIVE_HELP_TITLE_EN if language == "en" else INTERACTIVE_HELP_TITLE_ZH
    subtitle = INTERACTIVE_HELP_SUBTITLE_EN if language == "en" else INTERACTIVE_HELP_SUBTITLE_ZH
    examples_heading = INTERACTIVE_HELP_EXAMPLES_LABEL_EN if language == "en" else INTERACTIVE_HELP_EXAMPLES_LABEL_ZH
    examples = INTERACTIVE_HELP_EXAMPLES_EN if language == "en" else INTERACTIVE_HELP_EXAMPLES_ZH
    lines = [heading, subtitle, ""]
    for section_en, section_zh, commands in INTERACTIVE_HELP_SECTIONS:
        section = section_en if language == "en" else section_zh
        lines.append(f"[{section}]")
        for command, english, chinese in commands:
            description = english if language == "en" else chinese
            lines.append(f"  {command.ljust(INTERACTIVE_HELP_LABEL_WIDTH)} {description}")
        lines.append("")
    lines.append(f"[{examples_heading}]")
    lines.extend(f"  gamecritic> {example}" for example in _sample_interactive_help_examples(examples))
    return lines


def _print_interactive_help() -> str:
    return "\n".join(_build_interactive_help_lines())


def _print_interactive_help_zh() -> str:
    return "\n".join(_build_interactive_help_lines("zh"))


def _ordered_setting_keys(settings: dict[str, object]) -> list[str]:
    ordered_keys = [key for key in INTERACTIVE_SETTINGS_DISPLAY_ORDER if key in settings]
    ordered_keys.extend(sorted(key for key in settings if key not in INTERACTIVE_SETTINGS_DISPLAY_ORDER))
    return ordered_keys


def _sample_interactive_help_examples(examples: Sequence[str]) -> list[str]:
    if len(examples) <= INTERACTIVE_HELP_SAMPLE_SIZE:
        return list(examples)
    sampled_examples = random.sample(list(examples), INTERACTIVE_HELP_SAMPLE_SIZE)
    example_positions = {example: index for index, example in enumerate(examples)}
    return sorted(sampled_examples, key=example_positions.__getitem__)


def _setting_explanations_en() -> dict[str, str]:
    return {
        "backoff": "Retry backoff base; larger values increase wait time growth on retries",
        "concurrency": "Number of concurrent crawl workers (1 means serial)",
        "covers_dir": "Output directory for downloaded cover image files",
        "db": "Path to the SQLite database file",
        "delay": "Fixed delay in seconds between requests",
        "download_covers": "Whether to download cover image files during crawl",
        "export_output": "Default output path for Excel export",
        "include_critic_reviews": "Whether to also crawl critic reviews while fetching game data",
        "include_user_reviews": "Whether to also crawl user reviews while fetching game data",
        "max_retries": "Maximum retry attempts for failed requests",
        "max_review_pages": "Maximum review pages per review type",
        "overwrite_covers": "Whether to overwrite existing cover image files",
        "review_page_size": "Review API page size",
        "timeout": "HTTP timeout in seconds per request",
    }


def _format_settings(settings: dict[str, object]) -> str:
    explanations = _setting_explanations_en()
    return "\n".join(
        f"{key} = {settings[key]}  # {explanations.get(key, 'Explanation pending')}"
        for key in _ordered_setting_keys(settings)
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
        "include_critic_reviews": "抓取游戏数据时是否同时抓取媒体评论",
        "include_user_reviews": "抓取游戏数据时是否同时抓取用户评论",
        "max_retries": "请求失败后的最大重试次数",
        "max_review_pages": "每类评论最多翻页数",
        "overwrite_covers": "下载封面时是否覆盖本地已有文件",
        "review_page_size": "评论接口每页抓取条数",
        "timeout": "单次 HTTP 请求超时秒数",
    }


def _format_settings_zh(settings: dict[str, object]) -> str:
    explanations = _setting_explanations_zh()
    return "\n".join(
        f"{key} = {settings[key]}  # {explanations.get(key, '参数说明待补充')}"
        for key in _ordered_setting_keys(settings)
    )


def _style_output_line(line: str) -> list[tuple[str, str]]:
    if line.startswith("gamecritic>"):
        return [("class:prompt", line)]

    if re.match(r"^[a-z]+(?:-[a-z]+)* summary:", line):
        match = re.match(r"^([a-z]+(?:-[a-z]+)* summary:)\s*(.*)$", line)
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
        rf"^(?P<bullet>{re.escape(LOG_BULLET)}\s+)?(?P<header>(?P<label>\S+)(?:\s+(?P<progress>\S+))?)\s+-\s+(?P<message>.*)$",
        line,
    )
    if log_match:
        bullet = log_match.group("bullet") or ""
        label = log_match.group("label") or ""
        header = f"{log_match.group('header')} - "
        message = log_match.group("message")
        level = None
        if label in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            level = label
        else:
            suffix_match = re.match(r"^.+-(DEBUG|INFO|WARNING|ERROR|CRITICAL)$", label)
            if suffix_match:
                level = suffix_match.group(1)
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
    if lines and lines[0] in {INTERACTIVE_HELP_TITLE_EN, INTERACTIVE_HELP_TITLE_ZH}:
        fragments: list[tuple[str, str]] = []
        help_titles = {INTERACTIVE_HELP_TITLE_EN, INTERACTIVE_HELP_TITLE_ZH}
        help_subtitles = {INTERACTIVE_HELP_SUBTITLE_EN, INTERACTIVE_HELP_SUBTITLE_ZH}
        for idx, line in enumerate(lines):
            if idx:
                fragments.append(("", "\n"))
            if not line:
                continue
            if line in help_titles:
                fragments.append(("class:help.title", line))
                continue
            if line in help_subtitles:
                fragments.append(("class:help.subtitle", line))
                continue
            if re.match(r"^\[[^\]]+\]$", line):
                fragments.append(("class:help.section", line))
                continue
            if line.startswith("  gamecritic> "):
                fragments.extend(
                    [
                        ("", "  "),
                        ("class:prompt", "gamecritic> "),
                        ("class:help.example", line[len("  gamecritic> "):]),
                    ]
                )
                continue
            if line.startswith("  "):
                label_text = line[2 : 2 + INTERACTIVE_HELP_LABEL_WIDTH]
                detail_text = line[2 + INTERACTIVE_HELP_LABEL_WIDTH :].lstrip()
                fragments.extend(
                    [
                        ("", "  "),
                        ("class:help.command", label_text),
                    ]
                )
                if detail_text:
                    fragments.extend([("", " "), ("class:help.text", detail_text)])
                continue
            fragments.append(("", line))
        return fragments

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
        ("item", "crawl", "Run a crawl with the current settings"),
        ("item", "search-slug <game_name>", "Resolve a game name to the best local slug match"),
        ("item", "crawl-one <slug>", "Fetch one game immediately"),
        ("item", "crawl-reviews [slug]", "Backfill reviews for games already stored in SQLite"),
        ("item", "show", "Inspect the active configuration"),
        ("item", "stop", "Request stop for the current background crawl/download task"),
        ("item", "help or help-zh", "Show English or Chinese help and usage examples"),
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


def _interactive_help_hint_text() -> str:
    return "Type 'help' or 'help-zh'"


def _format_interactive_game_slugs_updated_at(value: object) -> str:
    if value is None:
        return "never"

    parsed = _parse_checkpoint_datetime(value)
    if parsed is not None:
        return parsed.date().isoformat()

    normalized = str(value).strip()
    return normalized or "never"


def _interactive_game_slugs_status_text(db_path: str) -> str:
    normalized_db_path = str(db_path).strip()
    if not normalized_db_path or not os.path.exists(normalized_db_path):
        return "games total=0 | game_slugs total=0 | last full sync=never"

    try:
        conn = sqlite3.connect(normalized_db_path)
        try:
            try:
                games_row = conn.execute("SELECT COUNT(*) FROM games").fetchone()
            except sqlite3.Error as exc:
                if "no such table" in str(exc).lower():
                    games_row = (0,)
                else:
                    raise

            try:
                total_row = conn.execute("SELECT COUNT(*) FROM game_slugs").fetchone()
            except sqlite3.Error as exc:
                if "no such table" in str(exc).lower():
                    total_row = (0,)
                else:
                    raise

            try:
                state_row = conn.execute(
                    "SELECT state_value FROM sync_state WHERE state_key = ?",
                    (GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY,),
                ).fetchone()
            except sqlite3.Error as exc:
                if "no such table" in str(exc).lower():
                    state_row = None
                else:
                    raise
        finally:
            conn.close()
    except sqlite3.Error as exc:
        if "no such table" in str(exc).lower():
            return "games total=0 | game_slugs total=0 | last full sync=never"
        return "games total=unavailable | game_slugs total=unavailable | last full sync=unavailable"

    games_total = int((games_row or (0,))[0] or 0)
    total = int((total_row or (0,))[0] or 0)
    last_updated = _format_interactive_game_slugs_updated_at(state_row[0] if state_row else None)
    return f"games total={games_total} | game_slugs total={total} | last full sync={last_updated}"


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
    with _logging_command_context(getattr(namespace, "command", None)):
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
    refresh_game_slugs_status: Callable[[], None] | None = None,
    stop_event: threading.Event | None = None,
) -> bool:
    def _refresh_status_if_needed() -> None:
        if refresh_game_slugs_status is not None:
            refresh_game_slugs_status()

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
        try:
            _save_shared_settings(settings)
        except OSError as exc:
            emit(f"Settings reset, but failed to save shared settings: {exc}")
            return True
        _refresh_status_if_needed()
        emit("Settings reset.")
        return True
    if cmd == "clear-db":
        if args:
            emit("Usage: clear-db")
            return True
        ns = _build_clear_db_namespace(settings, print_summary=True)
        try:
            _run_with_captured_stdout(run_clear_db, ns, emit)
        finally:
            _refresh_status_if_needed()
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
        try:
            _save_shared_settings(settings)
        except OSError as exc:
            emit(f"Updated in memory only: {key}={value} (save failed: {exc})")
            return True
        if key == "db":
            _refresh_status_if_needed()
        emit(f"Updated: {key}={value}")
        return True

    try:
        if cmd == "crawl":
            ns = _build_crawl_namespace(settings, print_summary=True, stop_event=stop_event)
            try:
                _run_with_captured_stdout(run_crawl, ns, emit)
            finally:
                _refresh_status_if_needed()
            return True

        if cmd == "crawl-one":
            if not args:
                emit("Usage: crawl-one <slug>")
                return True
            slug = args[0]
            ns = _build_crawl_one_namespace(settings, slug=slug, print_summary=True, stop_event=stop_event)
            _run_with_captured_stdout(run_crawl_one, ns, emit)
            return True

        if cmd == "search-slug":
            if not args:
                emit("Usage: search-slug <game_name>")
                return True
            ns = _build_search_slug_namespace(settings, query=" ".join(args))
            _run_with_captured_stdout(run_search_slug, ns, emit)
            return True

        if cmd == "crawl-reviews":
            if len(args) > 1:
                emit("Usage: crawl-reviews [slug]")
                return True
            slug = args[0] if args else None
            ns = _build_crawl_reviews_namespace(settings, slug=slug, print_summary=True, stop_event=stop_event)
            _run_with_captured_stdout(run_crawl_reviews, ns, emit)
            return True

        if cmd == "download-covers":
            if len(args) > 1:
                emit("Usage: download-covers [slug]")
                return True
            slug = args[0] if args else None
            ns = _build_download_covers_namespace(settings, slug=slug, stop_event=stop_event)
            _run_with_captured_stdout(run_download_covers, ns, emit)
            return True

        if cmd == "sync-slugs":
            ns = _build_sync_slugs_namespace(settings, print_summary=True, stop_event=stop_event)
            try:
                _run_with_captured_stdout(run_sync_slugs, ns, emit)
            finally:
                _refresh_status_if_needed()
            return True

        if cmd == "export-excel":
            output = args[0] if args else settings["export_output"]
            ns = _build_export_excel_namespace(settings, output=str(output))
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
        self.setFormatter(_ProgressAwareFormatter(LOG_FORMAT))

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
            line = input("gamecritic> ").strip()
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
    settings = _load_shared_settings()
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return _run_interactive_plain(settings)

    try:
        from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
        from prompt_toolkit import PromptSession, print_formatted_text
        from prompt_toolkit.cursor_shapes import CursorShape
        from prompt_toolkit.filters import is_done
        from prompt_toolkit.formatted_text import FormattedText
        from prompt_toolkit.history import InMemoryHistory
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.layout import Window
        from prompt_toolkit.layout.containers import ConditionalContainer
        from prompt_toolkit.layout.controls import FormattedTextControl
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
            "help.title": "bold ansibrightcyan",
            "help.subtitle": "ansibrightblack",
            "help.section": "bold ansibrightgreen",
            "help.command": "bold ansiyellow",
            "help.text": "ansiwhite",
            "help.example": "ansibrightblue",
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
            "toolbar.status": "ansibrightblack",
        }
    )

    previous_no_cpr = os.environ.get("PROMPT_TOOLKIT_NO_CPR")
    os.environ["PROMPT_TOOLKIT_NO_CPR"] = "1"

    status_state = {"text": _interactive_game_slugs_status_text(str(settings["db"]))}

    class _InteractivePromptSession(PromptSession):
        def _create_layout(self):
            layout = super()._create_layout()
            container = getattr(layout, "container", None)
            children = getattr(container, "children", None)
            if isinstance(children, list):
                children.insert(
                    1,
                    ConditionalContainer(
                        Window(
                            FormattedTextControl(
                                lambda: [("class:toolbar.status", f"  {status_state['text']}")]
                            ),
                            dont_extend_height=True,
                            height=1,
                        ),
                        filter=~is_done,
                    ),
                )
            return layout

    session = _InteractivePromptSession(history=InMemoryHistory())

    kb = KeyBindings()

    def _refresh_interactive_game_slugs_status() -> None:
        status_state["text"] = _interactive_game_slugs_status_text(str(settings["db"]))
        app = getattr(session, "app", None)
        invalidate = getattr(app, "invalidate", None)
        if callable(invalidate):
            invalidate()

    def emit_output(message: str) -> None:
        with output_lock:
            print_formatted_text(
                FormattedText(_style_output_text(str(message))),
                style=output_style,
            )

    def _request_stop_current_command() -> str:
        if not _interactive_command_is_running(running_command):
            return "No background command is running."

        command_name = str(running_command.get("name") or "")
        if command_name not in INTERACTIVE_STOPPABLE_COMMANDS:
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
                    refresh_game_slugs_status=_refresh_interactive_game_slugs_status,
                    stop_event=stop_event,
                )
            finally:
                stop_event.clear()
                running_command["thread"] = None
                running_command["name"] = None

        running_command["name"] = command_name
        worker_context = copy_context()
        worker = threading.Thread(
            target=worker_context.run,
            args=(_worker,),
            name=f"interactive-{command_name}",
            daemon=True,
        )
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
                        [("class:prompt", "gamecritic> ")],
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
                if cmd in INTERACTIVE_BACKGROUND_COMMANDS:
                    _run_command_in_background(tokens)
                    continue

                if not _run_interactive_command(
                    tokens,
                    settings,
                    emit_output,
                    request_stop=_request_stop_current_command,
                    refresh_game_slugs_status=_refresh_interactive_game_slugs_status,
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
    settings = _load_shared_settings()

    if args.command is None or args.command == "interactive":
        with _logging_command_context("interactive"):
            return run_interactive()
    if args.command == "crawl":
        with _logging_command_context(args.command):
            return run_crawl(_build_crawl_namespace(settings))
    if args.command == "crawl-one":
        with _logging_command_context(args.command):
            return run_crawl_one(_build_crawl_one_namespace(settings, slug=args.slug))
    if args.command == "search-slug":
        with _logging_command_context(args.command):
            return run_search_slug(_build_search_slug_namespace(settings, query=" ".join(args.query)))
    if args.command == "crawl-reviews":
        with _logging_command_context(args.command):
            return run_crawl_reviews(_build_crawl_reviews_namespace(settings, slug=args.slug))
    if args.command == "sync-slugs":
        with _logging_command_context(args.command):
            return run_sync_slugs(_build_sync_slugs_namespace(settings))
    if args.command == "export-excel":
        with _logging_command_context(args.command):
            return run_export_excel(_build_export_excel_namespace(settings))
    if args.command == "download-covers":
        with _logging_command_context(args.command):
            return run_download_covers(_build_download_covers_namespace(settings, slug=args.slug))
    if args.command == "clear-db":
        with _logging_command_context(args.command):
            return run_clear_db(_build_clear_db_namespace(settings))
    parser.error(f"unknown command: {args.command}")
    return 2
