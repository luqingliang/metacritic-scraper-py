from __future__ import annotations

import argparse
import io
import logging
import shlex
import sys
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
from .exporter import export_sqlite_to_excel
from .scraper import MetacriticScraper
from .storage import SQLiteStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="metacritic-scraper",
        description="Scrape Metacritic game data into SQLite.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logs.")

    subparsers = parser.add_subparsers(dest="command", required=False)

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


def _interactive_defaults() -> dict[str, object]:
    return {
        "db": "data/metacritic.db",
        "max_games": None,
        "start_slug": None,
        "limit_sitemaps": None,
        "limit_slugs": None,
        "include_reviews": False,
        "review_page_size": 50,
        "max_review_pages": None,
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
    bool_keys = {"include_reviews", "incremental_by_date", "include_raw_json"}
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
    if key in {"db", "incremental_state_key", "export_output"}:
        return value
    raise KeyError(f"unknown setting key: {key}")


def _print_interactive_help() -> str:
    return "\n".join(
        [
            "Interactive commands:",
            "  help                              Show help",
            "  help-zh | 帮助                    Show Chinese annotated help",
            "  show                              Show current session settings",
            "  set <key> <value>                 Update setting (use 'none' for null)",
            "  reset                             Reset settings to defaults",
            "  crawl                             Run crawl with current settings",
            "  crawl-one <slug>                  Crawl one game with current settings",
            "  slugs [output_path]               Print slugs or save to a file",
            "  export-excel [output_path]        Export DB data to Excel",
            "  exit | quit                       Exit interactive shell",
            "",
            "Examples:",
            "  set db data/metacritic.db",
            "  set include_reviews true",
            "  set concurrency 4",
            "  set incremental_by_date true",
            "  crawl",
            "  crawl-one the-legend-of-zelda-breath-of-the-wild",
        ]
    )


def _print_interactive_help_zh() -> str:
    return "\n".join(
        [
            "交互命令（中文释义）:",
            "  help                              显示英文帮助",
            "  help-zh | 帮助                    显示中文释义帮助",
            "  show                              显示当前会话配置",
            "  set <key> <value>                 修改配置（null/none 表示空值）",
            "  reset                             重置为默认配置",
            "  crawl                             用当前配置执行批量抓取",
            "  crawl-one <slug>                  抓取单个游戏",
            "  slugs [output_path]               打印 slug 或写入文件",
            "  export-excel [output_path]        导出 SQLite 数据到 Excel",
            "  exit | quit                       退出交互模式",
            "",
            "示例:",
            "  help-zh",
            "  set include_reviews true",
            "  set concurrency 4",
            "  crawl",
            "  export-excel data/metacritic_export.xlsx",
        ]
    )


def _format_settings(settings: dict[str, object]) -> str:
    return "\n".join(f"{key}={settings[key]}" for key in sorted(settings))


def _run_with_captured_stdout(
    func: Callable[[argparse.Namespace], int],
    namespace: argparse.Namespace,
    emit: Callable[[str], None],
) -> None:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        exit_code = func(namespace)
    output = buffer.getvalue().strip()
    if output:
        emit(output)
    emit(f"[done] exit_code={exit_code}")


def _run_interactive_command(
    tokens: list[str],
    settings: dict[str, object],
    emit: Callable[[str], None],
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
    if cmd in {"show", "config"}:
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
            )
            _run_with_captured_stdout(run_crawl_one, ns, emit)
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
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.layout import HSplit, Layout, Window
        from prompt_toolkit.styles import Style
        from prompt_toolkit.widgets import Frame, TextArea
    except Exception:
        return _run_interactive_plain(settings)

    output_lines: list[str] = []
    max_lines = 5000

    output_box = TextArea(
        text="",
        focusable=False,
        scrollbar=True,
        wrap_lines=False,
    )
    input_box = TextArea(
        height=1,
        prompt="metacritic> ",
        multiline=False,
        wrap_lines=False,
    )

    def append_output(message: str) -> None:
        for line in str(message).splitlines() or [""]:
            output_lines.append(line)
        if len(output_lines) > max_lines:
            del output_lines[: len(output_lines) - max_lines]
        output_box.text = "\n".join(output_lines)
        output_box.buffer.cursor_position = len(output_box.text)

    kb = KeyBindings()

    @kb.add("enter")
    def _(event) -> None:
        line = input_box.text.strip()
        input_box.text = ""
        if not line:
            return
        append_output(f"metacritic> {line}")
        try:
            tokens = shlex.split(line)
        except ValueError as exc:
            append_output(f"Invalid input: {exc}")
            return
        if not _run_interactive_command(tokens, settings, append_output):
            event.app.exit(result=0)

    @kb.add("c-c")
    @kb.add("c-d")
    def _(event) -> None:
        event.app.exit(result=0)

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
        mouse_support=True,
        style=Style.from_dict(
            {
                "frame.label": "bold",
            }
        ),
    )

    root_logger = logging.getLogger()
    previous_handlers = list(root_logger.handlers)
    previous_level = root_logger.level
    for handler in previous_handlers:
        root_logger.removeHandler(handler)
    ui_handler = _InteractiveLogHandler(append_output)
    root_logger.addHandler(ui_handler)
    root_logger.setLevel(previous_level)

    append_output("Metacritic Scraper Interactive Shell")
    append_output("Type 'help' to see commands. Press Ctrl-C/Ctrl-D to exit.")

    try:
        result = app.run()
        return int(result or 0)
    finally:
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
    parser.error(f"unknown command: {args.command}")
    return 2
