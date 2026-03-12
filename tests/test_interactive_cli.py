import argparse
import logging
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from gamecritic.cli import (
    DEFAULT_CONCURRENCY,
    DEFAULT_QUICKSTART_MAX_REVIEW_PAGES,
    INTERACTIVE_BACKGROUND_COMMANDS,
    GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY,
    INTERACTIVE_WELCOME_CONTENT_WIDTH,
    INTERACTIVE_WELCOME_TITLE,
    LOG_BULLET,
    _InteractiveLogHandler,
    _build_clear_db_namespace,
    _build_crawl_namespace,
    _build_crawl_reviews_namespace,
    _build_download_covers_namespace,
    _build_search_slug_namespace,
    _build_sync_slugs_namespace,
    build_parser,
    _convert_setting_value,
    _interactive_banner_lines,
    _interactive_command_is_running,
    _interactive_game_slugs_status_text,
    _interactive_help_hint_text,
    _interactive_title_art_lines,
    _interactive_defaults,
    _load_shared_settings,
    _logging_command_context,
    _parse_bool,
    _refresh_interactive_cursor_blink,
    _run_interactive_command,
    _run_with_captured_stdout,
    _style_output_text,
    _style_output_line,
    main,
    run_crawl,
    run_crawl_reviews,
    run_clear_db,
    run_download_covers,
    run_search_slug,
    run_sync_slugs,
)
from gamecritic.client import MetacriticClientError
from gamecritic.scraper import CrawlResult, MetacriticScraper
from gamecritic.storage import SQLiteStorage


class InteractiveCliParsingTestCase(unittest.TestCase):
    def test_interactive_crawl_enables_print_summary(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["command"] = getattr(namespace, "command", None)
            captured["print_summary"] = getattr(namespace, "print_summary", None)
            emit("[done] exit_code=0")

        with patch("gamecritic.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["crawl"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_crawl")
        self.assertEqual(captured.get("command"), "crawl")
        self.assertTrue(captured.get("print_summary"))

    def test_interactive_crawl_one_enables_print_summary(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["command"] = getattr(namespace, "command", None)
            captured["print_summary"] = getattr(namespace, "print_summary", None)
            emit("[done] exit_code=0")

        with patch("gamecritic.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["crawl-one", "demo-game"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_crawl_one")
        self.assertEqual(captured.get("command"), "crawl-one")
        self.assertTrue(captured.get("print_summary"))

    def test_run_with_captured_stdout_streams_lines(self) -> None:
        output: list[str] = []

        def _func(_: argparse.Namespace) -> int:
            print("line-1")
            print("line-2")
            return 0

        _run_with_captured_stdout(_func, argparse.Namespace(), output.append)
        self.assertEqual(output, ["line-1", "line-2", "[done] exit_code=0"])

    def test_run_with_captured_stdout_uses_namespace_command_for_logs(self) -> None:
        output: list[str] = []
        handler = _InteractiveLogHandler(output.append)
        root_logger = logging.getLogger()
        previous_handlers = list(root_logger.handlers)
        previous_level = root_logger.level
        for existing_handler in previous_handlers:
            root_logger.removeHandler(existing_handler)
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        def _func(_: argparse.Namespace) -> int:
            logging.info("crawl started")
            logging.warning("partial failure")
            return 0

        try:
            _run_with_captured_stdout(_func, argparse.Namespace(command="crawl"), output.append)
        finally:
            root_logger.removeHandler(handler)
            for existing_handler in previous_handlers:
                root_logger.addHandler(existing_handler)
            root_logger.setLevel(previous_level)

        self.assertIn(f"{LOG_BULLET} crawl - crawl started\n", output)
        self.assertIn(f"{LOG_BULLET} crawl-WARNING - partial failure\n", output)
        self.assertIn("[done] exit_code=0", output)

    def test_concurrent_crawl_worker_logs_use_active_command_name(self) -> None:
        class _ClientForConcurrentWorkerLogs:
            def fetch_product(self, slug: str) -> dict:
                if slug == "broken":
                    raise MetacriticClientError("product failed")
                return {"data": {"item": {"id": 1, "title": slug, "platform": "PC"}}}

            def resolve_cover_url(self, *, product_payload: dict) -> str | None:
                del product_payload
                return None

            def fetch_score_summary(self, slug: str, review_type: str) -> dict | None:
                if slug == "alpha" and review_type == "critic":
                    raise MetacriticClientError("critic summary failed")
                return None

        output: list[str] = []
        handler = _InteractiveLogHandler(output.append)
        root_logger = logging.getLogger()
        previous_handlers = list(root_logger.handlers)
        previous_level = root_logger.level
        for existing_handler in previous_handlers:
            root_logger.removeHandler(existing_handler)
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game_slugs(
                    [
                        ("alpha", "https://www.metacritic.com/game/alpha/", "https://www.metacritic.com/sitemap-1.xml"),
                        ("broken", "https://www.metacritic.com/game/broken/", "https://www.metacritic.com/sitemap-1.xml"),
                    ]
                )

                scraper = MetacriticScraper(_ClientForConcurrentWorkerLogs(), storage)
                with _logging_command_context("crawl"):
                    result = scraper.crawl_from_sitemaps(
                        include_critic_reviews=False,
                        include_user_reviews=False,
                        review_page_size=50,
                        max_review_pages=1,
                        concurrency=2,
                    )
            finally:
                storage.close()
                root_logger.removeHandler(handler)
                for existing_handler in previous_handlers:
                    root_logger.addHandler(existing_handler)
                root_logger.setLevel(previous_level)

        self.assertEqual(result.games_crawled, 1)
        self.assertEqual(result.failed_slugs, ["broken"])
        self.assertTrue(any("crawl-WARNING - critic summary unavailable for alpha" in line for line in output))
        self.assertTrue(any("crawl-ERROR - failed fetching product for broken" in line for line in output))
        self.assertFalse(any("command-WARNING" in line for line in output))
        self.assertFalse(any("command-ERROR" in line for line in output))

    def test_parse_bool(self) -> None:
        self.assertTrue(_parse_bool("true"))
        self.assertTrue(_parse_bool("YES"))
        self.assertFalse(_parse_bool("false"))
        self.assertFalse(_parse_bool("0"))
        with self.assertRaises(ValueError):
            _parse_bool("maybe")

    def test_convert_setting_value_optional(self) -> None:
        self.assertEqual(_convert_setting_value("concurrency", "4"), 4)
        self.assertEqual(_convert_setting_value("delay", "0.5"), 0.5)
        self.assertTrue(_convert_setting_value("include_critic_reviews", "true"))
        self.assertFalse(_convert_setting_value("include_user_reviews", "false"))
        self.assertTrue(_convert_setting_value("download_covers", "true"))
        self.assertFalse(_convert_setting_value("overwrite_covers", "false"))
        self.assertEqual(_convert_setting_value("covers_dir", "data/covers"), "data/covers")

    def test_quickstart_defaults_for_crawl_parser(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["crawl"])
        self.assertEqual(args.command, "crawl")
        self.assertEqual(set(vars(args)), {"verbose", "command"})

    def test_crawl_parser_rejects_removed_per_command_options(self) -> None:
        parser = build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["crawl", "--db", "data/other.db"])

    def test_crawl_help_does_not_expose_removed_per_command_options(self) -> None:
        parser = build_parser()
        crawl_parser = next(
            action.choices["crawl"]
            for action in parser._actions
            if hasattr(action, "choices") and isinstance(action.choices, dict) and "crawl" in action.choices
        )
        help_text = crawl_parser.format_help()
        self.assertNotIn("--include-critic-reviews", help_text)
        self.assertNotIn("--db", help_text)

    def test_interactive_defaults_use_quickstart_profile(self) -> None:
        settings = _interactive_defaults()
        self.assertFalse(settings["include_critic_reviews"])
        self.assertFalse(settings["include_user_reviews"])
        self.assertEqual(settings["max_review_pages"], DEFAULT_QUICKSTART_MAX_REVIEW_PAGES)
        self.assertEqual(settings["concurrency"], DEFAULT_CONCURRENCY)
        self.assertFalse(settings["download_covers"])
        self.assertEqual(settings["covers_dir"], "data/covers")
        self.assertFalse(settings["overwrite_covers"])
        self.assertEqual(settings["export_output"], "data/excel/gamecritic_export.xlsx")
        self.assertNotIn("include_raw_json", settings)

    def test_set_command_persists_shared_settings(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "gamecritic.cli.SHARED_SETTINGS_PATH",
            str(Path(tmpdir) / "cli_settings.json"),
        ):
            keep_running = _run_interactive_command(["set", "concurrency", "8"], settings, output.append)
            loaded = _load_shared_settings()

        self.assertTrue(keep_running)
        self.assertEqual(output, ["Updated: concurrency=8"])
        self.assertEqual(settings["concurrency"], 8)
        self.assertEqual(loaded["concurrency"], 8)

    def test_main_uses_loaded_shared_settings_for_noninteractive_crawl(self) -> None:
        settings = _interactive_defaults()
        settings["db"] = "data/custom.db"
        settings["concurrency"] = 8
        settings["download_covers"] = True

        with patch("gamecritic.cli._load_shared_settings", return_value=settings), patch(
            "gamecritic.cli.run_crawl",
            return_value=0,
        ) as run_crawl:
            exit_code = main(["crawl"])

        self.assertEqual(exit_code, 0)
        dispatched_args = run_crawl.call_args.args[0]
        self.assertEqual(dispatched_args.db, "data/custom.db")
        self.assertEqual(dispatched_args.concurrency, 8)
        self.assertTrue(dispatched_args.download_covers)

    def test_main_uses_loaded_shared_settings_for_noninteractive_search_slug(self) -> None:
        settings = _interactive_defaults()
        settings["db"] = "data/custom.db"

        with patch("gamecritic.cli._load_shared_settings", return_value=settings), patch(
            "gamecritic.cli.run_search_slug",
            return_value=0,
        ) as run_search_slug_mock:
            exit_code = main(["search-slug", "Elden", "Ring"])

        self.assertEqual(exit_code, 0)
        dispatched_args = run_search_slug_mock.call_args.args[0]
        self.assertEqual(dispatched_args.db, "data/custom.db")
        self.assertEqual(dispatched_args.query, "Elden Ring")

    def test_crawl_one_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["crawl-one", "demo-game"])
        self.assertEqual(args.command, "crawl-one")
        self.assertEqual(args.slug, "demo-game")
        self.assertEqual(set(vars(args)), {"verbose", "command", "slug"})

    def test_search_slug_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["search-slug", "Elden", "Ring"])
        self.assertEqual(args.command, "search-slug")
        self.assertEqual(args.query, ["Elden", "Ring"])
        self.assertEqual(set(vars(args)), {"verbose", "command", "query"})

    def test_crawl_reviews_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["crawl-reviews"])
        self.assertEqual(args.command, "crawl-reviews")
        self.assertEqual(set(vars(args)), {"verbose", "command"})

    def test_download_covers_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["download-covers"])
        self.assertEqual(args.command, "download-covers")
        self.assertEqual(set(vars(args)), {"verbose", "command"})

    def test_export_excel_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["export-excel"])
        self.assertEqual(args.command, "export-excel")
        self.assertEqual(set(vars(args)), {"verbose", "command"})

    def test_clear_db_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["clear-db"])
        self.assertEqual(args.command, "clear-db")
        self.assertEqual(set(vars(args)), {"verbose", "command"})

    def test_sync_slugs_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["sync-slugs"])
        self.assertEqual(args.command, "sync-slugs")
        self.assertEqual(set(vars(args)), {"verbose", "command"})

    def test_help_zh_command(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["help-zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("交互命令（中文释义）", output[0])
        self.assertIn("help | help-zh", output[0])
        self.assertIn("show | show-zh", output[0])
        self.assertIn("crawl-reviews", output[0])
        self.assertIn("search-slug", output[0])
        self.assertIn("clear-db", output[0])
        self.assertIn("请求停止当前后台抓取/下载任务", output[0])

    def test_help_with_zh_argument(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["help", "zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("交互命令（中文释义）", output[0])

    def test_help_command_describes_stop_scope(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["help"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("help | help-zh", output[0])
        self.assertIn("show | show-zh", output[0])
        self.assertIn("crawl-reviews", output[0])
        self.assertIn("search-slug", output[0])
        self.assertIn("clear-db", output[0])
        self.assertIn("current background crawl/download task", output[0])

    def test_show_zh_command(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["show-zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("concurrency = 4", output[0])
        self.assertIn("并发抓取 worker 数量", output[0])
        self.assertIn("抓取游戏数据时是否同时抓取媒体评论", output[0])

    def test_show_with_zh_argument(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["show", "zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("db = data/gamecritic.db", output[0])
        self.assertIn("SQLite 数据库文件路径", output[0])

    def test_show_command_includes_english_explanations(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["show"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("concurrency = 4", output[0])
        self.assertIn("Number of concurrent crawl workers", output[0])
        self.assertIn("Whether to also crawl critic reviews while fetching game data", output[0])

    def test_config_alias_includes_english_explanations(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["config"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("db = data/gamecritic.db", output[0])
        self.assertIn("Path to the SQLite database file", output[0])

    def test_clear_command_not_available_by_default(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["clear"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("Unknown command: clear", output[0])

    def test_interactive_clear_db_enables_print_summary(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["command"] = getattr(namespace, "command", None)
            captured["print_summary"] = getattr(namespace, "print_summary", None)
            captured["db"] = getattr(namespace, "db", None)
            emit("[done] exit_code=0")

        with patch("gamecritic.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["clear-db"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_clear_db")
        self.assertEqual(captured.get("command"), "clear-db")
        self.assertTrue(captured.get("print_summary"))
        self.assertEqual(captured.get("db"), "data/gamecritic.db")

    def test_interactive_search_slug_runs_with_joined_query(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["command"] = getattr(namespace, "command", None)
            captured["query"] = getattr(namespace, "query", None)
            emit("[done] exit_code=0")

        with patch("gamecritic.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["search-slug", "Elden", "Ring"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_search_slug")
        self.assertEqual(captured.get("command"), "search-slug")
        self.assertEqual(captured.get("query"), "Elden Ring")

    def test_interactive_search_slug_requires_query(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []

        keep_running = _run_interactive_command(["search-slug"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(output, ["Usage: search-slug <game_name>"])

    def test_interactive_clear_db_rejects_extra_args(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []

        keep_running = _run_interactive_command(["clear-db", "now"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(output, ["Usage: clear-db"])

    def test_stop_command_without_running_background_task(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []

        keep_running = _run_interactive_command(["stop"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(output, ["No background command is running."])

    def test_stop_command_uses_request_stop_callback(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []

        keep_running = _run_interactive_command(
            ["stop"],
            settings,
            output.append,
            request_stop=lambda: "[stopping] requested stop for crawl",
        )

        self.assertTrue(keep_running)
        self.assertEqual(output, ["[stopping] requested stop for crawl"])

    def test_interactive_crawl_passes_stop_event(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}
        stop_event = threading.Event()

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["command"] = getattr(namespace, "command", None)
            captured["stop_event"] = getattr(namespace, "stop_event", None)
            emit("[done] exit_code=130")

        with patch("gamecritic.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(
                ["crawl"],
                settings,
                output.append,
                stop_event=stop_event,
            )

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_crawl")
        self.assertEqual(captured.get("command"), "crawl")
        self.assertIs(captured.get("stop_event"), stop_event)

    def test_interactive_sync_slugs_enables_print_summary(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["command"] = getattr(namespace, "command", None)
            captured["print_summary"] = getattr(namespace, "print_summary", None)
            emit("[done] exit_code=0")

        with patch("gamecritic.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["sync-slugs"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_sync_slugs")
        self.assertEqual(captured.get("command"), "sync-slugs")
        self.assertTrue(captured.get("print_summary"))

    def test_interactive_crawl_reviews_enables_print_summary(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["command"] = getattr(namespace, "command", None)
            captured["include_critic_reviews"] = getattr(namespace, "include_critic_reviews", None)
            captured["include_user_reviews"] = getattr(namespace, "include_user_reviews", None)
            captured["print_summary"] = getattr(namespace, "print_summary", None)
            captured["stop_event"] = getattr(namespace, "stop_event", None)
            emit("[done] exit_code=0")

        stop_event = threading.Event()
        with patch("gamecritic.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["crawl-reviews"], settings, output.append, stop_event=stop_event)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_crawl_reviews")
        self.assertEqual(captured.get("command"), "crawl-reviews")
        self.assertTrue(captured.get("include_critic_reviews"))
        self.assertTrue(captured.get("include_user_reviews"))
        self.assertTrue(captured.get("print_summary"))
        self.assertIs(captured.get("stop_event"), stop_event)

    def test_interactive_crawl_reviews_ignores_crawl_review_toggle_settings(self) -> None:
        settings = _interactive_defaults()
        settings["include_critic_reviews"] = False
        settings["include_user_reviews"] = False
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["include_critic_reviews"] = getattr(namespace, "include_critic_reviews", None)
            captured["include_user_reviews"] = getattr(namespace, "include_user_reviews", None)
            emit("[done] exit_code=0")

        with patch("gamecritic.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["crawl-reviews"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(captured.get("include_critic_reviews"))
        self.assertTrue(captured.get("include_user_reviews"))

    def test_run_crawl_returns_130_when_scraper_stops(self) -> None:
        args = _build_crawl_namespace(
            _interactive_defaults(),
            print_summary=False,
            stop_event=threading.Event(),
        )

        storage = MagicMock()
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        scraper = MagicMock()
        scraper.crawl_from_sitemaps.return_value = CrawlResult(stopped=True)

        with patch("gamecritic.cli.SQLiteStorage", return_value=storage), patch(
            "gamecritic.cli._build_client",
            return_value=client,
        ), patch("gamecritic.cli.MetacriticScraper", return_value=scraper) as scraper_cls:
            exit_code = run_crawl(args)

        self.assertEqual(exit_code, 130)
        self.assertIs(scraper_cls.call_args.kwargs["stop_event"], args.stop_event)
        scraper.crawl_from_sitemaps.assert_called_once()
        storage.close.assert_called_once()

    def test_run_crawl_does_not_log_failed_slugs_at_end(self) -> None:
        args = _build_crawl_namespace(
            _interactive_defaults(),
            print_summary=False,
            stop_event=threading.Event(),
        )

        storage = MagicMock()
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        scraper = MagicMock()
        scraper.crawl_from_sitemaps.return_value = CrawlResult(failed_slugs=["demo-a", "demo-b"])

        with patch("gamecritic.cli.SQLiteStorage", return_value=storage), patch(
            "gamecritic.cli._maybe_run_auto_sync_slugs_before_crawl",
            return_value=None,
        ), patch(
            "gamecritic.cli._build_client",
            return_value=client,
        ), patch("gamecritic.cli.MetacriticScraper", return_value=scraper), self.assertLogs(
            level="INFO"
        ) as captured:
            exit_code = run_crawl(args)

        self.assertEqual(exit_code, 0)
        messages = [record.getMessage() for record in captured.records]
        self.assertIn(
            "crawl finished games=0 critic_reviews=0 user_reviews=0 covers_downloaded=0 covers_skipped=0 covers_failed=0 failed=2",
            messages,
        )
        self.assertFalse(any("failed slugs:" in message for message in messages))
        storage.close.assert_called_once()

    def test_run_crawl_reviews_defaults_to_both_review_types_when_interactive_settings_disable_both(self) -> None:
        args = argparse.Namespace(
            db="data/gamecritic.db",
            include_critic_reviews=False,
            include_user_reviews=False,
            review_page_size=50,
            max_review_pages=1,
            concurrency=4,
            timeout=10.0,
            max_retries=2,
            backoff=0.5,
            delay=0.0,
            print_summary=False,
            default_to_both_reviews=True,
            stop_event=threading.Event(),
        )

        storage = MagicMock()
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        scraper = MagicMock()
        scraper.crawl_reviews_from_games.return_value = CrawlResult()

        with patch("gamecritic.cli.SQLiteStorage", return_value=storage), patch(
            "gamecritic.cli._build_client",
            return_value=client,
        ), patch("gamecritic.cli.MetacriticScraper", return_value=scraper):
            exit_code = run_crawl_reviews(args)

        self.assertEqual(exit_code, 0)
        scraper.crawl_reviews_from_games.assert_called_once_with(
            include_critic_reviews=True,
            include_user_reviews=True,
            review_page_size=50,
            max_review_pages=1,
            concurrency=4,
        )
        storage.close.assert_called_once()

    def test_run_crawl_reviews_returns_130_when_scraper_stops(self) -> None:
        args = _build_crawl_reviews_namespace(
            _interactive_defaults(),
            print_summary=False,
            stop_event=threading.Event(),
        )

        storage = MagicMock()
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        scraper = MagicMock()
        scraper.crawl_reviews_from_games.return_value = CrawlResult(stopped=True)

        with patch("gamecritic.cli.SQLiteStorage", return_value=storage), patch(
            "gamecritic.cli._build_client",
            return_value=client,
        ), patch("gamecritic.cli.MetacriticScraper", return_value=scraper) as scraper_cls:
            exit_code = run_crawl_reviews(args)

        self.assertEqual(exit_code, 130)
        self.assertIs(scraper_cls.call_args.kwargs["stop_event"], args.stop_event)
        scraper.crawl_reviews_from_games.assert_called_once()
        storage.close.assert_called_once()

    def test_run_download_covers_returns_130_when_fetch_is_interrupted(self) -> None:
        args = _build_download_covers_namespace(
            _interactive_defaults(),
            stop_event=threading.Event(),
        )

        storage = MagicMock()
        storage.list_game_cover_urls.return_value = [("demo-game", "https://cdn.example.com/path/cover.jpg")]
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None

        def _fetch_binary(_: str) -> bytes:
            raise InterruptedError("stopped by user")

        client.fetch_binary = _fetch_binary

        with patch("gamecritic.cli.SQLiteStorage", return_value=storage), patch(
            "gamecritic.cli._build_client",
            return_value=client,
        ):
            exit_code = run_download_covers(args)

        self.assertEqual(exit_code, 130)
        storage.close.assert_called_once()

    def test_run_clear_db_prints_summary_and_closes_storage(self) -> None:
        args = _build_clear_db_namespace(_interactive_defaults(), print_summary=True)

        storage = MagicMock()
        storage.clear_all_tables.return_value = {
            "critic_reviews": 3,
            "user_reviews": 2,
            "games": 1,
            "game_slugs": 4,
            "sync_state": 1,
        }

        with patch("gamecritic.cli._validate_existing_project_db_for_clear", return_value=None), patch(
            "gamecritic.cli.SQLiteStorage",
            return_value=storage,
        ), patch("builtins.print") as print_mock:
            exit_code = run_clear_db(args)

        self.assertEqual(exit_code, 0)
        print_mock.assert_called_once_with(
            "clear-db summary: critic_reviews=3 user_reviews=2 games=1 game_slugs=4 sync_state=1 total=11"
        )
        storage.close.assert_called_once()

    def test_run_search_slug_prints_exact_title_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game(
                    slug="elden-ring",
                    product_payload={"data": {"item": {"id": 1, "title": "Elden Ring", "platform": "PC"}}},
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url=None,
                )
            finally:
                storage.close()

            settings = _interactive_defaults()
            settings["db"] = str(db_path)
            args = _build_search_slug_namespace(settings, query="Elden Ring")

            with self.assertLogs(level="INFO") as captured, patch("builtins.print") as print_mock:
                exit_code = run_search_slug(args)

        self.assertEqual(exit_code, 0)
        print_mock.assert_called_once_with("elden-ring")
        messages = [record.getMessage() for record in captured.records]
        self.assertIn("search-slug querying local database", messages)
        self.assertIn("search-slug matching candidates", messages)
        self.assertIn("search-slug selecting result", messages)
        self.assertIn("search-slug matched slug=elden-ring", messages)

    def test_run_search_slug_prints_candidate_list_when_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game(
                    slug="resident-evil-4",
                    product_payload={"data": {"item": {"id": 1, "title": "Resident Evil 4", "platform": "PC"}}},
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url=None,
                )
                storage.upsert_game(
                    slug="resident-evil-village",
                    product_payload={
                        "data": {"item": {"id": 2, "title": "Resident Evil Village", "platform": "PC"}}
                    },
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url=None,
                )
            finally:
                storage.close()

            settings = _interactive_defaults()
            settings["db"] = str(db_path)
            args = _build_search_slug_namespace(settings, query="Resident Evil")

            with patch("builtins.print") as print_mock:
                exit_code = run_search_slug(args)

        self.assertEqual(exit_code, 2)
        printed_lines = [call.args[0] for call in print_mock.call_args_list]
        self.assertEqual(printed_lines[0], "Multiple possible slugs matched query: Resident Evil")
        self.assertTrue(any(line.startswith("resident-evil-4  # ") for line in printed_lines[1:]))
        self.assertTrue(any(line.startswith("resident-evil-village  # ") for line in printed_lines[1:]))

    def test_run_search_slug_does_not_auto_select_same_title_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game(
                    slug="portal",
                    product_payload={"data": {"item": {"id": 1, "title": "Portal", "platform": "PC"}}},
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url=None,
                )
                storage.upsert_game(
                    slug="portal-remastered",
                    product_payload={"data": {"item": {"id": 2, "title": "Portal", "platform": "PS5"}}},
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url=None,
                )
            finally:
                storage.close()

            settings = _interactive_defaults()
            settings["db"] = str(db_path)
            args = _build_search_slug_namespace(settings, query="Portal")

            with patch("builtins.print") as print_mock:
                exit_code = run_search_slug(args)

        self.assertEqual(exit_code, 2)
        printed_lines = [call.args[0] for call in print_mock.call_args_list]
        self.assertEqual(printed_lines[0], "Multiple possible slugs matched query: Portal")
        self.assertTrue(any(line.startswith("portal  # ") for line in printed_lines[1:]))
        self.assertTrue(any(line.startswith("portal-remastered  # ") for line in printed_lines[1:]))

    def test_run_search_slug_prints_low_confidence_single_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game(
                    slug="resident-evil-village",
                    product_payload={
                        "data": {"item": {"id": 1, "title": "Resident Evil Village", "platform": "PC"}}
                    },
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url=None,
                )
            finally:
                storage.close()

            settings = _interactive_defaults()
            settings["db"] = str(db_path)
            args = _build_search_slug_namespace(settings, query="RE Village DLC")

            with patch("builtins.print") as print_mock:
                exit_code = run_search_slug(args)

        self.assertEqual(exit_code, 2)
        printed_lines = [call.args[0] for call in print_mock.call_args_list]
        self.assertEqual(printed_lines[0], "No confident slug match found for query: RE Village DLC")
        self.assertEqual(len(printed_lines), 2)
        self.assertTrue(printed_lines[1].startswith("resident-evil-village  # "))

    def test_run_search_slug_recommends_sync_when_no_match_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            settings = _interactive_defaults()
            settings["db"] = str(db_path)
            args = _build_search_slug_namespace(settings, query="Unknown Game")

            with patch("builtins.print") as print_mock:
                exit_code = run_search_slug(args)

        self.assertEqual(exit_code, 2)
        print_mock.assert_called_once_with(
            "No slug matched query: Unknown Game\nTip: run 'sync-slugs' first to build the local slug index."
        )
        self.assertFalse(db_path.exists())

    def test_run_clear_db_rejects_missing_db_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "missing.db"
            settings = _interactive_defaults()
            settings["db"] = str(db_path)
            args = _build_clear_db_namespace(settings, print_summary=True)

            with patch("gamecritic.cli.logging.error") as error_log:
                exit_code = run_clear_db(args)

            self.assertEqual(exit_code, 2)
            self.assertFalse(db_path.exists())
            error_log.assert_called_once_with(
                f"clear-db requires an existing project database file: {db_path}"
            )

    def test_run_clear_db_rejects_uninitialized_sqlite_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "other.db"
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
                conn.commit()
            finally:
                conn.close()

            settings = _interactive_defaults()
            settings["db"] = str(db_path)
            args = _build_clear_db_namespace(settings, print_summary=True)

            with patch("gamecritic.cli.logging.error") as error_log:
                exit_code = run_clear_db(args)

            self.assertEqual(exit_code, 2)
            error_log.assert_called_once()
            self.assertIn("clear-db requires an initialized project database; missing tables:", error_log.call_args.args[0])

            conn = sqlite3.connect(db_path)
            try:
                tables = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                    ).fetchall()
                }
            finally:
                conn.close()
            self.assertEqual(tables, {"unrelated"})

    def test_run_sync_slugs_returns_130_when_stop_is_requested(self) -> None:
        stop_event = threading.Event()
        stop_event.set()
        args = _build_sync_slugs_namespace(
            _interactive_defaults(),
            stop_event=stop_event,
        )

        storage = MagicMock()
        storage.upsert_game_slugs.return_value = (0, 0, 0)
        storage.count_rows.return_value = 0
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        client.iter_game_sitemap_urls.return_value = iter(["https://example.com/sitemap.xml"])
        client.iter_game_slug_records_for_sitemap.return_value = iter(
            [MagicMock(slug="demo", game_url="https://example.com", sitemap_url="https://example.com/sitemap.xml")]
        )

        with patch("gamecritic.cli.SQLiteStorage", return_value=storage), patch(
            "gamecritic.cli._build_client",
            return_value=client,
        ):
            exit_code = run_sync_slugs(args)

        self.assertEqual(exit_code, 130)
        storage.set_state.assert_not_called()
        storage.close.assert_called_once()

    def test_interactive_banner_lines(self) -> None:
        lines = _interactive_banner_lines()
        banner_text = "\n".join(lines)
        self.assertGreaterEqual(len(lines), 10)
        self.assertFalse(any("╭" in line or "╰" in line or "│" in line for line in lines))
        self.assertEqual(lines[0].strip(), INTERACTIVE_WELCOME_TITLE)
        self.assertIn("Crawl games, export Excel", banner_text)
        self.assertIn("Quick Start", banner_text)
        self.assertIn("Input Tips", banner_text)
        self.assertIn("help or help-zh", banner_text)
        self.assertIn("crawl/download task", banner_text)
        self.assertIn("crawl-one <slug>", banner_text)
        self.assertIn("search-slug <game_name>", banner_text)
        self.assertIn("crawl-reviews", banner_text)
        self.assertIn("Up / Down", banner_text)

    def test_interactive_title_art_lines(self) -> None:
        lines = _interactive_title_art_lines()
        self.assertEqual(lines, [INTERACTIVE_WELCOME_TITLE.center(INTERACTIVE_WELCOME_CONTENT_WIDTH)])

    def test_interactive_help_hint_text(self) -> None:
        self.assertEqual(_interactive_help_hint_text(), "Type 'help' or 'help-zh'")

    def test_clear_db_runs_as_background_interactive_command(self) -> None:
        self.assertIn("clear-db", INTERACTIVE_BACKGROUND_COMMANDS)

    def test_crawl_reviews_runs_as_background_interactive_command(self) -> None:
        self.assertIn("crawl-reviews", INTERACTIVE_BACKGROUND_COMMANDS)

    def test_search_slug_runs_as_background_interactive_command(self) -> None:
        self.assertIn("search-slug", INTERACTIVE_BACKGROUND_COMMANDS)

    def test_interactive_game_slugs_status_text_uses_sync_state_update_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game(
                    slug="demo-game",
                    product_payload={"data": {"item": {"id": 1, "title": "Demo"}}},
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url=None,
                )
                storage.upsert_game_slugs(
                    [("demo-game", "https://example.com/game/demo-game", "https://example.com/games.xml")]
                )
                storage.conn.execute(
                    "UPDATE game_slugs SET last_seen_at = ? WHERE slug = ?",
                    ("2026-03-11T08:30:00+00:00", "demo-game"),
                )
                storage.conn.commit()
                storage.set_state(
                    GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY,
                    "2026-03-11T09:45:00+00:00",
                )
            finally:
                storage.close()

            self.assertEqual(
                _interactive_game_slugs_status_text(str(db_path)),
                "games total=1 | game_slugs total=1 | last full sync=2026-03-11",
            )

    def test_interactive_game_slugs_status_text_handles_missing_sync_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game(
                    slug="demo-game",
                    product_payload={"data": {"item": {"id": 1, "title": "Demo"}}},
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url=None,
                )
                storage.upsert_game_slugs(
                    [("demo-game", "https://example.com/game/demo-game", "https://example.com/games.xml")]
                )
                storage.conn.execute(
                    "UPDATE game_slugs SET last_seen_at = ? WHERE slug = ?",
                    ("2026-03-11T08:30:00+00:00", "demo-game"),
                )
                storage.conn.commit()
            finally:
                storage.close()

            self.assertEqual(
                _interactive_game_slugs_status_text(str(db_path)),
                "games total=1 | game_slugs total=1 | last full sync=never",
            )

    def test_interactive_game_slugs_status_text_handles_missing_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_db_path = Path(tmpdir) / "missing.db"
            self.assertEqual(
                _interactive_game_slugs_status_text(str(missing_db_path)),
                "games total=0 | game_slugs total=0 | last full sync=never",
            )

    def test_interactive_command_is_running_clears_finished_thread(self) -> None:
        worker = threading.Thread(target=lambda: None)
        worker.start()
        worker.join()
        state: dict[str, object | None] = {"thread": worker, "name": "crawl"}

        self.assertFalse(_interactive_command_is_running(state))
        self.assertIsNone(state["thread"])
        self.assertIsNone(state["name"])

    def test_refresh_interactive_cursor_blink_invalidates_renderer_cache(self) -> None:
        class _Renderer:
            def __init__(self) -> None:
                self._last_cursor_shape = "BLINKING_BEAM"

        class _App:
            def __init__(self) -> None:
                self.renderer = _Renderer()
                self.invalidated = False

            def invalidate(self) -> None:
                self.invalidated = True

        app = _App()
        _refresh_interactive_cursor_blink(app)

        self.assertIsNone(app.renderer._last_cursor_shape)
        self.assertTrue(app.invalidated)

    def test_style_output_line_for_settings(self) -> None:
        fragments = _style_output_line("db = data/gamecritic.db  # Path to the SQLite database file")
        self.assertEqual(
            fragments,
            [
                ("class:settings.key", "db"),
                ("", " = "),
                ("class:settings.value", "data/gamecritic.db"),
                ("class:settings.comment_prefix", "  # "),
                ("class:settings.comment", "Path to the SQLite database file"),
            ],
        )

    def test_style_output_line_for_summary(self) -> None:
        fragments = _style_output_line(
            "crawl summary: games=3 critic_reviews=10 user_reviews=8 covers_downloaded=2 failed=0"
        )
        self.assertEqual(
            fragments,
            [
                ("class:summary.label", "crawl summary:"),
                ("", " "),
                ("class:summary.key", "games"),
                ("", "="),
                ("class:summary.value", "3"),
                ("", " "),
                ("class:summary.key", "critic_reviews"),
                ("", "="),
                ("class:summary.value", "10"),
                ("", " "),
                ("class:summary.key", "user_reviews"),
                ("", "="),
                ("class:summary.value", "8"),
                ("", " "),
                ("class:summary.key", "covers_downloaded"),
                ("", "="),
                ("class:summary.value", "2"),
                ("", " "),
                ("class:summary.key", "failed"),
                ("", "="),
                ("class:summary.value", "0"),
            ],
        )

    def test_style_output_line_for_sync_slugs_summary(self) -> None:
        fragments = _style_output_line("sync-slugs summary: processed=3 inserted=2 updated=1 total=9")
        self.assertEqual(
            fragments,
            [
                ("class:summary.label", "sync-slugs summary:"),
                ("", " "),
                ("class:summary.key", "processed"),
                ("", "="),
                ("class:summary.value", "3"),
                ("", " "),
                ("class:summary.key", "inserted"),
                ("", "="),
                ("class:summary.value", "2"),
                ("", " "),
                ("class:summary.key", "updated"),
                ("", "="),
                ("class:summary.value", "1"),
                ("", " "),
                ("class:summary.key", "total"),
                ("", "="),
                ("class:summary.value", "9"),
            ],
        )

    def test_style_output_line_for_warning_log(self) -> None:
        line = "● crawl-WARNING - failed slugs: demo"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                ("class:log.warning", "crawl-WARNING - "),
                ("", "failed slugs: demo"),
            ],
        )

    def test_style_output_line_for_error_log(self) -> None:
        line = "● crawl-ERROR - request failed"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                ("class:log.error", "crawl-ERROR - "),
                ("", "request failed"),
            ],
        )

    def test_style_output_line_for_cover_download_log(self) -> None:
        line = "● download-covers - download-covers finished total=20 downloaded=18 skipped=2 failed=0 output_dir=data/covers"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                (
                    "class:log.cover",
                    "download-covers - download-covers finished total=20 downloaded=18 skipped=2 failed=0 output_dir=data/covers",
                ),
            ],
        )

    def test_style_output_line_for_progress_log(self) -> None:
        line = "● crawl 99/100 - completed slug=demo-game status=ok"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                ("", "crawl 99/100 - completed slug=demo-game status=ok"),
            ],
        )

    def test_interactive_log_handler_appends_blank_line(self) -> None:
        output: list[str] = []
        handler = _InteractiveLogHandler(output.append)
        record = logging.LogRecord(
            name="gamecritic.scraper",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="crawl started",
            args=(),
            exc_info=None,
        )
        record.command_name = "crawl"

        handler.emit(record)

        self.assertEqual(output, [f"{LOG_BULLET} crawl - crawl started\n"])

    def test_style_output_line_for_non_settings(self) -> None:
        fragments = _style_output_line("Unknown command: clear. Type 'help' or 'help-zh' for available commands.")
        self.assertEqual(
            fragments,
            [("", "Unknown command: clear. Type 'help' or 'help-zh' for available commands.")],
        )

    def test_style_output_text_preserves_line_breaks(self) -> None:
        fragments = _style_output_text("gamecritic> show\ncrawl summary: games=1 failed=0")
        self.assertIn(("class:prompt", "gamecritic> show"), fragments)
        self.assertIn(("", "\n"), fragments)
        self.assertIn(("class:summary.label", "crawl summary:"), fragments)


if __name__ == "__main__":
    unittest.main()
