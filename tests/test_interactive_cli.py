import argparse
import logging
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from metacritic_scraper_py.cli import (
    DEFAULT_CONCURRENCY,
    DEFAULT_QUICKSTART_MAX_REVIEW_PAGES,
    INTERACTIVE_BACKGROUND_COMMANDS,
    GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY,
    INTERACTIVE_WELCOME_CONTENT_WIDTH,
    INTERACTIVE_WELCOME_TITLE,
    LOG_BULLET,
    _InteractiveLogHandler,
    build_parser,
    _convert_setting_value,
    _interactive_banner_lines,
    _interactive_command_is_running,
    _interactive_game_slugs_status_text,
    _interactive_help_hint_text,
    _interactive_title_art_lines,
    _interactive_defaults,
    _parse_bool,
    _refresh_interactive_cursor_blink,
    _run_interactive_command,
    _run_with_captured_stdout,
    _style_output_text,
    _style_output_line,
    run_crawl,
    run_clear_db,
    run_download_covers,
    run_sync_slugs,
)
from metacritic_scraper_py.scraper import CrawlResult
from metacritic_scraper_py.storage import SQLiteStorage


class InteractiveCliParsingTestCase(unittest.TestCase):
    def test_interactive_crawl_enables_print_summary(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["print_summary"] = getattr(namespace, "print_summary", None)
            emit("[done] exit_code=0")

        with patch("metacritic_scraper_py.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["crawl"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_crawl")
        self.assertTrue(captured.get("print_summary"))

    def test_interactive_crawl_one_enables_print_summary(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["print_summary"] = getattr(namespace, "print_summary", None)
            emit("[done] exit_code=0")

        with patch("metacritic_scraper_py.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["crawl-one", "demo-game"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_crawl_one")
        self.assertTrue(captured.get("print_summary"))

    def test_run_with_captured_stdout_streams_lines(self) -> None:
        output: list[str] = []

        def _func(_: argparse.Namespace) -> int:
            print("line-1")
            print("line-2")
            return 0

        _run_with_captured_stdout(_func, argparse.Namespace(), output.append)
        self.assertEqual(output, ["line-1", "line-2", "[done] exit_code=0"])

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
        self.assertFalse(args.include_critic_reviews)
        self.assertFalse(args.include_user_reviews)
        self.assertEqual(args.max_review_pages, DEFAULT_QUICKSTART_MAX_REVIEW_PAGES)
        self.assertEqual(args.concurrency, DEFAULT_CONCURRENCY)
        self.assertFalse(args.download_covers)
        self.assertEqual(args.covers_dir, "data/covers")
        self.assertFalse(args.overwrite_covers)

    def test_crawl_parser_can_enable_review_types_separately(self) -> None:
        parser = build_parser()
        critic_args = parser.parse_args(["crawl", "--include-critic-reviews"])
        user_args = parser.parse_args(["crawl", "--include-user-reviews"])
        self.assertTrue(critic_args.include_critic_reviews)
        self.assertFalse(critic_args.include_user_reviews)
        self.assertFalse(user_args.include_critic_reviews)
        self.assertTrue(user_args.include_user_reviews)

    def test_interactive_defaults_use_quickstart_profile(self) -> None:
        settings = _interactive_defaults()
        self.assertFalse(settings["include_critic_reviews"])
        self.assertFalse(settings["include_user_reviews"])
        self.assertEqual(settings["max_review_pages"], DEFAULT_QUICKSTART_MAX_REVIEW_PAGES)
        self.assertEqual(settings["concurrency"], DEFAULT_CONCURRENCY)
        self.assertFalse(settings["download_covers"])
        self.assertEqual(settings["covers_dir"], "data/covers")
        self.assertFalse(settings["overwrite_covers"])
        self.assertEqual(settings["export_output"], "data/excel/metacritic_export.xlsx")
        self.assertNotIn("include_raw_json", settings)

    def test_crawl_one_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["crawl-one", "demo-game"])
        self.assertFalse(args.include_critic_reviews)
        self.assertFalse(args.include_user_reviews)
        self.assertEqual(args.max_review_pages, DEFAULT_QUICKSTART_MAX_REVIEW_PAGES)

    def test_download_covers_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["download-covers"])
        self.assertEqual(args.db, "data/metacritic.db")
        self.assertEqual(args.output_dir, "data/covers")
        self.assertFalse(args.overwrite)

    def test_export_excel_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["export-excel"])
        self.assertEqual(args.db, "data/metacritic.db")
        self.assertEqual(args.output, "data/excel/metacritic_export.xlsx")
        self.assertFalse(hasattr(args, "include_raw_json"))

    def test_clear_db_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["clear-db"])
        self.assertEqual(args.db, "data/metacritic.db")
        self.assertEqual(args.command, "clear-db")
        self.assertEqual(set(vars(args)), {"verbose", "command", "db"})

    def test_sync_slugs_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["sync-slugs"])
        self.assertEqual(args.db, "data/metacritic.db")
        self.assertEqual(args.command, "sync-slugs")
        self.assertEqual(
            set(vars(args)),
            {"verbose", "command", "db", "timeout", "max_retries", "backoff", "delay"},
        )

    def test_help_zh_command(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["help-zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("交互命令（中文释义）", output[0])
        self.assertIn("help | help-zh", output[0])
        self.assertIn("show | show-zh", output[0])
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

    def test_show_with_zh_argument(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["show", "zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("db = data/metacritic.db", output[0])
        self.assertIn("SQLite 数据库文件路径", output[0])

    def test_show_command_includes_english_explanations(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["show"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("concurrency = 4", output[0])
        self.assertIn("Number of concurrent crawl workers", output[0])

    def test_config_alias_includes_english_explanations(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["config"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("db = data/metacritic.db", output[0])
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
            captured["print_summary"] = getattr(namespace, "print_summary", None)
            captured["db"] = getattr(namespace, "db", None)
            emit("[done] exit_code=0")

        with patch("metacritic_scraper_py.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["clear-db"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_clear_db")
        self.assertTrue(captured.get("print_summary"))
        self.assertEqual(captured.get("db"), "data/metacritic.db")

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
            captured["stop_event"] = getattr(namespace, "stop_event", None)
            emit("[done] exit_code=130")

        with patch("metacritic_scraper_py.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(
                ["crawl"],
                settings,
                output.append,
                stop_event=stop_event,
            )

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_crawl")
        self.assertIs(captured.get("stop_event"), stop_event)

    def test_interactive_sync_slugs_enables_print_summary(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        captured: dict[str, object] = {}

        def _fake_run_with_captured_stdout(func, namespace, emit) -> None:
            captured["func_name"] = getattr(func, "__name__", "")
            captured["print_summary"] = getattr(namespace, "print_summary", None)
            emit("[done] exit_code=0")

        with patch("metacritic_scraper_py.cli._run_with_captured_stdout", side_effect=_fake_run_with_captured_stdout):
            keep_running = _run_interactive_command(["sync-slugs"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertEqual(captured.get("func_name"), "run_sync_slugs")
        self.assertTrue(captured.get("print_summary"))

    def test_run_crawl_returns_130_when_scraper_stops(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["crawl"])
        args.print_summary = False
        args.stop_event = threading.Event()

        storage = MagicMock()
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        scraper = MagicMock()
        scraper.crawl_from_sitemaps.return_value = CrawlResult(stopped=True)

        with patch("metacritic_scraper_py.cli.SQLiteStorage", return_value=storage), patch(
            "metacritic_scraper_py.cli._build_client",
            return_value=client,
        ), patch("metacritic_scraper_py.cli.MetacriticScraper", return_value=scraper) as scraper_cls:
            exit_code = run_crawl(args)

        self.assertEqual(exit_code, 130)
        self.assertIs(scraper_cls.call_args.kwargs["stop_event"], args.stop_event)
        scraper.crawl_from_sitemaps.assert_called_once()
        storage.close.assert_called_once()

    def test_run_crawl_does_not_log_failed_slugs_at_end(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["crawl"])
        args.print_summary = False
        args.stop_event = threading.Event()

        storage = MagicMock()
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        scraper = MagicMock()
        scraper.crawl_from_sitemaps.return_value = CrawlResult(failed_slugs=["demo-a", "demo-b"])

        with patch("metacritic_scraper_py.cli.SQLiteStorage", return_value=storage), patch(
            "metacritic_scraper_py.cli._maybe_run_auto_sync_slugs_before_crawl",
            return_value=None,
        ), patch(
            "metacritic_scraper_py.cli._build_client",
            return_value=client,
        ), patch("metacritic_scraper_py.cli.MetacriticScraper", return_value=scraper), self.assertLogs(
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

    def test_run_download_covers_returns_130_when_fetch_is_interrupted(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["download-covers"])
        args.stop_event = threading.Event()

        storage = MagicMock()
        storage.list_game_cover_urls.return_value = [("demo-game", "https://cdn.example.com/path/cover.jpg")]
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None

        def _fetch_binary(_: str) -> bytes:
            raise InterruptedError("stopped by user")

        client.fetch_binary = _fetch_binary

        with patch("metacritic_scraper_py.cli.SQLiteStorage", return_value=storage), patch(
            "metacritic_scraper_py.cli._build_client",
            return_value=client,
        ):
            exit_code = run_download_covers(args)

        self.assertEqual(exit_code, 130)
        storage.close.assert_called_once()

    def test_run_clear_db_prints_summary_and_closes_storage(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["clear-db"])
        args.print_summary = True

        storage = MagicMock()
        storage.clear_all_tables.return_value = {
            "critic_reviews": 3,
            "user_reviews": 2,
            "games": 1,
            "game_slugs": 4,
            "sync_state": 1,
        }

        with patch("metacritic_scraper_py.cli._validate_existing_project_db_for_clear", return_value=None), patch(
            "metacritic_scraper_py.cli.SQLiteStorage",
            return_value=storage,
        ), patch("builtins.print") as print_mock:
            exit_code = run_clear_db(args)

        self.assertEqual(exit_code, 0)
        print_mock.assert_called_once_with(
            "clear-db summary: critic_reviews=3 user_reviews=2 games=1 game_slugs=4 sync_state=1 total=11"
        )
        storage.close.assert_called_once()

    def test_run_clear_db_rejects_missing_db_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "missing.db"
            parser = build_parser()
            args = parser.parse_args(["clear-db", "--db", str(db_path)])
            args.print_summary = True

            with patch("metacritic_scraper_py.cli.logging.error") as error_log:
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

            parser = build_parser()
            args = parser.parse_args(["clear-db", "--db", str(db_path)])
            args.print_summary = True

            with patch("metacritic_scraper_py.cli.logging.error") as error_log:
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
        parser = build_parser()
        args = parser.parse_args(["sync-slugs"])
        args.stop_event = threading.Event()
        args.stop_event.set()

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

        with patch("metacritic_scraper_py.cli.SQLiteStorage", return_value=storage), patch(
            "metacritic_scraper_py.cli._build_client",
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
        self.assertIn("Up / Down", banner_text)

    def test_interactive_title_art_lines(self) -> None:
        lines = _interactive_title_art_lines()
        self.assertEqual(lines, [INTERACTIVE_WELCOME_TITLE.center(INTERACTIVE_WELCOME_CONTENT_WIDTH)])

    def test_interactive_help_hint_text(self) -> None:
        self.assertEqual(_interactive_help_hint_text(), "Type 'help' or 'help-zh'")

    def test_clear_db_runs_as_background_interactive_command(self) -> None:
        self.assertIn("clear-db", INTERACTIVE_BACKGROUND_COMMANDS)

    def test_interactive_game_slugs_status_text_uses_sync_state_update_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
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
                "game_slugs total=1 | last full sync=2026-03-11 09:45:00+00:00",
            )

    def test_interactive_game_slugs_status_text_handles_missing_sync_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
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
                "game_slugs total=1 | last full sync=never",
            )

    def test_interactive_game_slugs_status_text_handles_missing_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_db_path = Path(tmpdir) / "missing.db"
            self.assertEqual(
                _interactive_game_slugs_status_text(str(missing_db_path)),
                "game_slugs total=0 | last full sync=never",
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
        fragments = _style_output_line("db = data/metacritic.db  # Path to the SQLite database file")
        self.assertEqual(
            fragments,
            [
                ("class:settings.key", "db"),
                ("", " = "),
                ("class:settings.value", "data/metacritic.db"),
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
        line = "● WARNING - failed slugs: demo"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                ("class:log.warning", "WARNING - "),
                ("", "failed slugs: demo"),
            ],
        )

    def test_style_output_line_for_error_log(self) -> None:
        line = "● ERROR - request failed"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                ("class:log.error", "ERROR - "),
                ("", "request failed"),
            ],
        )

    def test_style_output_line_for_cover_download_log(self) -> None:
        line = "● INFO - download-covers finished total=20 downloaded=18 skipped=2 failed=0 output_dir=data/covers"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                (
                    "class:log.cover",
                    "INFO - download-covers finished total=20 downloaded=18 skipped=2 failed=0 output_dir=data/covers",
                ),
            ],
        )

    def test_style_output_line_for_progress_log(self) -> None:
        line = "● INFO 99/100 - completed slug=demo-game status=ok"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                ("", "INFO 99/100 - completed slug=demo-game status=ok"),
            ],
        )

    def test_interactive_log_handler_appends_blank_line(self) -> None:
        output: list[str] = []
        handler = _InteractiveLogHandler(output.append)
        record = logging.LogRecord(
            name="metacritic_scraper_py.scraper",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="crawl started",
            args=(),
            exc_info=None,
        )

        handler.emit(record)

        self.assertEqual(output, [f"{LOG_BULLET} INFO - crawl started\n"])

    def test_style_output_line_for_non_settings(self) -> None:
        fragments = _style_output_line("Unknown command: clear. Type 'help' or 'help-zh' for available commands.")
        self.assertEqual(
            fragments,
            [("", "Unknown command: clear. Type 'help' or 'help-zh' for available commands.")],
        )

    def test_style_output_text_preserves_line_breaks(self) -> None:
        fragments = _style_output_text("metacritic> show\ncrawl summary: games=1 failed=0")
        self.assertIn(("class:prompt", "metacritic> show"), fragments)
        self.assertIn(("", "\n"), fragments)
        self.assertIn(("class:summary.label", "crawl summary:"), fragments)


if __name__ == "__main__":
    unittest.main()
