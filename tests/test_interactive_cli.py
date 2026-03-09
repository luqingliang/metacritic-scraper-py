import argparse
import logging
import threading
import unittest
from unittest.mock import MagicMock, patch

from metacritic_scraper_py.cli import (
    DEFAULT_QUICKSTART_MAX_GAMES,
    DEFAULT_QUICKSTART_MAX_REVIEW_PAGES,
    INTERACTIVE_COMPOSER_MAX_LINES,
    INTERACTIVE_COMPOSER_MIN_LINES,
    INTERACTIVE_WELCOME_CONTENT_WIDTH,
    INTERACTIVE_WELCOME_TITLE,
    LOG_BULLET,
    _InteractiveLogHandler,
    build_parser,
    _convert_setting_value,
    _format_interactive_command_echo,
    _interactive_banner_lines,
    _interactive_command_is_running,
    _interactive_composer_height,
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
    run_download_covers,
    run_sync_slugs,
)
from metacritic_scraper_py.scraper import CrawlResult


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
        self.assertIsNone(_convert_setting_value("max_games", "none"))
        self.assertEqual(_convert_setting_value("max_games", "12"), 12)
        self.assertIsNone(_convert_setting_value("since_date", "none"))
        self.assertEqual(_convert_setting_value("since_date", "2026-03-05"), "2026-03-05")
        self.assertTrue(_convert_setting_value("download_covers", "true"))
        self.assertFalse(_convert_setting_value("overwrite_covers", "false"))
        self.assertEqual(_convert_setting_value("covers_dir", "data/covers"), "data/covers")

    def test_quickstart_defaults_for_crawl_parser(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["crawl"])
        self.assertTrue(args.include_reviews)
        self.assertEqual(args.max_review_pages, DEFAULT_QUICKSTART_MAX_REVIEW_PAGES)
        self.assertEqual(args.max_games, DEFAULT_QUICKSTART_MAX_GAMES)
        self.assertFalse(args.download_covers)
        self.assertEqual(args.covers_dir, "data/covers")
        self.assertFalse(args.overwrite_covers)

    def test_crawl_parser_can_disable_default_reviews(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["crawl", "--no-include-reviews"])
        self.assertFalse(args.include_reviews)

    def test_interactive_defaults_use_quickstart_profile(self) -> None:
        settings = _interactive_defaults()
        self.assertTrue(settings["include_reviews"])
        self.assertEqual(settings["max_review_pages"], DEFAULT_QUICKSTART_MAX_REVIEW_PAGES)
        self.assertEqual(settings["max_games"], DEFAULT_QUICKSTART_MAX_GAMES)
        self.assertFalse(settings["download_covers"])
        self.assertEqual(settings["covers_dir"], "data/covers")
        self.assertFalse(settings["overwrite_covers"])

    def test_download_covers_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["download-covers"])
        self.assertEqual(args.db, "data/metacritic.db")
        self.assertEqual(args.output_dir, "data/covers")
        self.assertIsNone(args.limit)
        self.assertFalse(args.overwrite)

    def test_sync_slugs_parser_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["sync-slugs"])
        self.assertEqual(args.db, "data/metacritic.db")
        self.assertIsNone(args.limit_sitemaps)
        self.assertIsNone(args.limit_slugs)

    def test_help_zh_command(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["help-zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("交互命令（中文释义）", output[0])
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
        self.assertIn("current background crawl/download task", output[0])

    def test_show_zh_command(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["show-zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("concurrency = 1", output[0])
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
        self.assertIn("concurrency = 1", output[0])
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

    def test_interactive_composer_height_clamps_to_min_and_max(self) -> None:
        self.assertEqual(_interactive_composer_height(""), INTERACTIVE_COMPOSER_MIN_LINES)
        self.assertEqual(_interactive_composer_height("crawl"), INTERACTIVE_COMPOSER_MIN_LINES)

        tall_text = "\n".join(f"line-{idx}" for idx in range(20))
        self.assertEqual(_interactive_composer_height(tall_text), INTERACTIVE_COMPOSER_MAX_LINES)

    def test_interactive_help_hint_text(self) -> None:
        self.assertEqual(_interactive_help_hint_text(), "Type 'help' or 'help-zh'")

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

    def test_format_interactive_command_echo_preserves_multiline_alignment(self) -> None:
        echoed = _format_interactive_command_echo("set db data/demo.db\nshow")
        self.assertEqual(echoed, "metacritic> set db data/demo.db\n            show")

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
        line = "● WARNING metacritic_scraper_py.scraper - failed slugs: demo"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                ("class:log.warning", "WARNING metacritic_scraper_py.scraper - "),
                ("", "failed slugs: demo"),
            ],
        )

    def test_style_output_line_for_error_log(self) -> None:
        line = "● ERROR metacritic_scraper_py.scraper - request failed"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                ("class:log.error", "ERROR metacritic_scraper_py.scraper - "),
                ("", "request failed"),
            ],
        )

    def test_style_output_line_for_cover_download_log(self) -> None:
        line = "● INFO metacritic_scraper_py.cli - download-covers finished total=20 downloaded=18 skipped=2 failed=0 output_dir=data/covers"
        fragments = _style_output_line(line)
        self.assertEqual(
            fragments,
            [
                ("class:log.bullet", "● "),
                (
                    "class:log.cover",
                    "INFO metacritic_scraper_py.cli - download-covers finished total=20 downloaded=18 skipped=2 failed=0 output_dir=data/covers",
                ),
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

        self.assertEqual(output, [f"{LOG_BULLET} INFO metacritic_scraper_py.scraper - crawl started\n"])

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
