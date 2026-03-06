import unittest

from metacritic_scraper_py.cli import (
    DEFAULT_QUICKSTART_MAX_GAMES,
    DEFAULT_QUICKSTART_MAX_REVIEW_PAGES,
    build_parser,
    _convert_setting_value,
    _interactive_banner_lines,
    _interactive_defaults,
    _parse_bool,
    _run_interactive_command,
    _style_output_line,
)


class InteractiveCliParsingTestCase(unittest.TestCase):
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

    def test_help_zh_command(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["help-zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("交互命令（中文释义）", output[0])

    def test_help_with_zh_argument(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        keep_running = _run_interactive_command(["help", "zh"], settings, output.append)

        self.assertTrue(keep_running)
        self.assertTrue(output)
        self.assertIn("交互命令（中文释义）", output[0])

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

    def test_clear_command_invokes_clear_callback_when_enabled(self) -> None:
        settings = _interactive_defaults()
        output: list[str] = []
        cleared = {"value": False}

        def _clear() -> None:
            cleared["value"] = True

        keep_running = _run_interactive_command(
            ["clear"],
            settings,
            output.append,
            include_clear=True,
            clear_output=_clear,
        )

        self.assertTrue(keep_running)
        self.assertTrue(cleared["value"])
        self.assertEqual(output, [])

    def test_interactive_banner_lines(self) -> None:
        lines = _interactive_banner_lines()
        self.assertEqual(len(lines), 2)
        self.assertIn("Metacritic Scraper Interactive Shell", lines[0])
        self.assertIn("Type 'help' to see commands.", lines[1])

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

    def test_style_output_line_for_non_settings(self) -> None:
        fragments = _style_output_line("Unknown command: clear. Type 'help' for available commands.")
        self.assertEqual(fragments, [("", "Unknown command: clear. Type 'help' for available commands.")])


if __name__ == "__main__":
    unittest.main()
