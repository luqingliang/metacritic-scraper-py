import unittest

from metacritic_scraper_py.cli import (
    _convert_setting_value,
    _interactive_defaults,
    _parse_bool,
    _run_interactive_command,
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


if __name__ == "__main__":
    unittest.main()
