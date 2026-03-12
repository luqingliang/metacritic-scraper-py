import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from gamecritic.cli import (
    _build_crawl_namespace,
    _interactive_defaults,
    GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY,
    run_crawl,
)
from gamecritic.scraper import CrawlResult
from gamecritic.storage import SQLiteStorage


class CrawlAutoSyncTestCase(unittest.TestCase):
    def _build_args(self, db_path: Path):
        settings = _interactive_defaults()
        settings["db"] = str(db_path)
        return _build_crawl_namespace(settings, print_summary=False)

    def _set_checkpoint(self, db_path: Path, checkpoint: str | None) -> None:
        storage = SQLiteStorage(db_path)
        try:
            if checkpoint is not None:
                storage.set_state(GAME_SLUGS_LAST_FULL_SYNC_AT_STATE_KEY, checkpoint)
        finally:
            storage.close()

    def test_run_crawl_auto_syncs_when_game_slug_checkpoint_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            stale_checkpoint = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat(timespec="seconds")
            self._set_checkpoint(db_path, stale_checkpoint)
            args = self._build_args(db_path)

            client = MagicMock()
            client.__enter__.return_value = client
            client.__exit__.return_value = None
            scraper = MagicMock()
            scraper.crawl_from_sitemaps.return_value = CrawlResult()

            with patch("gamecritic.cli.run_sync_slugs", return_value=0) as sync_mock, patch(
                "gamecritic.cli._build_client",
                return_value=client,
            ), patch("gamecritic.cli.MetacriticScraper", return_value=scraper):
                exit_code = run_crawl(args)

            self.assertEqual(exit_code, 0)
            sync_mock.assert_called_once()
            sync_args = sync_mock.call_args.args[0]
            self.assertEqual(sync_args.db, str(db_path))
            self.assertFalse(sync_args.print_summary)
            scraper.crawl_from_sitemaps.assert_called_once()

    def test_run_crawl_skips_auto_sync_when_checkpoint_is_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            fresh_checkpoint = (datetime.now(timezone.utc) - timedelta(days=6)).isoformat(timespec="seconds")
            self._set_checkpoint(db_path, fresh_checkpoint)
            args = self._build_args(db_path)

            client = MagicMock()
            client.__enter__.return_value = client
            client.__exit__.return_value = None
            scraper = MagicMock()
            scraper.crawl_from_sitemaps.return_value = CrawlResult()

            with patch("gamecritic.cli.run_sync_slugs", return_value=0) as sync_mock, patch(
                "gamecritic.cli._build_client",
                return_value=client,
            ), patch("gamecritic.cli.MetacriticScraper", return_value=scraper):
                exit_code = run_crawl(args)

            self.assertEqual(exit_code, 0)
            sync_mock.assert_not_called()
            scraper.crawl_from_sitemaps.assert_called_once()

    def test_run_crawl_auto_syncs_when_checkpoint_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            args = self._build_args(db_path)

            client = MagicMock()
            client.__enter__.return_value = client
            client.__exit__.return_value = None
            scraper = MagicMock()
            scraper.crawl_from_sitemaps.return_value = CrawlResult()

            with patch("gamecritic.cli.run_sync_slugs", return_value=0) as sync_mock, patch(
                "gamecritic.cli._build_client",
                return_value=client,
            ), patch("gamecritic.cli.MetacriticScraper", return_value=scraper):
                exit_code = run_crawl(args)

            self.assertEqual(exit_code, 0)
            sync_mock.assert_called_once()
            scraper.crawl_from_sitemaps.assert_called_once()

    def test_run_crawl_aborts_when_auto_sync_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            stale_checkpoint = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat(timespec="seconds")
            self._set_checkpoint(db_path, stale_checkpoint)
            args = self._build_args(db_path)

            with patch("gamecritic.cli.run_sync_slugs", return_value=130) as sync_mock, patch(
                "gamecritic.cli._build_client"
            ) as client_builder, patch("gamecritic.cli.MetacriticScraper") as scraper_cls:
                exit_code = run_crawl(args)

            self.assertEqual(exit_code, 130)
            sync_mock.assert_called_once()
            client_builder.assert_not_called()
            scraper_cls.assert_not_called()


if __name__ == "__main__":
    unittest.main()
