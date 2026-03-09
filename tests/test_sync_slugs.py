import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from metacritic_scraper_py.cli import build_parser, run_sync_slugs
from metacritic_scraper_py.client import GameSlugRecord
from metacritic_scraper_py.storage import SQLiteStorage


class _FakeSlugClient:
    def __init__(self, records: list[GameSlugRecord]) -> None:
        self._records = records

    def __enter__(self) -> "_FakeSlugClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def iter_game_sitemap_urls(
        self,
        *,
        limit_sitemaps: int | None = None,
    ):
        sitemap_urls = list(dict.fromkeys(record.sitemap_url for record in self._records))
        if limit_sitemaps is not None:
            sitemap_urls = sitemap_urls[:limit_sitemaps]
        for sitemap_url in sitemap_urls:
            yield sitemap_url

    def iter_game_slug_records_for_sitemap(self, sitemap_url: str):
        for record in self._records:
            if record.sitemap_url == sitemap_url:
                yield record


class SyncSlugsStorageTestCase(unittest.TestCase):
    def test_upsert_game_slugs_tracks_inserted_and_updated_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                processed, inserted, updated = storage.upsert_game_slugs(
                    [
                        (
                            "alpha",
                            "https://www.metacritic.com/game/alpha/",
                            "https://www.metacritic.com/sitemap-alpha.xml",
                        ),
                        (
                            "beta",
                            "https://www.metacritic.com/game/beta/",
                            "https://www.metacritic.com/sitemap-beta.xml",
                        ),
                    ]
                )
                self.assertEqual((processed, inserted, updated), (2, 2, 0))

                processed, inserted, updated = storage.upsert_game_slugs(
                    [
                        (
                            "alpha",
                            "https://www.metacritic.com/game/alpha-remastered/",
                            "https://www.metacritic.com/sitemap-remastered.xml",
                        ),
                        (
                            "gamma",
                            "https://www.metacritic.com/game/gamma/",
                            "https://www.metacritic.com/sitemap-gamma.xml",
                        ),
                    ]
                )
                self.assertEqual((processed, inserted, updated), (2, 1, 1))
                self.assertEqual(storage.count_rows("game_slugs"), 3)

                row = storage.conn.execute(
                    "SELECT game_url, sitemap_url, discovered_at, last_seen_at FROM game_slugs WHERE slug = ?",
                    ("alpha",),
                ).fetchone()
                self.assertEqual(row[0], "https://www.metacritic.com/game/alpha-remastered/")
                self.assertEqual(row[1], "https://www.metacritic.com/sitemap-remastered.xml")
                self.assertIsNotNone(row[2])
                self.assertIsNotNone(row[3])
            finally:
                storage.close()


class SyncSlugsCommandTestCase(unittest.TestCase):
    def test_run_sync_slugs_writes_records_to_database(self) -> None:
        records = [
            GameSlugRecord(
                slug="alpha",
                game_url="https://www.metacritic.com/game/alpha/",
                sitemap_url="https://www.metacritic.com/sitemap-1.xml",
            ),
            GameSlugRecord(
                slug="beta",
                game_url="https://www.metacritic.com/game/beta/",
                sitemap_url="https://www.metacritic.com/sitemap-1.xml",
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "slugs.db"
            parser = build_parser()
            args = parser.parse_args(["sync-slugs", "--db", str(db_path)])
            args.print_summary = False

            with patch("metacritic_scraper_py.cli._build_client", return_value=_FakeSlugClient(records)):
                exit_code = run_sync_slugs(args)

            self.assertEqual(exit_code, 0)

            storage = SQLiteStorage(db_path)
            try:
                rows = storage.conn.execute(
                    "SELECT slug, game_url, sitemap_url FROM game_slugs ORDER BY slug ASC"
                ).fetchall()
                self.assertEqual(
                    rows,
                    [
                        ("alpha", "https://www.metacritic.com/game/alpha/", "https://www.metacritic.com/sitemap-1.xml"),
                        ("beta", "https://www.metacritic.com/game/beta/", "https://www.metacritic.com/sitemap-1.xml"),
                    ],
                )
            finally:
                storage.close()

    def test_run_sync_slugs_logs_progress_for_each_sitemap(self) -> None:
        records = [
            GameSlugRecord(
                slug="alpha",
                game_url="https://www.metacritic.com/game/alpha/",
                sitemap_url="https://www.metacritic.com/sitemap-1.xml",
            ),
            GameSlugRecord(
                slug="beta",
                game_url="https://www.metacritic.com/game/beta/",
                sitemap_url="https://www.metacritic.com/sitemap-1.xml",
            ),
            GameSlugRecord(
                slug="gamma",
                game_url="https://www.metacritic.com/game/gamma/",
                sitemap_url="https://www.metacritic.com/sitemap-2.xml",
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "slugs.db"
            parser = build_parser()
            args = parser.parse_args(["sync-slugs", "--db", str(db_path)])
            args.print_summary = False

            with patch("metacritic_scraper_py.cli._build_client", return_value=_FakeSlugClient(records)), self.assertLogs(
                level="INFO"
            ) as captured:
                exit_code = run_sync_slugs(args)

            self.assertEqual(exit_code, 0)
            joined = "\n".join(captured.output)
            self.assertIn(
                "sync-slugs sitemap=https://www.metacritic.com/sitemap-1.xml total_games=2 saved_games=2 inserted=2 updated=0",
                joined,
            )
            self.assertIn(
                "sync-slugs sitemap=https://www.metacritic.com/sitemap-2.xml total_games=1 saved_games=1 inserted=1 updated=0",
                joined,
            )

    def test_run_sync_slugs_deduplicates_repeated_slug_within_one_sitemap(self) -> None:
        records = [
            GameSlugRecord(
                slug="alpha",
                game_url="https://www.metacritic.com/game/alpha/",
                sitemap_url="https://www.metacritic.com/sitemap-1.xml",
            ),
            GameSlugRecord(
                slug="beta",
                game_url="https://www.metacritic.com/game/beta/",
                sitemap_url="https://www.metacritic.com/sitemap-1.xml",
            ),
            GameSlugRecord(
                slug="alpha",
                game_url="https://www.metacritic.com/game/alpha/",
                sitemap_url="https://www.metacritic.com/sitemap-1.xml",
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "slugs.db"
            parser = build_parser()
            args = parser.parse_args(["sync-slugs", "--db", str(db_path)])
            args.print_summary = False

            with patch("metacritic_scraper_py.cli._build_client", return_value=_FakeSlugClient(records)), patch(
                "metacritic_scraper_py.cli.DEFAULT_SLUG_SYNC_BATCH_SIZE",
                1,
            ), self.assertLogs(level="INFO") as captured:
                exit_code = run_sync_slugs(args)

            self.assertEqual(exit_code, 0)
            joined = "\n".join(captured.output)
            self.assertIn(
                "sync-slugs sitemap=https://www.metacritic.com/sitemap-1.xml total_games=3 saved_games=2 inserted=2 updated=0",
                joined,
            )

            storage = SQLiteStorage(db_path)
            try:
                self.assertEqual(storage.count_rows("game_slugs"), 2)
            finally:
                storage.close()

    def test_run_sync_slugs_returns_empty_result_when_limit_is_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "slugs.db"
            parser = build_parser()
            args = parser.parse_args(["sync-slugs", "--db", str(db_path), "--limit-slugs", "0"])
            args.print_summary = False

            with patch("metacritic_scraper_py.cli._build_client") as build_client:
                exit_code = run_sync_slugs(args)

            self.assertEqual(exit_code, 0)
            build_client.assert_not_called()

            storage = SQLiteStorage(db_path)
            try:
                self.assertEqual(storage.count_rows("game_slugs"), 0)
            finally:
                storage.close()


if __name__ == "__main__":
    unittest.main()
