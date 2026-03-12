import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from gamecritic.cover_downloader import CoverImageDownloader
from gamecritic.storage import SQLiteStorage


class CoverImageDownloaderTestCase(unittest.TestCase):
    def test_download_writes_file_with_extension_from_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            calls: list[str] = []

            def _fetch(url: str) -> bytes:
                calls.append(url)
                return b"binary-content"

            downloader = CoverImageDownloader(
                fetch_binary=_fetch,
                output_dir=tmpdir,
                overwrite=False,
            )
            status = downloader.download(slug="demo-game", cover_url="https://cdn.example.com/path/cover.webp")
            self.assertEqual(status, "downloaded")
            self.assertEqual(calls, ["https://cdn.example.com/path/cover.webp"])
            self.assertEqual((Path(tmpdir) / "demo-game.webp").read_bytes(), b"binary-content")

    def test_download_skips_when_cover_url_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = CoverImageDownloader(
                fetch_binary=lambda _: b"unused",
                output_dir=tmpdir,
                overwrite=False,
            )
            self.assertEqual(downloader.download(slug="demo-game", cover_url=None), "skipped")

    def test_download_skips_existing_file_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "demo-game.jpg"
            target.write_bytes(b"old")
            called = {"value": False}

            def _fetch(_: str) -> bytes:
                called["value"] = True
                return b"new"

            downloader = CoverImageDownloader(
                fetch_binary=_fetch,
                output_dir=tmpdir,
                overwrite=False,
            )
            status = downloader.download(slug="demo-game", cover_url="https://cdn.example.com/path/cover.jpg")
            self.assertEqual(status, "skipped")
            self.assertFalse(called["value"])
            self.assertEqual(target.read_bytes(), b"old")

    def test_download_overwrites_existing_file_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "demo-game.jpg"
            target.write_bytes(b"old")

            downloader = CoverImageDownloader(
                fetch_binary=lambda _: b"new",
                output_dir=tmpdir,
                overwrite=True,
            )
            status = downloader.download(slug="demo-game", cover_url="https://cdn.example.com/path/cover.jpg")
            self.assertEqual(status, "downloaded")
            self.assertEqual(target.read_bytes(), b"new")

    def test_download_returns_failed_when_fetch_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            def _raise(_: str) -> bytes:
                raise RuntimeError("network error")

            downloader = CoverImageDownloader(
                fetch_binary=_raise,
                output_dir=tmpdir,
                overwrite=False,
            )
            status = downloader.download(slug="demo-game", cover_url="https://cdn.example.com/path/cover.jpg")
            self.assertEqual(status, "failed")

    def test_download_reraises_interrupted_error_and_cleans_tmp_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            downloader = CoverImageDownloader(
                fetch_binary=lambda _: b"binary-content",
                output_dir=tmpdir,
                overwrite=False,
            )
            original_write_bytes = Path.write_bytes

            def _interrupt_after_partial_write(path: Path, data: bytes) -> int:
                written = original_write_bytes(path, data)
                if path.suffix == ".part":
                    raise InterruptedError("stopped by user")
                return written

            with patch("pathlib.Path.write_bytes", autospec=True, side_effect=_interrupt_after_partial_write):
                with self.assertRaises(InterruptedError):
                    downloader.download(slug="demo-game", cover_url="https://cdn.example.com/path/cover.jpg")

            self.assertEqual(list(Path(tmpdir).glob("*.part")), [])


class CoverUrlStorageQueryTestCase(unittest.TestCase):
    def test_list_game_cover_urls_lists_all_cover_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game(
                    slug="a-game",
                    product_payload={"data": {"item": {"id": 1, "title": "A"}}},
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url="https://www.metacritic.com/a/img/catalog/provider/1/1/a.jpg",
                )
                storage.upsert_game(
                    slug="b-game",
                    product_payload={"data": {"item": {"id": 2, "title": "B"}}},
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url="https://www.metacritic.com/a/img/catalog/provider/1/1/b.jpg",
                )
                storage.upsert_game(
                    slug="c-game",
                    product_payload={"data": {"item": {"id": 3, "title": "C"}}},
                    critic_summary_payload=None,
                    user_summary_payload=None,
                    cover_url=None,
                )

                all_rows = storage.list_game_cover_urls()
                self.assertEqual(
                    all_rows,
                    [
                        ("a-game", "https://www.metacritic.com/a/img/catalog/provider/1/1/a.jpg"),
                        ("b-game", "https://www.metacritic.com/a/img/catalog/provider/1/1/b.jpg"),
                    ],
                )
            finally:
                storage.close()


if __name__ == "__main__":
    unittest.main()
