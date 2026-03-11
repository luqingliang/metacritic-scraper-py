import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from metacritic_scraper_py.client import MetacriticClientError
from metacritic_scraper_py.scraper import CrawlResult, MetacriticScraper
from metacritic_scraper_py.storage import SQLiteStorage


class _ClientThatShouldNotListSlugs:
    pass


class FullCrawlStorageSelectionTestCase(unittest.TestCase):
    def test_list_game_slugs_orders_stored_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game_slugs(
                    [
                        ("gamma", "https://www.metacritic.com/game/gamma/", "https://www.metacritic.com/sitemap-2.xml"),
                        ("beta", "https://www.metacritic.com/game/beta/", "https://www.metacritic.com/sitemap-1.xml"),
                        ("delta", "https://www.metacritic.com/game/delta/", "https://www.metacritic.com/sitemap-3.xml"),
                        ("alpha", "https://www.metacritic.com/game/alpha/", "https://www.metacritic.com/sitemap-1.xml"),
                    ]
                )

                self.assertEqual(storage.list_game_slugs(), ["alpha", "beta", "gamma", "delta"])
            finally:
                storage.close()


class FullCrawlSourceTestCase(unittest.TestCase):
    def test_crawl_from_sitemaps_reads_slugs_from_game_slugs_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game_slugs(
                    [
                        ("alpha", "https://www.metacritic.com/game/alpha/", "https://www.metacritic.com/sitemap-1.xml"),
                        ("beta", "https://www.metacritic.com/game/beta/", "https://www.metacritic.com/sitemap-1.xml"),
                        ("gamma", "https://www.metacritic.com/game/gamma/", "https://www.metacritic.com/sitemap-2.xml"),
                        ("delta", "https://www.metacritic.com/game/delta/", "https://www.metacritic.com/sitemap-3.xml"),
                    ]
                )
                scraper = MetacriticScraper(_ClientThatShouldNotListSlugs(), storage)
                captured: dict[str, object] = {}

                def _fake_crawl_slugs(slugs, **kwargs):
                    captured["slugs"] = list(slugs)
                    captured["kwargs"] = kwargs
                    return CrawlResult()

                with patch.object(scraper, "_crawl_slugs", side_effect=_fake_crawl_slugs):
                    scraper.crawl_from_sitemaps(
                        include_critic_reviews=True,
                        include_user_reviews=False,
                        review_page_size=50,
                        max_review_pages=1,
                        concurrency=1,
                    )

                self.assertEqual(captured["slugs"], ["alpha", "beta", "gamma", "delta"])
                self.assertEqual(captured["kwargs"]["include_critic_reviews"], True)
                self.assertEqual(captured["kwargs"]["include_user_reviews"], False)
                self.assertEqual(captured["kwargs"]["review_page_size"], 50)
                self.assertEqual(captured["kwargs"]["max_review_pages"], 1)
                self.assertEqual(captured["kwargs"]["concurrency"], 1)
            finally:
                storage.close()

    def test_crawl_slug_respects_separate_review_flags(self) -> None:
        class _ClientSelectiveReviews:
            def __init__(self) -> None:
                self.review_calls: list[str] = []

            def fetch_product(self, slug: str) -> dict:
                return {"data": {"item": {"id": hash(slug) & 0xFFFF, "title": slug, "platform": "PC"}}}

            def resolve_cover_url(self, *, product_payload: dict) -> str | None:
                del product_payload
                return None

            def fetch_score_summary(self, slug: str, review_type: str) -> dict | None:
                del slug, review_type
                return None

            def iter_reviews(
                self,
                *,
                slug: str,
                review_type: str,
                page_size: int = 50,
                max_pages: int | None = None,
            ):
                del slug, page_size, max_pages
                self.review_calls.append(review_type)
                if review_type == "critic":
                    yield {
                        "publicationSlug": "edge",
                        "publicationName": "Edge",
                        "date": "2026-03-10",
                        "score": 80,
                        "url": "https://example.com/falcon-40-review",
                        "quote": "solid",
                        "author": "Critic A",
                    }
                    return
                yield {
                    "id": "user-review-1",
                    "author": "UserA",
                    "score": 9,
                    "date": "2026-03-10",
                    "spoiler": False,
                    "quote": "great",
                }

        with tempfile.TemporaryDirectory() as tmpdir:
            critic_db_path = Path(tmpdir) / "critic-only.db"
            critic_storage = SQLiteStorage(critic_db_path)
            try:
                critic_client = _ClientSelectiveReviews()
                critic_scraper = MetacriticScraper(critic_client, critic_storage)
                critic_result = critic_scraper.crawl_slug(
                    "critic-only",
                    include_critic_reviews=True,
                    include_user_reviews=False,
                    review_page_size=50,
                    max_review_pages=1,
                )

                self.assertEqual(critic_client.review_calls, ["critic"])
                self.assertEqual(critic_result.critic_reviews_saved, 1)
                self.assertEqual(critic_result.user_reviews_saved, 0)
                self.assertEqual(critic_storage.count_rows("critic_reviews"), 1)
                self.assertEqual(critic_storage.count_rows("user_reviews"), 0)
            finally:
                critic_storage.close()

            user_db_path = Path(tmpdir) / "user-only.db"
            user_storage = SQLiteStorage(user_db_path)
            try:
                user_client = _ClientSelectiveReviews()
                user_scraper = MetacriticScraper(user_client, user_storage)
                user_result = user_scraper.crawl_slug(
                    "user-only",
                    include_critic_reviews=False,
                    include_user_reviews=True,
                    review_page_size=50,
                    max_review_pages=1,
                )

                self.assertEqual(user_client.review_calls, ["user"])
                self.assertEqual(user_result.critic_reviews_saved, 0)
                self.assertEqual(user_result.user_reviews_saved, 1)
                self.assertEqual(user_storage.count_rows("critic_reviews"), 0)
                self.assertEqual(user_storage.count_rows("user_reviews"), 1)
            finally:
                user_storage.close()

    def test_crawl_from_sitemaps_continues_when_review_fetch_fails_for_one_slug(self) -> None:
        class _ClientWithReview404:
            def fetch_product(self, slug: str) -> dict:
                return {"data": {"item": {"id": hash(slug) & 0xFFFF, "title": slug, "platform": "PC"}}}

            def resolve_cover_url(self, *, product_payload: dict) -> str | None:
                del product_payload
                return None

            def fetch_score_summary(self, slug: str, review_type: str) -> dict | None:
                return None

            def iter_reviews(
                self,
                *,
                slug: str,
                review_type: str,
                page_size: int = 50,
                max_pages: int | None = None,
            ):
                del page_size, max_pages
                if slug == "falcon-40" and review_type == "critic":
                    yield {
                        "publicationSlug": "edge",
                        "publicationName": "Edge",
                        "date": "2026-03-10",
                        "score": 80,
                        "url": "https://example.com/falcon-40-review",
                        "quote": "solid",
                        "author": "Critic A",
                    }
                    raise MetacriticClientError("status code 404 for latest critic reviews")
                if slug == "second-game" and review_type == "user":
                    yield {
                        "id": "user-review-1",
                        "author": "UserA",
                        "score": 9,
                        "date": "2026-03-10",
                        "spoiler": False,
                        "quote": "great",
                    }

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game_slugs(
                    [
                        ("falcon-40", "https://www.metacritic.com/game/falcon-40/", "https://www.metacritic.com/sitemap-1.xml"),
                        (
                            "second-game",
                            "https://www.metacritic.com/game/second-game/",
                            "https://www.metacritic.com/sitemap-1.xml",
                        ),
                    ]
                )

                scraper = MetacriticScraper(_ClientWithReview404(), storage)
                result = scraper.crawl_from_sitemaps(
                    include_critic_reviews=True,
                    include_user_reviews=True,
                    review_page_size=50,
                    max_review_pages=1,
                    concurrency=1,
                )

                self.assertEqual(result.games_crawled, 2)
                self.assertEqual(result.critic_reviews_saved, 1)
                self.assertEqual(result.user_reviews_saved, 1)
                self.assertEqual(result.failed_slugs, [])
                self.assertFalse(result.stopped)
                self.assertEqual(storage.count_rows("games"), 2)
                self.assertEqual(storage.count_rows("critic_reviews"), 1)
                self.assertEqual(storage.count_rows("user_reviews"), 1)
            finally:
                storage.close()

    def test_crawl_from_sitemaps_logs_progress_labels(self) -> None:
        class _ClientForProgressLogs:
            def fetch_product(self, slug: str) -> dict:
                return {"data": {"item": {"id": hash(slug) & 0xFFFF, "title": slug, "platform": "PC"}}}

            def resolve_cover_url(self, *, product_payload: dict) -> str | None:
                del product_payload
                return None

            def fetch_score_summary(self, slug: str, review_type: str) -> dict | None:
                return None

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            storage = SQLiteStorage(db_path)
            try:
                storage.upsert_game_slugs(
                    [
                        ("alpha", "https://www.metacritic.com/game/alpha/", "https://www.metacritic.com/sitemap-1.xml"),
                        ("beta", "https://www.metacritic.com/game/beta/", "https://www.metacritic.com/sitemap-1.xml"),
                    ]
                )

                scraper = MetacriticScraper(_ClientForProgressLogs(), storage)
                with self.assertLogs("metacritic_scraper_py.scraper", level="INFO") as captured:
                    result = scraper.crawl_from_sitemaps(
                        include_critic_reviews=False,
                        include_user_reviews=False,
                        review_page_size=50,
                        max_review_pages=1,
                        concurrency=1,
                    )

                messages = [record.getMessage() for record in captured.records]
                progress_records = [record for record in captured.records if hasattr(record, "progress")]
                self.assertEqual(result.games_crawled, 2)
                self.assertEqual(messages, ["completed slug=alpha status=ok", "completed slug=beta status=ok"])
                self.assertEqual([record.progress for record in progress_records], ["1/2", "2/2"])
            finally:
                storage.close()


if __name__ == "__main__":
    unittest.main()
