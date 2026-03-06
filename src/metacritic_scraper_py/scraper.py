from __future__ import annotations

import concurrent.futures
import logging
from datetime import date, timedelta
from dataclasses import dataclass, field
from typing import Iterable

from .client import MetacriticClient, MetacriticClientError
from .cover_downloader import CoverImageDownloader
from .storage import SQLiteStorage

logger = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    games_crawled: int = 0
    critic_reviews_saved: int = 0
    user_reviews_saved: int = 0
    covers_downloaded: int = 0
    covers_skipped: int = 0
    covers_failed: int = 0
    failed_slugs: list[str] = field(default_factory=list)


class MetacriticScraper:
    def __init__(self, client: MetacriticClient, storage: SQLiteStorage) -> None:
        self.client = client
        self.storage = storage

    @staticmethod
    def _parse_iso_date(value: str | None) -> date | None:
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _merge_result(into: CrawlResult, one: CrawlResult) -> None:
        into.games_crawled += one.games_crawled
        into.critic_reviews_saved += one.critic_reviews_saved
        into.user_reviews_saved += one.user_reviews_saved
        into.covers_downloaded += one.covers_downloaded
        into.covers_skipped += one.covers_skipped
        into.covers_failed += one.covers_failed
        into.failed_slugs.extend(one.failed_slugs)

    def _crawl_slugs(
        self,
        slugs: Iterable[str],
        *,
        include_reviews: bool,
        review_page_size: int,
        max_review_pages: int | None,
        max_games: int | None,
        concurrency: int,
        cover_downloader: CoverImageDownloader | None = None,
    ) -> CrawlResult:
        result = CrawlResult()
        worker_count = max(1, concurrency)

        if worker_count == 1:
            for slug in slugs:
                one = self.crawl_slug(
                    slug,
                    include_reviews=include_reviews,
                    review_page_size=review_page_size,
                    max_review_pages=max_review_pages,
                    cover_downloader=cover_downloader,
                )
                self._merge_result(result, one)
                if max_games is not None and result.games_crawled >= max_games:
                    break
            return result

        logger.info("concurrent crawl enabled, workers=%d", worker_count)
        pending_limit = worker_count * 2
        future_to_slug: dict[concurrent.futures.Future[CrawlResult], str] = {}

        def _drain_one_completed() -> None:
            done, _ = concurrent.futures.wait(
                future_to_slug.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                slug = future_to_slug.pop(future)
                try:
                    one = future.result()
                except Exception as exc:  # pragma: no cover
                    logger.error("unhandled error while crawling slug=%s: %s", slug, exc)
                    result.failed_slugs.append(slug)
                    continue
                self._merge_result(result, one)

        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            for slug in slugs:
                if max_games is not None and result.games_crawled >= max_games:
                    break

                # Keep pending futures bounded by remaining success budget so
                # max_games remains based on successfully crawled games.
                while (
                    max_games is not None
                    and future_to_slug
                    and result.games_crawled + len(future_to_slug) >= max_games
                ):
                    _drain_one_completed()
                    if result.games_crawled >= max_games:
                        break
                if max_games is not None and result.games_crawled >= max_games:
                    break

                future = pool.submit(
                    self.crawl_slug,
                    slug,
                    include_reviews=include_reviews,
                    review_page_size=review_page_size,
                    max_review_pages=max_review_pages,
                    cover_downloader=cover_downloader,
                )
                future_to_slug[future] = slug

                if len(future_to_slug) >= pending_limit:
                    _drain_one_completed()

            while future_to_slug:
                _drain_one_completed()

        return result

    def crawl_slug(
        self,
        slug: str,
        *,
        include_reviews: bool,
        review_page_size: int,
        max_review_pages: int | None,
        cover_downloader: CoverImageDownloader | None = None,
    ) -> CrawlResult:
        result = CrawlResult()
        logger.info("crawling slug=%s", slug)

        try:
            product = self.client.fetch_product(slug)
        except MetacriticClientError as exc:
            logger.error("failed fetching product for %s: %s", slug, exc)
            result.failed_slugs.append(slug)
            return result

        cover_url = self.client.resolve_cover_url(slug=slug, product_payload=product)

        try:
            critic_summary = self.client.fetch_score_summary(slug, "critic")
        except MetacriticClientError as exc:
            logger.warning("critic summary unavailable for %s: %s", slug, exc)
            critic_summary = None

        try:
            user_summary = self.client.fetch_score_summary(slug, "user")
        except MetacriticClientError as exc:
            logger.warning("user summary unavailable for %s: %s", slug, exc)
            user_summary = None

        self.storage.upsert_game(
            slug=slug,
            product_payload=product,
            critic_summary_payload=critic_summary,
            user_summary_payload=user_summary,
            cover_url=cover_url,
        )
        result.games_crawled += 1

        if cover_downloader is not None:
            status = cover_downloader.download(slug=slug, cover_url=cover_url)
            if status == "downloaded":
                result.covers_downloaded += 1
            elif status == "skipped":
                result.covers_skipped += 1
            else:
                result.covers_failed += 1
                logger.warning("cover download failed for slug=%s url=%s", slug, cover_url)

        if not include_reviews:
            return result

        critic_buffer: list[dict] = []
        for review in self.client.iter_reviews(
            slug=slug,
            review_type="critic",
            page_size=review_page_size,
            max_pages=max_review_pages,
        ):
            critic_buffer.append(review)
            if len(critic_buffer) >= 200:
                result.critic_reviews_saved += self.storage.upsert_critic_reviews(slug, critic_buffer)
                critic_buffer.clear()
        if critic_buffer:
            result.critic_reviews_saved += self.storage.upsert_critic_reviews(slug, critic_buffer)

        user_buffer: list[dict] = []
        for review in self.client.iter_reviews(
            slug=slug,
            review_type="user",
            page_size=review_page_size,
            max_pages=max_review_pages,
        ):
            user_buffer.append(review)
            if len(user_buffer) >= 200:
                result.user_reviews_saved += self.storage.upsert_user_reviews(slug, user_buffer)
                user_buffer.clear()
        if user_buffer:
            result.user_reviews_saved += self.storage.upsert_user_reviews(slug, user_buffer)

        return result

    def crawl_from_sitemaps(
        self,
        *,
        include_reviews: bool,
        review_page_size: int,
        max_review_pages: int | None,
        max_games: int | None,
        start_slug: str | None,
        limit_sitemaps: int | None,
        limit_slugs: int | None,
        concurrency: int = 1,
        download_covers: bool = False,
        covers_dir: str = "data/covers",
        overwrite_covers: bool = False,
    ) -> CrawlResult:
        started = start_slug is None

        def selected_slugs() -> Iterable[str]:
            nonlocal started
            for slug in self.client.iter_game_slugs(
                limit_sitemaps=limit_sitemaps,
                limit_slugs=limit_slugs,
            ):
                if not started:
                    if slug == start_slug:
                        started = True
                    else:
                        continue
                yield slug

        cover_downloader = None
        if download_covers:
            cover_downloader = CoverImageDownloader(
                fetch_binary=self.client.fetch_binary,
                output_dir=covers_dir,
                overwrite=overwrite_covers,
            )

        return self._crawl_slugs(
            selected_slugs(),
            include_reviews=include_reviews,
            review_page_size=review_page_size,
            max_review_pages=max_review_pages,
            max_games=max_games,
            concurrency=concurrency,
            cover_downloader=cover_downloader,
        )

    def crawl_incremental_by_date(
        self,
        *,
        include_reviews: bool,
        review_page_size: int,
        max_review_pages: int | None,
        max_games: int | None,
        since_date: str | None,
        lookback_days: int,
        finder_page_size: int,
        state_key: str,
        concurrency: int = 1,
        download_covers: bool = False,
        covers_dir: str = "data/covers",
        overwrite_covers: bool = False,
    ) -> CrawlResult:
        explicit_since = self._parse_iso_date(since_date)
        stored_since = self._parse_iso_date(self.storage.get_state(state_key))
        base_since = explicit_since or stored_since
        cutoff_date = None
        if base_since:
            cutoff_date = base_since - timedelta(days=max(0, lookback_days))
            logger.info(
                "incremental mode enabled, since=%s, lookback_days=%d, effective_cutoff=%s",
                base_since.isoformat(),
                lookback_days,
                cutoff_date.isoformat(),
            )
        else:
            logger.info("incremental mode enabled, no previous checkpoint found; running from newest downward")

        visited_slugs: set[str] = set()
        selected_slugs: list[str] = []
        newest_release_date: date | None = stored_since
        offset = 0
        reached_cutoff = False

        while True:
            finder_payload = self.client.fetch_finder_page(
                sort_by="-releaseDate",
                offset=offset,
                limit=finder_page_size,
            )
            items = list(finder_payload.get("data", {}).get("items", []))
            if not items:
                break

            for item in items:
                slug = item.get("slug")
                if not slug:
                    continue
                if slug in visited_slugs:
                    continue

                release_date = self._parse_iso_date(item.get("releaseDate"))
                if release_date and (newest_release_date is None or release_date > newest_release_date):
                    newest_release_date = release_date

                # Finder is sorted by releaseDate desc. Once we pass the cutoff,
                # remaining pages should be older and can be skipped.
                if cutoff_date and release_date and release_date < cutoff_date:
                    reached_cutoff = True
                    break

                visited_slugs.add(slug)
                selected_slugs.append(slug)
                if max_games is not None and len(selected_slugs) >= max_games:
                    break

            if reached_cutoff:
                logger.info("stopped incremental crawl after reaching cutoff date")
                break

            if max_games is not None and len(selected_slugs) >= max_games:
                break

            next_href = finder_payload.get("links", {}).get("next", {}).get("href")
            if not next_href:
                break
            offset += len(items)

        cover_downloader = None
        if download_covers:
            cover_downloader = CoverImageDownloader(
                fetch_binary=self.client.fetch_binary,
                output_dir=covers_dir,
                overwrite=overwrite_covers,
            )

        result = self._crawl_slugs(
            selected_slugs,
            include_reviews=include_reviews,
            review_page_size=review_page_size,
            max_review_pages=max_review_pages,
            max_games=max_games,
            concurrency=concurrency,
            cover_downloader=cover_downloader,
        )

        if newest_release_date:
            self.storage.set_state(state_key, newest_release_date.isoformat())
            logger.info(
                "updated incremental checkpoint state_key=%s state_value=%s",
                state_key,
                newest_release_date.isoformat(),
            )

        return result
