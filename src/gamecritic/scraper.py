from __future__ import annotations

import concurrent.futures
import logging
import threading
from dataclasses import dataclass, field
from contextvars import copy_context
from typing import Callable, Iterable

from .client import MetacriticClient, MetacriticClientError
from .cover_downloader import CoverImageDownloader
from .storage import SQLiteStorage

logger = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    slugs_processed: int = 0
    games_crawled: int = 0
    critic_reviews_saved: int = 0
    user_reviews_saved: int = 0
    covers_downloaded: int = 0
    covers_skipped: int = 0
    covers_failed: int = 0
    failed_slugs: list[str] = field(default_factory=list)
    stopped: bool = False


class MetacriticScraper:
    def __init__(
        self,
        client: MetacriticClient,
        storage: SQLiteStorage,
        *,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.client = client
        self.storage = storage
        self._stop_event = stop_event

    def _stop_requested(self) -> bool:
        return self._stop_event is not None and self._stop_event.is_set()

    def _check_stopped(self) -> None:
        if self._stop_requested():
            raise InterruptedError("stopped by user")

    @staticmethod
    def _log_with_progress(
        level: int,
        message: str,
        *args: object,
        progress_label: str | None = None,
    ) -> None:
        extra = {"progress": progress_label} if progress_label else None
        logger.log(level, message, *args, extra=extra)

    @staticmethod
    def _result_status(slug: str, result: CrawlResult) -> str:
        if result.stopped:
            return "stopped"
        if slug in result.failed_slugs:
            return "failed"
        return "ok"

    def _log_slug_progress(
        self,
        *,
        slug: str,
        result: CrawlResult,
        completed: int,
        total: int,
    ) -> None:
        self._log_with_progress(
            logging.INFO,
            "completed slug=%s status=%s",
            slug,
            self._result_status(slug, result),
            progress_label=f"{completed}/{total}" if total else None,
        )

    @staticmethod
    def _merge_result(into: CrawlResult, one: CrawlResult) -> None:
        into.slugs_processed += one.slugs_processed
        into.games_crawled += one.games_crawled
        into.critic_reviews_saved += one.critic_reviews_saved
        into.user_reviews_saved += one.user_reviews_saved
        into.covers_downloaded += one.covers_downloaded
        into.covers_skipped += one.covers_skipped
        into.covers_failed += one.covers_failed
        into.failed_slugs.extend(one.failed_slugs)
        into.stopped = into.stopped or one.stopped

    def _crawl_slugs(
        self,
        slugs: Iterable[str],
        *,
        include_critic_reviews: bool,
        include_user_reviews: bool,
        review_page_size: int,
        max_review_pages: int | None,
        concurrency: int,
        cover_downloader: CoverImageDownloader | None = None,
        slug_handler: Callable[[str], CrawlResult] | None = None,
    ) -> CrawlResult:
        result = CrawlResult()
        worker_count = max(1, concurrency)
        slug_list = slugs if isinstance(slugs, list) else list(slugs)
        total = len(slug_list)
        completed = 0
        handle_slug = slug_handler or (
            lambda slug: self.crawl_slug(
                slug,
                include_critic_reviews=include_critic_reviews,
                include_user_reviews=include_user_reviews,
                review_page_size=review_page_size,
                max_review_pages=max_review_pages,
                cover_downloader=cover_downloader,
            )
        )

        if worker_count == 1:
            try:
                for slug in slug_list:
                    self._check_stopped()
                    one = handle_slug(slug)
                    self._merge_result(result, one)
                    if not one.stopped:
                        completed += 1
                        result.slugs_processed += 1
                        self._log_slug_progress(slug=slug, result=one, completed=completed, total=total)
                    if one.stopped:
                        break
            except InterruptedError:
                result.stopped = True
            return result

        logger.info("concurrent crawl enabled, workers=%d", worker_count)
        pending_limit = worker_count * 2
        future_to_slug: dict[concurrent.futures.Future[CrawlResult], str] = {}

        def _drain_one_completed(*, ignore_stop: bool = False) -> None:
            nonlocal completed
            while future_to_slug:
                done, _ = concurrent.futures.wait(
                    future_to_slug.keys(),
                    timeout=0.1,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                if not done:
                    if not ignore_stop:
                        self._check_stopped()
                    continue
                for future in done:
                    slug = future_to_slug.pop(future)
                    try:
                        one = future.result()
                    except InterruptedError:
                        result.stopped = True
                        continue
                    except Exception as exc:  # pragma: no cover
                        logger.error("unhandled error while crawling slug=%s: %s", slug, exc)
                        result.failed_slugs.append(slug)
                        completed += 1
                        result.slugs_processed += 1
                        self._log_with_progress(
                            logging.INFO,
                            "completed slug=%s status=failed",
                            slug,
                            progress_label=f"{completed}/{total}" if total else None,
                        )
                        continue
                    self._merge_result(result, one)
                    if not one.stopped:
                        completed += 1
                        result.slugs_processed += 1
                        self._log_slug_progress(slug=slug, result=one, completed=completed, total=total)
                return

        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            try:
                for slug in slug_list:
                    self._check_stopped()
                    if result.stopped:
                        break

                    future_context = copy_context()
                    future = pool.submit(
                        future_context.run,
                        handle_slug,
                        slug,
                    )
                    future_to_slug[future] = slug

                    if len(future_to_slug) >= pending_limit:
                        _drain_one_completed()

                while future_to_slug and not result.stopped:
                    _drain_one_completed()
            except InterruptedError:
                result.stopped = True
            finally:
                if result.stopped:
                    for future in list(future_to_slug):
                        future.cancel()
                    while future_to_slug:
                        _drain_one_completed(ignore_stop=True)

        return result

    def crawl_slug(
        self,
        slug: str,
        *,
        include_critic_reviews: bool,
        include_user_reviews: bool,
        review_page_size: int,
        max_review_pages: int | None,
        cover_downloader: CoverImageDownloader | None = None,
    ) -> CrawlResult:
        result = CrawlResult()

        try:
            self._check_stopped()
            product = self.client.fetch_product(slug)
        except MetacriticClientError as exc:
            logger.error("failed fetching product for %s: %s", slug, exc)
            result.failed_slugs.append(slug)
            return result
        except InterruptedError:
            result.stopped = True
            return result

        cover_url = self.client.resolve_cover_url(product_payload=product)

        try:
            self._check_stopped()
            critic_summary = self.client.fetch_score_summary(slug, "critic")
        except MetacriticClientError as exc:
            logger.warning("critic summary unavailable for %s: %s", slug, exc)
            critic_summary = None
        except InterruptedError:
            result.stopped = True
            return result

        try:
            self._check_stopped()
            user_summary = self.client.fetch_score_summary(slug, "user")
        except MetacriticClientError as exc:
            logger.warning("user summary unavailable for %s: %s", slug, exc)
            user_summary = None
        except InterruptedError:
            result.stopped = True
            return result

        self.storage.upsert_game(
            slug=slug,
            product_payload=product,
            critic_summary_payload=critic_summary,
            user_summary_payload=user_summary,
            cover_url=cover_url,
        )
        result.games_crawled += 1

        if cover_downloader is not None:
            try:
                self._check_stopped()
                status = cover_downloader.download(slug=slug, cover_url=cover_url)
            except InterruptedError:
                result.stopped = True
                return result
            if status == "downloaded":
                result.covers_downloaded += 1
            elif status == "skipped":
                result.covers_skipped += 1
            else:
                result.covers_failed += 1
                logger.warning("cover download failed for slug=%s url=%s", slug, cover_url)

        reviews_result = self.crawl_reviews_for_slug(
            slug,
            include_critic_reviews=include_critic_reviews,
            include_user_reviews=include_user_reviews,
            review_page_size=review_page_size,
            max_review_pages=max_review_pages,
        )
        self._merge_result(result, reviews_result)
        return result

    def crawl_reviews_for_slug(
        self,
        slug: str,
        *,
        include_critic_reviews: bool,
        include_user_reviews: bool,
        review_page_size: int,
        max_review_pages: int | None,
    ) -> CrawlResult:
        result = CrawlResult()

        if not include_critic_reviews and not include_user_reviews:
            return result

        if include_critic_reviews:
            critic_buffer: list[dict] = []
            try:
                for review in self.client.iter_reviews(
                    slug=slug,
                    review_type="critic",
                    page_size=review_page_size,
                    max_pages=max_review_pages,
                ):
                    self._check_stopped()
                    critic_buffer.append(review)
                    if len(critic_buffer) >= 200:
                        result.critic_reviews_saved += self.storage.upsert_critic_reviews(slug, critic_buffer)
                        critic_buffer.clear()
            except MetacriticClientError as exc:
                logger.warning("critic reviews unavailable for %s: %s", slug, exc)
            except InterruptedError:
                result.stopped = True
                return result
            if critic_buffer:
                result.critic_reviews_saved += self.storage.upsert_critic_reviews(slug, critic_buffer)

        if include_user_reviews:
            user_buffer: list[dict] = []
            try:
                for review in self.client.iter_reviews(
                    slug=slug,
                    review_type="user",
                    page_size=review_page_size,
                    max_pages=max_review_pages,
                ):
                    self._check_stopped()
                    user_buffer.append(review)
                    if len(user_buffer) >= 200:
                        result.user_reviews_saved += self.storage.upsert_user_reviews(slug, user_buffer)
                        user_buffer.clear()
            except MetacriticClientError as exc:
                logger.warning("user reviews unavailable for %s: %s", slug, exc)
            except InterruptedError:
                result.stopped = True
                return result
            if user_buffer:
                result.user_reviews_saved += self.storage.upsert_user_reviews(slug, user_buffer)

        return result

    def crawl_from_sitemaps(
        self,
        *,
        include_critic_reviews: bool,
        include_user_reviews: bool,
        review_page_size: int,
        max_review_pages: int | None,
        concurrency: int = 4,
        download_covers: bool = False,
        covers_dir: str = "data/covers",
        overwrite_covers: bool = False,
    ) -> CrawlResult:
        full_crawl_slugs = self.storage.list_game_slugs()

        if not full_crawl_slugs:
            logger.warning("game_slugs table is empty or no rows matched; run sync-slugs first")

        cover_downloader = None
        if download_covers:
            cover_downloader = CoverImageDownloader(
                fetch_binary=self.client.fetch_binary,
                output_dir=covers_dir,
                overwrite=overwrite_covers,
            )

        return self._crawl_slugs(
            full_crawl_slugs,
            include_critic_reviews=include_critic_reviews,
            include_user_reviews=include_user_reviews,
            review_page_size=review_page_size,
            max_review_pages=max_review_pages,
            concurrency=concurrency,
            cover_downloader=cover_downloader,
        )

    def crawl_reviews_from_games(
        self,
        *,
        slug: str | None = None,
        include_critic_reviews: bool,
        include_user_reviews: bool,
        review_page_size: int,
        max_review_pages: int | None,
        concurrency: int = 4,
    ) -> CrawlResult:
        review_slugs = self.storage.list_crawled_game_slugs(slug=slug)

        if not review_slugs:
            if slug is not None and slug.strip():
                logger.warning("games table has no matching row for slug=%s; crawl that game first", slug)
            else:
                logger.warning("games table is empty or no rows matched; crawl games first")

        return self._crawl_slugs(
            review_slugs,
            include_critic_reviews=include_critic_reviews,
            include_user_reviews=include_user_reviews,
            review_page_size=review_page_size,
            max_review_pages=max_review_pages,
            concurrency=concurrency,
            slug_handler=lambda slug: self.crawl_reviews_for_slug(
                slug,
                include_critic_reviews=include_critic_reviews,
                include_user_reviews=include_user_reviews,
                review_page_size=review_page_size,
                max_review_pages=max_review_pages,
            ),
        )
