from __future__ import annotations

import logging
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterator, Literal
from urllib.parse import urlparse

import httpx

from .config import (
    BASE_API_URL,
    BASE_SITE_URL,
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_DELAY_SECONDS,
    DEFAULT_HEADERS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)

RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
ReviewType = Literal["critic", "user"]


class MetacriticClientError(RuntimeError):
    """Raised when API calls fail."""


@dataclass(frozen=True)
class ReviewPage:
    items: list[dict]
    next_href: str | None
    total_results: int


@dataclass(frozen=True)
class GameSlugRecord:
    slug: str
    game_url: str
    sitemap_url: str


def slug_from_game_url(url: str) -> str | None:
    """
    Extract slug from URLs like:
    https://www.metacritic.com/game/the-legend-of-zelda-breath-of-the-wild/
    """
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        return None
    if parts[0] != "game":
        return None
    return parts[1]


def normalize_bucket_path(bucket_path: str | None) -> str | None:
    if not bucket_path:
        return None
    normalized = bucket_path.strip()
    if not normalized:
        return None
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    return normalized


def cover_bucket_path_from_product(product_payload: dict) -> str | None:
    item = product_payload.get("data", {}).get("item", {})
    images = list(item.get("images") or [])
    if not images:
        return None

    def _pick_path(candidates: list[dict]) -> str | None:
        for image in candidates:
            raw = image.get("bucketPath") or image.get("path")
            if not isinstance(raw, str):
                continue
            normalized = normalize_bucket_path(raw)
            if normalized:
                return normalized
        return None

    for image_type in ("cardImage", "mainImage"):
        matched = [image for image in images if image.get("typeName") == image_type]
        chosen = _pick_path(matched)
        if chosen:
            return chosen
    return _pick_path(images)


def catalog_image_url_from_bucket_path(bucket_path: str | None) -> str | None:
    normalized = normalize_bucket_path(bucket_path)
    if not normalized:
        return None
    return f"{BASE_SITE_URL}/a/img/catalog{normalized}"


class MetacriticClient:
    def __init__(
        self,
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
        delay_seconds: float = DEFAULT_DELAY_SECONDS,
        user_agent: str | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        headers = dict(DEFAULT_HEADERS)
        if user_agent:
            headers["User-Agent"] = user_agent
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.delay_seconds = delay_seconds
        self._stop_event = stop_event
        self._closed_event = threading.Event()
        self._http = httpx.Client(
            headers=headers,
            timeout=timeout_seconds,
            follow_redirects=True,
            http2=False,
        )
        self._stop_watcher: threading.Thread | None = None
        if self._stop_event is not None:
            self._stop_watcher = threading.Thread(
                target=self._watch_for_stop,
                name="metacritic-client-stop",
                daemon=True,
            )
            self._stop_watcher.start()

    def __enter__(self) -> "MetacriticClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self._closed_event.set()
        self._http.close()
        if self._stop_watcher is not None and self._stop_watcher is not threading.current_thread():
            self._stop_watcher.join(timeout=0.2)

    def _watch_for_stop(self) -> None:
        assert self._stop_event is not None
        while not self._closed_event.is_set():
            if self._stop_event.wait(0.1):
                self._http.close()
                return

    def _check_stopped(self) -> None:
        if self._stop_event is not None and self._stop_event.is_set():
            raise InterruptedError("stopped by user")

    def _sleep_for(self, seconds: float) -> None:
        if seconds <= 0:
            return
        if self._stop_event is None:
            time.sleep(seconds)
            return
        if self._stop_event.wait(seconds):
            self._check_stopped()

    def _sleep(self) -> None:
        self._sleep_for(self.delay_seconds)

    def _request(self, url: str, *, params: dict | None = None) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            self._check_stopped()
            try:
                self._sleep()
                response = self._http.get(url, params=params)
                self._check_stopped()
                if response.status_code in RETRY_STATUS_CODES:
                    raise MetacriticClientError(
                        f"retryable status code {response.status_code} for {response.url}"
                    )
                if response.status_code >= 400:
                    raise MetacriticClientError(
                        f"status code {response.status_code} for {response.url}"
                    )
                return response
            except RuntimeError:
                self._check_stopped()
                raise
            except (httpx.TransportError, httpx.TimeoutException, MetacriticClientError) as exc:
                self._check_stopped()
                last_error = exc
                if attempt >= self.max_retries:
                    break
                wait_seconds = self.backoff_seconds * attempt
                logger.warning(
                    "request failed (%s), retrying in %.1fs (%d/%d)",
                    exc,
                    wait_seconds,
                    attempt,
                    self.max_retries,
                )
                self._sleep_for(wait_seconds)
        raise MetacriticClientError(f"request failed after retries: {last_error}")

    def _get_text(self, url: str) -> str:
        response = self._request(url)
        return response.text

    def _get_binary(self, url: str) -> bytes:
        response = self._request(url)
        return response.content

    def _get_json(self, url: str, *, params: dict | None = None) -> dict:
        response = self._request(url, params=params)
        try:
            return response.json()
        except ValueError as exc:
            raise MetacriticClientError(f"invalid json from {response.url}") from exc

    def get_robots_txt(self) -> str:
        return self._get_text(f"{BASE_SITE_URL}/robots.txt")

    def iter_game_sitemap_urls(self, *, limit_sitemaps: int | None = None) -> Iterator[str]:
        xml_text = self._get_text(f"{BASE_SITE_URL}/games.xml")
        root = ET.fromstring(xml_text)
        count = 0
        for node in root.findall(".//sm:sitemap/sm:loc", SITEMAP_NS):
            self._check_stopped()
            if not node.text:
                continue
            yield node.text.strip()
            count += 1
            if limit_sitemaps and count >= limit_sitemaps:
                break

    def iter_game_slugs(
        self,
        *,
        limit_sitemaps: int | None = None,
        limit_slugs: int | None = None,
    ) -> Iterator[str]:
        for record in self.iter_game_slug_records(
            limit_sitemaps=limit_sitemaps,
            limit_slugs=limit_slugs,
        ):
            yield record.slug

    def iter_game_slug_records(
        self,
        *,
        limit_sitemaps: int | None = None,
        limit_slugs: int | None = None,
    ) -> Iterator[GameSlugRecord]:
        yielded = 0
        for sitemap_url in self.iter_game_sitemap_urls(limit_sitemaps=limit_sitemaps):
            self._check_stopped()
            for record in self.iter_game_slug_records_for_sitemap(sitemap_url):
                yield record
                yielded += 1
                if limit_slugs and yielded >= limit_slugs:
                    return

    def iter_game_slug_records_for_sitemap(self, sitemap_url: str) -> Iterator[GameSlugRecord]:
        self._check_stopped()
        xml_text = self._get_text(sitemap_url)
        root = ET.fromstring(xml_text)
        for node in root.findall(".//sm:url/sm:loc", SITEMAP_NS):
            self._check_stopped()
            if not node.text:
                continue
            game_url = node.text.strip()
            slug = slug_from_game_url(game_url)
            if not slug:
                continue
            yield GameSlugRecord(
                slug=slug,
                game_url=game_url,
                sitemap_url=sitemap_url,
            )

    def fetch_product(self, slug: str) -> dict:
        url = f"{BASE_API_URL}/games/metacritic/{slug}/web"
        params = {
            "componentName": "product",
            "componentDisplayName": "Product",
            "componentType": "Product",
        }
        return self._get_json(url, params=params)

    def fetch_game_page_html(self, slug: str) -> str:
        return self._get_text(f"{BASE_SITE_URL}/game/{slug}/")

    def resolve_cover_url(
        self,
        *,
        slug: str,
        product_payload: dict,
    ) -> str | None:
        bucket_path = cover_bucket_path_from_product(product_payload)
        return catalog_image_url_from_bucket_path(bucket_path)

    def fetch_binary(self, url: str) -> bytes:
        return self._get_binary(url)

    def fetch_score_summary(self, slug: str, review_type: ReviewType) -> dict:
        if review_type == "critic":
            component_name = "critic-score-summary"
            display_name = "Critic Score Summary"
        else:
            component_name = "user-score-summary"
            display_name = "User Score Summary"
        url = f"{BASE_API_URL}/reviews/metacritic/{review_type}/games/{slug}/stats/web"
        params = {
            "componentName": component_name,
            "componentDisplayName": display_name,
            "componentType": "MetaScoreSummary",
        }
        return self._get_json(url, params=params)

    def fetch_reviews_page(
        self,
        *,
        slug: str,
        review_type: ReviewType,
        offset: int = 0,
        limit: int = 50,
    ) -> ReviewPage:
        url = f"{BASE_API_URL}/reviews/metacritic/{review_type}/games/{slug}/web"
        if review_type == "critic":
            params = {
                "offset": offset,
                "limit": limit,
                "sort": "date",
                "componentName": "latest-critic-reviews",
                "componentDisplayName": "Latest Critic Reviews",
                "componentType": "ReviewList",
            }
        else:
            params = {
                "offset": offset,
                "limit": limit,
                "orderBy": "score",
                "orderType": "desc",
                "sort": "date",
                "componentName": "top-user-reviews",
                "componentDisplayName": "Top User Reviews",
                "componentType": "ReviewList",
            }
        payload = self._get_json(url, params=params)
        items = payload.get("data", {}).get("items", [])
        total_results = int(payload.get("data", {}).get("totalResults", 0))
        next_href = payload.get("links", {}).get("next", {}).get("href")
        return ReviewPage(items=list(items), next_href=next_href, total_results=total_results)

    def iter_reviews(
        self,
        *,
        slug: str,
        review_type: ReviewType,
        page_size: int = 50,
        max_pages: int | None = None,
    ) -> Iterator[dict]:
        offset = 0
        page_num = 0
        while True:
            self._check_stopped()
            if max_pages is not None and page_num >= max_pages:
                return
            page = self.fetch_reviews_page(
                slug=slug,
                review_type=review_type,
                offset=offset,
                limit=page_size,
            )
            if not page.items:
                return
            for item in page.items:
                self._check_stopped()
                yield item
            page_num += 1
            if not page.next_href:
                return
            offset += len(page.items)

    def fetch_finder_page(
        self,
        *,
        sort_by: str = "-releaseDate",
        offset: int = 0,
        limit: int = 24,
        mco_type_id: int = 13,
        platform_ids: list[int] | None = None,
        genres: list[str] | None = None,
    ) -> dict:
        params: dict[str, str | int] = {
            "mcoTypeId": mco_type_id,
            "sortBy": sort_by,
            "offset": offset,
            "limit": limit,
            "componentName": "finder-list",
            "componentDisplayName": "Finder List",
            "componentType": "ProductList",
        }
        if platform_ids:
            params["platform"] = ",".join(str(v) for v in platform_ids)
        if genres:
            params["genres"] = ",".join(genres)
        return self._get_json(f"{BASE_API_URL}/finder/metacritic/web", params=params)
