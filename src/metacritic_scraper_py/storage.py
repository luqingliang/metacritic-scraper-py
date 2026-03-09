from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_dumps(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _critic_review_key(review: dict) -> str:
    parts = [
        str(review.get("publicationSlug") or ""),
        str(review.get("date") or ""),
        str(review.get("score") or ""),
        str(review.get("url") or ""),
        str(review.get("quote") or "")[:120],
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def _user_review_key(review: dict) -> str:
    review_id = review.get("id")
    if review_id:
        return str(review_id)
    parts = [
        str(review.get("author") or ""),
        str(review.get("date") or ""),
        str(review.get("score") or ""),
        str(review.get("quote") or "")[:120],
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


class SQLiteStorage:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Shared across worker threads when concurrent crawl is enabled.
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        with self._lock:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def _init_schema(self) -> None:
        with self._lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS games (
                    slug TEXT PRIMARY KEY,
                    game_id INTEGER,
                    title TEXT,
                    platform TEXT,
                    release_date TEXT,
                    premiere_year INTEGER,
                    rating TEXT,
                    critic_score REAL,
                    critic_review_count INTEGER,
                    user_score REAL,
                    user_review_count INTEGER,
                    cover_url TEXT,
                    product_json TEXT NOT NULL,
                    critic_summary_json TEXT,
                    user_summary_json TEXT,
                    scraped_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS game_slugs (
                    slug TEXT PRIMARY KEY,
                    game_url TEXT NOT NULL,
                    sitemap_url TEXT NOT NULL,
                    discovered_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS critic_reviews (
                    slug TEXT NOT NULL,
                    review_key TEXT NOT NULL,
                    score REAL,
                    review_date TEXT,
                    author TEXT,
                    publication_name TEXT,
                    source_url TEXT,
                    quote TEXT,
                    review_json TEXT NOT NULL,
                    scraped_at TEXT NOT NULL,
                    PRIMARY KEY (slug, review_key)
                );

                CREATE TABLE IF NOT EXISTS user_reviews (
                    review_id TEXT PRIMARY KEY,
                    slug TEXT NOT NULL,
                    author TEXT,
                    score REAL,
                    review_date TEXT,
                    spoiler INTEGER NOT NULL DEFAULT 0,
                    quote TEXT,
                    review_json TEXT NOT NULL,
                    scraped_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sync_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_critic_reviews_slug
                    ON critic_reviews(slug);

                CREATE INDEX IF NOT EXISTS idx_game_slugs_sitemap_url
                    ON game_slugs(sitemap_url);

                CREATE INDEX IF NOT EXISTS idx_user_reviews_slug
                    ON user_reviews(slug);
                """
            )
            self._ensure_column("games", "cover_url", "TEXT")
            self.conn.commit()

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        cursor = self.conn.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {str(row[1]) for row in cursor.fetchall()}
        if column_name in existing_columns:
            return
        self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def upsert_game(
        self,
        *,
        slug: str,
        product_payload: dict,
        critic_summary_payload: dict | None,
        user_summary_payload: dict | None,
        cover_url: str | None = None,
    ) -> None:
        item = product_payload.get("data", {}).get("item", {})
        critic_summary_item = (critic_summary_payload or {}).get("data", {}).get("item", {})
        user_summary_item = (user_summary_payload or {}).get("data", {}).get("item", {})

        critic_score = critic_summary_item.get("score")
        critic_review_count = (
            critic_summary_item.get("reviewCount")
            or critic_summary_item.get("ratingsCount")
            or critic_summary_item.get("ratingCount")
        )
        user_score = user_summary_item.get("score")
        user_review_count = (
            user_summary_item.get("reviewCount")
            or user_summary_item.get("ratingsCount")
            or user_summary_item.get("ratingCount")
        )

        with self._lock:
            self.conn.execute(
                """
                INSERT INTO games (
                    slug, game_id, title, platform, release_date, premiere_year, rating,
                    critic_score, critic_review_count, user_score, user_review_count, cover_url,
                    product_json, critic_summary_json, user_summary_json, scraped_at
                ) VALUES (
                    :slug, :game_id, :title, :platform, :release_date, :premiere_year, :rating,
                    :critic_score, :critic_review_count, :user_score, :user_review_count, :cover_url,
                    :product_json, :critic_summary_json, :user_summary_json, :scraped_at
                )
                ON CONFLICT(slug) DO UPDATE SET
                    game_id=excluded.game_id,
                    title=excluded.title,
                    platform=excluded.platform,
                    release_date=excluded.release_date,
                    premiere_year=excluded.premiere_year,
                    rating=excluded.rating,
                    critic_score=excluded.critic_score,
                    critic_review_count=excluded.critic_review_count,
                    user_score=excluded.user_score,
                    user_review_count=excluded.user_review_count,
                    cover_url=excluded.cover_url,
                    product_json=excluded.product_json,
                    critic_summary_json=excluded.critic_summary_json,
                    user_summary_json=excluded.user_summary_json,
                    scraped_at=excluded.scraped_at
                """,
                {
                    "slug": slug,
                    "game_id": item.get("id"),
                    "title": item.get("title"),
                    "platform": item.get("platform"),
                    "release_date": item.get("releaseDate"),
                    "premiere_year": item.get("premiereYear"),
                    "rating": item.get("rating"),
                    "critic_score": critic_score,
                    "critic_review_count": critic_review_count,
                    "user_score": user_score,
                    "user_review_count": user_review_count,
                    "cover_url": cover_url,
                    "product_json": _json_dumps(product_payload),
                    "critic_summary_json": _json_dumps(critic_summary_payload) if critic_summary_payload else None,
                    "user_summary_json": _json_dumps(user_summary_payload) if user_summary_payload else None,
                    "scraped_at": _utc_now_iso(),
                },
            )
            self.conn.commit()

    def upsert_critic_reviews(self, slug: str, reviews: Iterable[dict]) -> int:
        rows = []
        now = _utc_now_iso()
        for review in reviews:
            rows.append(
                {
                    "slug": slug,
                    "review_key": _critic_review_key(review),
                    "score": review.get("score"),
                    "review_date": review.get("date"),
                    "author": review.get("author"),
                    "publication_name": review.get("publicationName"),
                    "source_url": review.get("url"),
                    "quote": review.get("quote"),
                    "review_json": _json_dumps(review),
                    "scraped_at": now,
                }
            )
        if not rows:
            return 0
        with self._lock:
            self.conn.executemany(
                """
                INSERT INTO critic_reviews (
                    slug, review_key, score, review_date, author, publication_name,
                    source_url, quote, review_json, scraped_at
                ) VALUES (
                    :slug, :review_key, :score, :review_date, :author, :publication_name,
                    :source_url, :quote, :review_json, :scraped_at
                )
                ON CONFLICT(slug, review_key) DO UPDATE SET
                    score=excluded.score,
                    review_date=excluded.review_date,
                    author=excluded.author,
                    publication_name=excluded.publication_name,
                    source_url=excluded.source_url,
                    quote=excluded.quote,
                    review_json=excluded.review_json,
                    scraped_at=excluded.scraped_at
                """,
                rows,
            )
            self.conn.commit()
        return len(rows)

    def upsert_user_reviews(self, slug: str, reviews: Iterable[dict]) -> int:
        rows = []
        now = _utc_now_iso()
        for review in reviews:
            rows.append(
                {
                    "review_id": _user_review_key(review),
                    "slug": slug,
                    "author": review.get("author"),
                    "score": review.get("score"),
                    "review_date": review.get("date"),
                    "spoiler": 1 if review.get("spoiler") else 0,
                    "quote": review.get("quote"),
                    "review_json": _json_dumps(review),
                    "scraped_at": now,
                }
            )
        if not rows:
            return 0
        with self._lock:
            self.conn.executemany(
                """
                INSERT INTO user_reviews (
                    review_id, slug, author, score, review_date, spoiler, quote, review_json, scraped_at
                ) VALUES (
                    :review_id, :slug, :author, :score, :review_date, :spoiler, :quote, :review_json, :scraped_at
                )
                ON CONFLICT(review_id) DO UPDATE SET
                    slug=excluded.slug,
                    author=excluded.author,
                    score=excluded.score,
                    review_date=excluded.review_date,
                    spoiler=excluded.spoiler,
                    quote=excluded.quote,
                    review_json=excluded.review_json,
                    scraped_at=excluded.scraped_at
                """,
                rows,
            )
            self.conn.commit()
        return len(rows)

    def upsert_game_slugs(self, game_slugs: Iterable[tuple[str, str, str]]) -> tuple[int, int, int]:
        now = _utc_now_iso()
        row_by_slug: dict[str, dict[str, str]] = {}
        for slug, game_url, sitemap_url in game_slugs:
            normalized_slug = str(slug).strip()
            if not normalized_slug:
                continue
            row_by_slug[normalized_slug] = {
                "slug": normalized_slug,
                "game_url": str(game_url),
                "sitemap_url": str(sitemap_url),
                "discovered_at": now,
                "last_seen_at": now,
            }

        rows = list(row_by_slug.values())
        if not rows:
            return 0, 0, 0

        placeholders = ",".join("?" for _ in rows)
        with self._lock:
            cursor = self.conn.execute(
                f"SELECT slug FROM game_slugs WHERE slug IN ({placeholders})",
                tuple(row["slug"] for row in rows),
            )
            existing_slugs = {str(row[0]) for row in cursor.fetchall()}
            self.conn.executemany(
                """
                INSERT INTO game_slugs (
                    slug, game_url, sitemap_url, discovered_at, last_seen_at
                ) VALUES (
                    :slug, :game_url, :sitemap_url, :discovered_at, :last_seen_at
                )
                ON CONFLICT(slug) DO UPDATE SET
                    game_url=excluded.game_url,
                    sitemap_url=excluded.sitemap_url,
                    last_seen_at=excluded.last_seen_at
                """,
                rows,
            )
            self.conn.commit()

        inserted = sum(1 for row in rows if row["slug"] not in existing_slugs)
        updated = len(rows) - inserted
        return len(rows), inserted, updated

    def count_rows(self, table_name: str) -> int:
        with self._lock:
            cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            return int(cursor.fetchone()[0])

    def list_game_cover_urls(
        self,
        *,
        slug: str | None = None,
        limit: int | None = None,
    ) -> list[tuple[str, str]]:
        query = """
            SELECT slug, cover_url
            FROM games
            WHERE cover_url IS NOT NULL AND TRIM(cover_url) != ''
        """
        params: list[object] = []
        if slug:
            query += " AND slug = ?"
            params.append(slug)
        query += " ORDER BY slug ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        with self._lock:
            cursor = self.conn.execute(query, tuple(params))
            rows = cursor.fetchall()
        return [(str(row[0]), str(row[1])) for row in rows]

    def get_state(self, key: str) -> str | None:
        with self._lock:
            cursor = self.conn.execute(
                "SELECT state_value FROM sync_state WHERE state_key = ?",
                (key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return str(row[0])

    def set_state(self, key: str, value: str) -> None:
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO sync_state (state_key, state_value, updated_at)
                VALUES (:state_key, :state_value, :updated_at)
                ON CONFLICT(state_key) DO UPDATE SET
                    state_value=excluded.state_value,
                    updated_at=excluded.updated_at
                """,
                {
                    "state_key": key,
                    "state_value": value,
                    "updated_at": _utc_now_iso(),
                },
            )
            self.conn.commit()
