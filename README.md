# metacritic-scraper-py

[中文说明](./README.zh-CN.md)

Python crawler for Metacritic game data, focused on:

- game discovery from the official games sitemap
- game detail extraction from Metacritic backend JSON endpoints
- critic/user reviews pagination
- SQLite persistence for crawled data and sync checkpoints

## Features

- Uses `https://www.metacritic.com/games.xml` as the primary game seed source.
- Crawls game detail endpoint (`Product`) and score summary endpoints.
- Crawls critic and user reviews with pagination (`offset/limit`).
- Stores normalized data + raw JSON payloads into SQLite for traceability.
- Can sync the full sitemap slug inventory into a dedicated `game_slugs` table.
- Includes retry + backoff for unstable network/API responses.
- Exports crawled results to Excel (`.xlsx`) for manual QA.

## Requirements

- Python 3.10+

## Install

```bash
cd /home/luqingliang/projects/metacritic-scraper-py
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

## Quick Start

1) Start interactive shell (persistent session, like a REPL):

```bash
metacritic-scraper
# or: metacritic-scraper interactive
```

Interactive UI uses a fixed bottom input box (`metacritic>`) and a scrollable output pane above it.
Press `Enter` to run a command, `Ctrl-C`/`Ctrl-D` to exit.
If the session is not a TTY (for example piped input), it automatically falls back to plain REPL mode.

Inside interactive shell:

```text
show
help-zh
show-zh
clear-db
set db data/metacritic.db
set concurrency 4
crawl
export-excel data/excel/metacritic_export.xlsx
exit
```

## Quick-Start Defaults

For easier out-of-box usage, `crawl` and interactive mode now use a quick-start profile by default:

- `include_critic_reviews = false`
- `include_user_reviews = false`
- `max_review_pages = 1`
- `concurrency = 4`

Full crawl now processes all slugs stored in `game_slugs` by default.

2) Crawl one game:

```bash
metacritic-scraper crawl-one the-legend-of-zelda-breath-of-the-wild --db data/metacritic.db --include-critic-reviews --include-user-reviews --max-review-pages 2
```

3) Crawl all stored `game_slugs`:

```bash
metacritic-scraper crawl --db data/metacritic.db --include-critic-reviews --include-user-reviews --max-review-pages 1
```

Optional: download cover image files while crawling (disabled by default).

```bash
metacritic-scraper crawl --db data/metacritic.db --download-covers --covers-dir data/covers
```

Optional: enable concurrent workers (for example 4 workers).

```bash
metacritic-scraper crawl --concurrency 4 --db data/metacritic.db --include-critic-reviews --include-user-reviews
```

4) Sync all sitemap slugs into SQLite:

```bash
metacritic-scraper sync-slugs --db data/metacritic.db
```

5) Batch download cover image files from already crawled games:

```bash
metacritic-scraper download-covers --db data/metacritic.db --output-dir data/covers
```

6) Export SQLite data to Excel:

```bash
metacritic-scraper export-excel --db data/metacritic.db --output data/excel/metacritic_export.xlsx
```

7) Clear all project tables while keeping the schema:

```bash
metacritic-scraper clear-db --db data/metacritic.db
```

## CLI Overview

```bash
metacritic-scraper --help
metacritic-scraper crawl --help
metacritic-scraper crawl-one --help
metacritic-scraper sync-slugs --help
metacritic-scraper download-covers --help
metacritic-scraper export-excel --help
metacritic-scraper clear-db --help
metacritic-scraper interactive --help
```

## Data Schema

SQLite tables:

- `games`
- `game_slugs`
- `critic_reviews`
- `user_reviews`
- `sync_state`

Each table stores essential normalized fields and raw JSON payloads (`*_json`) for future reprocessing.
`games.cover_url` stores the cover image URL built from product `bucketPath` (`/a/img/catalog/...`).
`game_slugs` stores the current sitemap slug index with `game_url`, `sitemap_url`, `discovered_at`, and `last_seen_at`.
`sync_state` stores lightweight checkpoints such as
`game_slugs_last_successful_full_sync_at`.

## License

This project is licensed under the MIT License. See [LICENSE](./LICENSE).

## Roadmap

- [x] Crawl game details and reviews
- [x] Export results to Excel
- [x] Optional concurrent crawling (`--concurrency`)
- [x] Interactive CLI mode
- [x] Store cover URLs in `games.cover_url`
- [x] Sync sitemap slug inventory into `game_slugs`
- [x] Optional cover download during crawl (`--download-covers`)
- [x] Batch cover download from DB (`download-covers`)
- [ ] Expand to Movies
- [ ] Expand to TV Shows
- [ ] Expand to Music

## Notes

- Respect target site rules and terms before large-scale crawling.
- Use moderate request rates and avoid disallowed paths in `robots.txt`.
