# metacritic-scraper-py

[中文说明](./README.zh-CN.md)

Python crawler for Metacritic game data, focused on:

- game discovery from the official games sitemap
- game detail extraction from Metacritic backend JSON endpoints
- critic/user reviews pagination
- SQLite persistence for incremental crawling

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
set db data/metacritic.db
set concurrency 4
crawl
export-excel data/metacritic_export.xlsx
exit
```

## Quick-Start Defaults

For easier out-of-box usage, `crawl` and interactive mode now use a quick-start profile by default:

- `include_reviews = true`
- `max_review_pages = 1`
- `max_games = 50`

This means your first run already contains review data but stays bounded in runtime.
You can still override defaults, for example:

```bash
metacritic-scraper crawl --max-games 200 --max-review-pages 3 --no-include-reviews
```

2) Crawl one game:

```bash
metacritic-scraper crawl-one the-legend-of-zelda-breath-of-the-wild --db data/metacritic.db --include-reviews --max-review-pages 2
```

3) Crawl from sitemap (example: first 50 games):

```bash
metacritic-scraper crawl --max-games 50 --db data/metacritic.db --include-reviews --max-review-pages 1
```

Optional: download cover image files while crawling (disabled by default).

```bash
metacritic-scraper crawl --max-games 50 --db data/metacritic.db --download-covers --covers-dir data/covers
```

Optional: enable concurrent workers (for example 4 workers).

```bash
metacritic-scraper crawl --max-games 50 --concurrency 4 --db data/metacritic.db --include-reviews
```

4) Crawl incrementally by release date (switch on):

```bash
metacritic-scraper crawl --incremental-by-date --db data/metacritic.db --include-reviews --max-review-pages 1
```

5) Crawl incrementally with explicit date override:

```bash
metacritic-scraper crawl --incremental-by-date --since-date 2026-03-01 --lookback-days 2 --db data/metacritic.db
```

6) Sync all sitemap slugs into SQLite:

```bash
metacritic-scraper sync-slugs --db data/metacritic.db
```

7) Batch download cover image files from already crawled games:

```bash
metacritic-scraper download-covers --db data/metacritic.db --output-dir data/covers
```

Optional: overwrite existing files or limit this run.

```bash
metacritic-scraper download-covers --db data/metacritic.db --overwrite --limit 200
```

8) Export SQLite data to Excel:

```bash
metacritic-scraper export-excel --db data/metacritic.db --output data/metacritic_export.xlsx
```

Optional: export only one slug and include raw JSON columns.

```bash
metacritic-scraper export-excel --db data/metacritic.db --slug the-legend-of-zelda-breath-of-the-wild --include-raw-json
```

## CLI Overview

```bash
metacritic-scraper --help
metacritic-scraper crawl --help
metacritic-scraper crawl-one --help
metacritic-scraper sync-slugs --help
metacritic-scraper download-covers --help
metacritic-scraper export-excel --help
metacritic-scraper interactive --help
```

## Incremental Toggle

- Default (`crawl` without switch): full crawl from games sitemap.
- With `--incremental-by-date`: use finder endpoint sorted by `releaseDate`, and stop when older than the effective cutoff date.
- Checkpoint date is persisted in DB key `games_incremental_release_date` by default.
- `--since-date YYYY-MM-DD` can override stored checkpoint for one run.
- `--lookback-days` re-crawls a safety window to reduce missed late updates.

## Data Schema

SQLite tables:

- `games`
- `game_slugs`
- `critic_reviews`
- `user_reviews`

Each table stores essential normalized fields and raw JSON payloads (`*_json`) for future reprocessing.
`games.cover_url` stores the cover image URL built from product `bucketPath` (`/a/img/catalog/...`).
`game_slugs` stores the current sitemap slug index with `game_url`, `sitemap_url`, `discovered_at`, and `last_seen_at`.

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
