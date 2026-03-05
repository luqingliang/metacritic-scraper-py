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

1) Crawl one game:

```bash
metacritic-scraper crawl-one the-legend-of-zelda-breath-of-the-wild --db data/metacritic.db --include-reviews --max-review-pages 2
```

2) Crawl from sitemap (example: first 50 games):

```bash
metacritic-scraper crawl --max-games 50 --db data/metacritic.db --include-reviews --max-review-pages 1
```

Optional: enable concurrent workers (for example 4 workers).

```bash
metacritic-scraper crawl --max-games 50 --concurrency 4 --db data/metacritic.db --include-reviews
```

3) Crawl incrementally by release date (switch on):

```bash
metacritic-scraper crawl --incremental-by-date --db data/metacritic.db --include-reviews --max-review-pages 1
```

4) Crawl incrementally with explicit date override:

```bash
metacritic-scraper crawl --incremental-by-date --since-date 2026-03-01 --lookback-days 2 --db data/metacritic.db
```

5) Export slugs from sitemap:

```bash
metacritic-scraper slugs --limit-slugs 100 --output data/slugs.txt
```

6) Export SQLite data to Excel:

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
metacritic-scraper slugs --help
metacritic-scraper export-excel --help
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
- `critic_reviews`
- `user_reviews`

Each table stores essential normalized fields and raw JSON payloads (`*_json`) for future reprocessing.

## License

This project is licensed under the MIT License. See [LICENSE](./LICENSE).

## Roadmap

- [x] Game coverage: crawl game details, critic reviews, and user reviews
- [x] Result inspection: export to Excel for manual QA checks
- [x] Optional concurrent crawl: speed up batch crawling with `--concurrency`
- [ ] Domain expansion: add Movies crawling
- [ ] Domain expansion: add TV Shows crawling
- [ ] Domain expansion: add Music crawling
- [ ] Unified content hub: query across Games/Movies/TV/Music in one dataset
- [ ] Rankings and trends: cross-domain score charts, popularity lists, release trends
- [ ] Delivery layer: lightweight web viewer and scheduled exports (Excel/CSV)

## Notes

- Respect target site rules and terms before large-scale crawling.
- Use moderate request rates and avoid disallowed paths in `robots.txt`.
