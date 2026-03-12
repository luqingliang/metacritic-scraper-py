# Gamecritic

[中文说明](./README.zh-CN.md)

Python crawler for Metacritic game data, focused on:

- game discovery from the official games sitemap
- game detail extraction from Metacritic backend JSON endpoints
- critic/user reviews pagination
- SQLite persistence for crawled data and sync checkpoints
- Excel export for crawled SQLite data
- optional cover image file download

## Requirements

- Python 3.10+

## Quick Start

```bash
# From the project root, create a local virtual environment and install the package
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .

# Start the interactive shell
gamecritic
# or: gamecritic interactive
```

## CLI Settings

`config/cli_settings.json` is the shared settings profile for the interactive shell and the
regular CLI commands.
You can edit this file directly, or update the same settings from
`gamecritic interactive` with commands such as `set <key> <value>` and `reset`.

Parameter reference:

```jsonc
{
  // SQLite database path
  "db": "data/gamecritic.db",

  // Fetch critic reviews during `crawl` / `crawl-one`
  "include_critic_reviews": false,

  // Fetch user reviews during `crawl` / `crawl-one`
  "include_user_reviews": false,

  // Number of reviews requested per page
  "review_page_size": 50,

  // Maximum review pages fetched per game
  "max_review_pages": 1,

  // Number of concurrent workers for batch crawl tasks
  "concurrency": 4,

  // Request timeout in seconds
  "timeout": 30.0,

  // Maximum retry attempts per request
  "max_retries": 4,

  // Retry backoff interval in seconds
  "backoff": 1.5,

  // Delay between requests in seconds
  "delay": 0.2,

  // Download cover files while crawling
  "download_covers": false,

  // Directory for downloaded cover files
  "covers_dir": "data/covers",

  // Overwrite existing cover files
  "overwrite_covers": false,

  // Output path for Excel export
  "export_output": "data/excel/gamecritic_export.xlsx"
}
```

## Common Commands

```bash
# Crawl one game
gamecritic crawl-one the-legend-of-zelda-breath-of-the-wild
```

```bash
# Crawl all stored `game_slugs`
gamecritic crawl
```

```bash
# Backfill reviews for games already stored in `games`
gamecritic crawl-reviews
```

```bash
# Enable `download_covers` in interactive mode before running `crawl`
gamecritic interactive
# then run inside the interactive shell: set download_covers true
```

```bash
# Change `concurrency` in interactive mode, for example to 4 workers
gamecritic interactive
# then run inside the interactive shell: set concurrency 4
```

```bash
# Sync all sitemap slugs into SQLite
gamecritic sync-slugs
```

```bash
# Batch download cover image files from already crawled games
gamecritic download-covers
```

```bash
# Export SQLite data to Excel
gamecritic export-excel
```

```bash
# Clear all project tables while keeping the schema
gamecritic clear-db
```

## Data Schema

SQLite tables:

- `games`: Stores crawled game metadata, score summaries, cover URL, and raw product/summary JSON snapshots.
- `game_slugs`: Stores the sitemap-derived slug index, including source sitemap and discovery timestamps.
- `critic_reviews`: Stores critic review records associated with each game slug.
- `user_reviews`: Stores user review records keyed by review ID and linked back to each game slug.
- `sync_state`: Stores lightweight key-value checkpoints such as sync progress markers.

## License

This project is licensed under the MIT License. See [LICENSE](./LICENSE).

## Notes

- Respect target site rules and terms before large-scale crawling.
- Use moderate request rates and avoid paths disallowed by Metacritic's `robots.txt`: `https://www.metacritic.com/robots.txt`.
