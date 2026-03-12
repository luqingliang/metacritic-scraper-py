# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Planned: multi-domain crawling support (Movies / TV Shows / Music).

## [0.1.8] - 2026-03-12

### Added
- Shared `config/cli_settings.json` template for persistent CLI defaults.
- Interactive mode can now run `crawl-reviews` as a background command.
- Regression coverage for shared settings, crawl auto-sync, and review crawl workflows.

### Changed
- `crawl`, `crawl-one`, `crawl-reviews`, `sync-slugs`, `download-covers`, and `export-excel` now read from one shared settings profile.
- Interactive `set` / `reset` changes are now reused by later non-interactive command runs.
- README documentation was reorganized around quick start, CLI settings, and common command reference sections.

## [0.1.7] - 2026-03-11

### Added
- New `sync-slugs` command to sync game slugs from Metacritic sitemaps into SQLite.
- Automatic slug sync before `crawl` when the stored sitemap checkpoint is missing, invalid, or stale.
- New `clear-db` command to delete project data while keeping the SQLite schema intact.
- Regression coverage for full crawl, slug sync, and storage helper flows.

### Changed
- `crawl` now reads from stored SQLite slugs by default, separates critic/user review switches, and defaults to concurrency `4`.
- Interactive mode now exposes slug sync status and supports `sync-slugs`, `export-excel`, and `clear-db` workflows directly.
- Excel export now defaults to `data/excel/gamecritic_export.xlsx`.

### Fixed
- Crawl progress logs now include per-slug completion counters and clearer stop/failure states.
- Review crawling now tolerates critic and user review availability independently.
- Slug sync upserts now deduplicate repeated sitemap entries before writing to SQLite.

## [0.1.6] - 2026-03-09

### Added
- Interactive `stop` command for background `crawl`, `crawl-one`, `slugs`, and `download-covers` tasks.
- Client-level stop regression tests for in-flight request cancellation handling.

### Changed
- Interactive welcome/help text now reflects the current stop-capable command set.
- Package version metadata is now aligned across `pyproject.toml` and `gamecritic.__version__`.

### Fixed
- Background stop requests now propagate through the CLI, scraper, HTTP client, and cover download flow.
- Cover downloads no longer swallow `InterruptedError`, and interrupted downloads clean up temporary `.part` files.
- Interactive crawl state cleanup now avoids repeated stop exceptions during concurrent worker shutdown.

## [0.1.5] - 2026-03-07

### Changed
- Interactive mode now keeps the command prompt responsive while streaming command output directly to terminal output.
- Interactive log highlighting now colors only the log header for `WARNING`, `ERROR`, and `CRITICAL` levels.

### Removed
- Interactive `clear` / `cls` command and related help entries.

## [0.1.4] - 2026-03-06

### Added
- Optional cover image binary download during `crawl` / `crawl-one` via `--download-covers`.
- New `download-covers` command to batch download cover image files from existing `games.cover_url`.
- New cover downloader module and tests for downloader + cover URL storage query flow.

### Changed
- Interactive mode settings and help now include cover download controls.
- Ignore downloaded cover files under `data/covers/`.
- Roadmap text clarifies crawl-time and post-crawl cover sync modes.

## [0.1.3] - 2026-03-06

### Added
- Persist game cover URL in `games.cover_url`, built from Product image `bucketPath`.
- Cover URL helper tests and schema migration test for legacy databases.

### Changed
- Excel exporter now includes `cover_url` when present.
- Excel export column selection is schema-aware for backward compatibility.
- Roadmap updated to document current cover strategy (URL-only) and optional future binary sync.

## [0.1.2] - 2026-03-05

### Added
- Interactive CLI TUI with persistent bottom input box.
- Chinese interactive help command: `help-zh` / `帮助`.
- Chinese settings view: `show-zh` (also supports `show zh`).
- Colored settings output in TUI to improve readability.

### Changed
- `show` now displays settings with English explanations.
- Settings output format updated to `key = value  # explanation`.
- Quick-start defaults improved for out-of-box use:
  - `include_reviews = true`
  - `max_review_pages = 1`
  - `max_games = 50`
- Added `--no-include-reviews` support for `crawl` and `crawl-one`.

### Fixed
- In TUI mode, `clear` keeps the top banner after clearing output.
- Better fallback behavior for non-TTY sessions (plain REPL).

## [0.1.1] - 2026-03-05

### Added
- Optional concurrent crawling via `--concurrency`.
- MIT license.

### Changed
- Roadmap documentation shifted to function-oriented planning.

## [0.1.0] - 2026-03-05

### Added
- Initial project setup and first crawler workflow.
- SQLite-based storage for crawled game data.
- Core CLI commands for crawling and basic data operations.

[Unreleased]: https://github.com/luqingliang/gamecritic/compare/v0.1.8...HEAD
[0.1.8]: https://github.com/luqingliang/gamecritic/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/luqingliang/gamecritic/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/luqingliang/gamecritic/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/luqingliang/gamecritic/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/luqingliang/gamecritic/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/luqingliang/gamecritic/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/luqingliang/gamecritic/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/luqingliang/gamecritic/compare/release/v0.1.0...v0.1.1
[0.1.0]: https://github.com/luqingliang/gamecritic/tree/release/v0.1.0
