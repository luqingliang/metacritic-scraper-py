# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Planned: multi-domain crawling support (Movies / TV Shows / Music).

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

[Unreleased]: https://github.com/luqingliang/metacritic-scraper-py/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/luqingliang/metacritic-scraper-py/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/luqingliang/metacritic-scraper-py/compare/release/v0.1.0...v0.1.1
[0.1.0]: https://github.com/luqingliang/metacritic-scraper-py/tree/release/v0.1.0
