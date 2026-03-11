# metacritic-scraper-py

[English](./README.md)

用于抓取 Metacritic 游戏数据的 Python 爬虫，重点能力：

- 从官方游戏 sitemap 发现游戏
- 从 Metacritic 后端 JSON 接口抓取游戏详情
- 抓取媒体评分/用户评分评论分页数据
- 使用 SQLite 持久化抓取结果和同步检查点

## 功能特性

- 使用 `https://www.metacritic.com/games.xml` 作为主要种子来源。
- 抓取游戏详情接口（`Product`）和评分摘要接口。
- 按分页抓取媒体评论与用户评论（`offset/limit`）。
- 以规范化字段 + 原始 JSON 方式落库，便于追溯与二次处理。
- 支持把 sitemap 全量 slug 同步到独立的 `game_slugs` 表。
- 内置重试与退避，提升网络波动下的稳定性。
- 支持导出 `.xlsx`，方便人工检查抓取结果。

## 运行要求

- Python 3.10+

## 安装

```bash
cd /home/luqingliang/projects/metacritic-scraper-py
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

## 快速开始

1) 启动交互模式（常驻 REPL）：

```bash
metacritic-scraper
# 或：metacritic-scraper interactive
```

交互界面为“底部固定输入框（`metacritic>`）+ 上方可滚动输出区”。
按 `Enter` 执行命令，按 `Ctrl-C`/`Ctrl-D` 退出。
当会话不是 TTY（例如通过管道输入）时，会自动回退到普通 REPL 模式。

在交互模式里可直接输入：

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

## 开箱默认配置

为了更利于上手，`crawl` 和交互模式默认使用“快速上手配置”：

- `include_critic_reviews = false`
- `include_user_reviews = false`
- `max_review_pages = 1`
- `concurrency = 4`

全量抓取现在默认会处理 `game_slugs` 表中的全部 slug。

2) 抓取单个游戏：

```bash
metacritic-scraper crawl-one the-legend-of-zelda-breath-of-the-wild --db data/metacritic.db --include-critic-reviews --include-user-reviews --max-review-pages 2
```

3) 抓取 `game_slugs` 表中的全部 slug：

```bash
metacritic-scraper crawl --db data/metacritic.db --include-critic-reviews --include-user-reviews --max-review-pages 1
```

可选：在抓取游戏信息时同时下载封面图片实体（默认关闭）。

```bash
metacritic-scraper crawl --db data/metacritic.db --download-covers --covers-dir data/covers
```

可选：开启并发抓取（例如 4 个 worker）。

```bash
metacritic-scraper crawl --concurrency 4 --db data/metacritic.db --include-critic-reviews --include-user-reviews
```

4) 将 sitemap 中的全部 slug 同步到 SQLite：

```bash
metacritic-scraper sync-slugs --db data/metacritic.db
```

5) 基于已抓取游戏信息批量下载封面图片实体：

```bash
metacritic-scraper download-covers --db data/metacritic.db --output-dir data/covers
```

6) 导出 SQLite 数据到 Excel：

```bash
metacritic-scraper export-excel --db data/metacritic.db --output data/excel/metacritic_export.xlsx
```

7) 在保留表结构的前提下一键清空所有业务表：

```bash
metacritic-scraper clear-db --db data/metacritic.db
```

## CLI 概览

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

## 数据表结构

SQLite 表：

- `games`
- `game_slugs`
- `critic_reviews`
- `user_reviews`
- `sync_state`

每张表都保存关键规范化字段和原始 JSON（`*_json`），便于后续重放解析。
其中 `games.cover_url` 用于保存封面图链接，直接由产品 `bucketPath` 组装为 catalog 原图地址（`/a/img/catalog/...`）。
`game_slugs` 用于保存 sitemap slug 索引，并记录 `game_url`、`sitemap_url`、`discovered_at` 和 `last_seen_at`。
`sync_state` 用于保存轻量级检查点，比如
`game_slugs_last_successful_full_sync_at`。

## 许可证

本项目使用 MIT License，详见 [LICENSE](./LICENSE)。

## 项目路书

- [x] 抓取游戏详情与评论
- [x] 支持 Excel 导出
- [x] 支持并发抓取（`--concurrency`）
- [x] 支持交互 CLI 模式
- [x] 保存封面链接到 `games.cover_url`
- [x] 支持将 sitemap slug 索引同步到 `game_slugs`
- [x] 抓取时可选下载封面（`--download-covers`）
- [x] 支持基于数据库批量下载封面（`download-covers`）
- [ ] 扩展电影数据
- [ ] 扩展电视剧/节目数据
- [ ] 扩展音乐数据

## 注意事项

- 大规模抓取前请先确认目标站点规则与条款。
- 请使用合理请求速率，并避免抓取 `robots.txt` 明确禁止的路径。
