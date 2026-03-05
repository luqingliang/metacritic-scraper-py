# metacritic-scraper-py

[English](./README.md)

用于抓取 Metacritic 游戏数据的 Python 爬虫，重点能力：

- 从官方游戏 sitemap 发现游戏
- 从 Metacritic 后端 JSON 接口抓取游戏详情
- 抓取媒体评分/用户评分评论分页数据
- 使用 SQLite 持久化，支持增量抓取

## 功能特性

- 使用 `https://www.metacritic.com/games.xml` 作为主要种子来源。
- 抓取游戏详情接口（`Product`）和评分摘要接口。
- 按分页抓取媒体评论与用户评论（`offset/limit`）。
- 以规范化字段 + 原始 JSON 方式落库，便于追溯与二次处理。
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

1) 抓取单个游戏：

```bash
metacritic-scraper crawl-one the-legend-of-zelda-breath-of-the-wild --db data/metacritic.db --include-reviews --max-review-pages 2
```

2) 从 sitemap 抓取（示例：前 50 个游戏）：

```bash
metacritic-scraper crawl --max-games 50 --db data/metacritic.db --include-reviews --max-review-pages 1
```

可选：开启并发抓取（例如 4 个 worker）。

```bash
metacritic-scraper crawl --max-games 50 --concurrency 4 --db data/metacritic.db --include-reviews
```

3) 开启按日期增量抓取：

```bash
metacritic-scraper crawl --incremental-by-date --db data/metacritic.db --include-reviews --max-review-pages 1
```

4) 按指定日期进行增量抓取（覆盖检查点）：

```bash
metacritic-scraper crawl --incremental-by-date --since-date 2026-03-01 --lookback-days 2 --db data/metacritic.db
```

5) 导出 sitemap 中的 slug：

```bash
metacritic-scraper slugs --limit-slugs 100 --output data/slugs.txt
```

6) 导出 SQLite 数据到 Excel：

```bash
metacritic-scraper export-excel --db data/metacritic.db --output data/metacritic_export.xlsx
```

可选：仅导出单个 slug，并包含原始 JSON 字段。

```bash
metacritic-scraper export-excel --db data/metacritic.db --slug the-legend-of-zelda-breath-of-the-wild --include-raw-json
```

## CLI 概览

```bash
metacritic-scraper --help
metacritic-scraper crawl --help
metacritic-scraper crawl-one --help
metacritic-scraper slugs --help
metacritic-scraper export-excel --help
```

## 增量开关说明

- 默认模式（`crawl` 不加开关）：走 sitemap 全量抓取。
- 开启 `--incremental-by-date`：走 finder 接口并按 `releaseDate` 倒序抓取，遇到早于有效截止日期的数据会停止继续翻页。
- 默认将检查点日期写入数据库键 `games_incremental_release_date`。
- `--since-date YYYY-MM-DD` 可在单次运行中覆盖已保存检查点。
- `--lookback-days` 会回抓安全窗口，降低漏抓晚到更新的风险。

## 数据表结构

SQLite 表：

- `games`
- `critic_reviews`
- `user_reviews`

每张表都保存关键规范化字段和原始 JSON（`*_json`），便于后续重放解析。

## 许可证

本项目使用 MIT License，详见 [LICENSE](./LICENSE)。

## 项目路书

- [x] 游戏数据抓取：支持游戏详情、媒体评论、用户评论的采集与导出
- [x] 数据核查能力：支持 Excel 导出，方便人工抽样检查
- [x] 并发抓取（可选）：支持通过 `--concurrency` 提升批量抓取速度
- [ ] 内容扩展：增加电影（Movies）数据抓取
- [ ] 内容扩展：增加电视剧/节目（TV Shows）数据抓取
- [ ] 内容扩展：增加音乐（Music）数据抓取
- [ ] 统一内容中心：支持在同一数据集里按类别（游戏/电影/电视/音乐）统一查询
- [ ] 榜单与趋势：提供跨类别的评分榜单、热度榜单、发布日期趋势
- [ ] 可视化与交付：提供 Web 查询页和定时导出（Excel/CSV）

## 注意事项

- 大规模抓取前请先确认目标站点规则与条款。
- 请使用合理请求速率，并避免抓取 `robots.txt` 明确禁止的路径。
