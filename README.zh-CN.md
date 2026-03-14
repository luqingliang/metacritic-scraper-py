# Gamecritic

[English](./README.md)

用于抓取 Metacritic 游戏数据的 Python 爬虫，重点能力：

- 从官方游戏 sitemap 发现游戏
- 从 Metacritic 后端 JSON 接口抓取游戏详情
- 抓取媒体评分/用户评分评论分页数据
- 使用 SQLite 持久化抓取结果和同步检查点
- 支持将抓取结果导出为 Excel
- 支持下载封面实体图片文件

## 运行要求

- Python 3.10+

## 快速开始

```bash
# 在项目根目录中创建虚拟环境并安装项目
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .

# 启动交互模式
gamecritic
# 或：gamecritic interactive
```

## CLI Settings

`config/cli_settings.json` 是交互模式和普通 CLI 命令共用的一份共享配置文件。
你既可以直接手动编辑这个文件，也可以在 `gamecritic interactive`
里通过 `set <key> <value>` 和 `reset` 这类命令修改同一份配置。
现在除位置参数外，运行参数都统一从这份共享配置读取，不再为每个子命令单独提供一套 CLI 选项。

参数说明：

```jsonc
{
  // SQLite 数据库路径
  "db": "data/gamecritic.db",

  // 在 `crawl` / `crawl-one` 时抓取媒体评论
  "include_critic_reviews": false,

  // 在 `crawl` / `crawl-one` 时抓取用户评论
  "include_user_reviews": false,

  // 每页请求的评论条数
  "review_page_size": 50,

  // 每个游戏最多抓取的评论页数
  "max_review_pages": 1,

  // 批量抓取任务的并发 worker 数
  "concurrency": 4,

  // 单次请求超时时间，单位为秒
  "timeout": 30.0,

  // 单次请求的最大重试次数
  "max_retries": 4,

  // 重试退避间隔，单位为秒
  "backoff": 1.5,

  // 常规请求之间的间隔，单位为秒
  "delay": 0.2,

  // 抓取时是否顺带下载封面文件
  "download_covers": false,

  // 封面文件下载目录
  "covers_dir": "data/covers",

  // 是否覆盖已存在的封面文件
  "overwrite_covers": false,

  // Excel 导出文件路径
  "export_output": "data/excel/gamecritic_export.xlsx"
}
```

## 常用命令

```bash
# 进入交互模式
gamecritic interactive

# 抓取 `game_slugs` 表中的全部 slug
gamecritic crawl

# 按游戏名称在本地 slug 索引里查找最佳匹配
gamecritic search-slug "The Legend of Zelda Breath of the Wild"

# 按 slug 抓取单个游戏
gamecritic crawl-one the-legend-of-zelda-breath-of-the-wild

# 为 `games` 表中已保存的游戏补抓评论
gamecritic crawl-reviews

# 按 slug 为单个已抓取游戏补抓评论
gamecritic crawl-reviews the-legend-of-zelda-breath-of-the-wild

# 将 sitemap 中的全部 slug 同步到 SQLite
gamecritic sync-slugs

# 基于已抓取游戏信息批量下载封面图片实体
gamecritic download-covers

# 按 slug 下载单个游戏的封面图片实体
gamecritic download-covers the-legend-of-zelda-breath-of-the-wild

# 导出 SQLite 数据到 Excel
gamecritic export-excel

# 在保留表结构的前提下一键清空所有业务表
gamecritic clear-db
```

## 数据表结构

SQLite 表：

- `games`：保存抓取到的游戏基础信息、评分摘要、封面链接，以及原始 product/summary JSON 快照。
- `game_slugs`：保存从 sitemap 同步得到的 slug 索引，以及来源 sitemap 和发现时间信息。
- `critic_reviews`：保存与游戏 slug 关联的媒体评论数据。
- `user_reviews`：保存按 `review_id` 去重、并关联到游戏 slug 的用户评论数据。
- `sync_state`：保存轻量级键值状态，例如同步进度检查点。

## 许可证

本项目使用 MIT License，详见 [LICENSE](./LICENSE)。

## 注意事项

- 大规模抓取前请先确认目标站点规则与条款。
- 请使用合理请求速率，并避免抓取 Metacritic `robots.txt` 明确禁止的路径：`https://www.metacritic.com/robots.txt`。
