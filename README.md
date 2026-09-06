# Scrapling_2.0_superhot

基于 [Scrapling](https://github.com/D4Vinci/Scrapling) 框架的 eMAG 卖家后台商品数据抓取工具。

通过模拟浏览器请求 eMAG 后台 API 接口，循环翻页抓取海量商品数据（实测支持 21 万+ 条），支持多线程并发、断点续传、失败重试、登录失效检测，输出标准 JSON 文件。

当前版本：**1.0.2**

## 版本日志

### [v1.0.2](https://github.com/bendankill/Scrapling_2.0_superhot/tree/v1.0.2)（2026-09-06）

- 新增 `/ui/offer/images` 图片接口，通过 PNK 关联商品图片
- 新增 `/commission/estimate` 佣金接口，通过 PNK 关联佣金百分比
- 修正 `part_number_key` 的业务含义为 PNK码
- 重构 JSON 输出为中文业务字段
- 新增 `category_path` 最多五级类目拆分

### [v1.0.1](https://github.com/bendankill/Scrapling_2.0_superhot/tree/v1.0.1)（2026-09-05）

- JSON 输出文件使用 `YYYYMMDD_HHMMSS_PER_PAGE_MAX_PAGE.json` 命名
- 支持可配置多线程批次并发抓取（`CONCURRENCY`，`1` = 单线程）
- 修复连续 403 后仍继续创建请求批次的问题
- 增强 JSON 写入异常/中断时的回滚保护
- 修复同一秒启动多个相同配置任务时可能争用同一个输出文件的问题，采用原子文件占位避免覆盖和数据竞争

## 功能特性

- 基于 Scrapling `Fetcher`（curl_cffi 引擎）+ `impersonate='chrome'` 模拟浏览器 TLS 指纹，绕过 WAF 拦截
- 多线程批次并发：每批同时请求 `CONCURRENCY` 个页面，主线程按页码顺序统一写入，速度可控（`1` = 单线程）
- 自动循环翻页：优先按接口返回的总页数自动停止，兜底安全上限防死循环
- 断点续传：每页抓完立即写入文件并记录进度，中途中断重跑不丢数据（可通过 `RESUME` 配置开关）
- 失败重试：每页失败自动重试，仍失败则记录错误日志并跳过，程序绝不中断
- 登录失效检测：Cookie 过期时自动识别（302 跳转/登录页响应/连续 403），明确提示并停止
- 防封禁：每批并发请求之间随机暂停（`SLEEP_MIN`~`SLEEP_MAX` 秒）
- 输出标准 JSON 数组文件（文件名带时间戳，可直接用 pandas 读取，历史结果不会被覆盖删除）
- 安全输出文件命名：时间戳 JSON 采用原子文件占位，同一秒启动多个相同配置任务时不会争用、覆盖或删除同一个结果文件
- 三接口联合抓取：主商品接口 + 图片接口 + 佣金接口，通过 PNK 自动关联
- 支持最多五级类目拆分
- 输出字段改为中文业务字段
- JSON + CSV 双格式输出：每次抓取同步生成同基名 `.json` 和 `.csv` 文件

## 环境要求

- Python 3.10+
- Scrapling 0.4.13+（`python -m pip install -U "scrapling[fetchers]"`）

## 快速开始

```bash
# 1. 安装依赖
python -m pip install -U "scrapling[fetchers]"

# 2. 配置 Cookie（见下文），保存到项目目录 cookies.txt

# 3. 运行
python emag_scraper.py
```

## 获取 Cookie

1. 用 Chrome 登录 eMAG 卖家后台（https://marketplace.emag.ro），进入商品列表页
2. 按 `F12` 打开开发者工具 → 切到 `Network`（网络）标签
3. 按 `F5` 刷新页面，在筛选框输入 `opportunities` 找到接口请求
4. 右键该请求 → `Copy` → `Copy as cURL (bash)`
5. 粘贴到记事本，复制 `-b '...'` 单引号内的全部内容
6. 覆盖保存到项目目录下的 `cookies.txt`（一行）

> Cookie 会过期（一般几小时到 1 天）。过期后重新复制覆盖即可，已抓数据不会丢。

## 配置说明（脚本顶部配置区）

| 配置项 | 说明 |
| --- | --- |
| `PER_PAGE` | 每页条数。实测服务端接受 100 条/页（21 万条共 2134 页，约 2 小时） |
| `MAX_PAGE` | 翻页安全上限。接口会返回总页数，抓满自动停止；建议设 10000 |
| `CONCURRENCY` | 并发数量：同时请求的页面数量，`1` = 单线程模式；`5` = 每批同时抓 5 页。配置小于 1 时自动按 1 处理并提示 |
| `RESUME` | 断点续传开关：`True` 从上次进度继续；`False` 新建时间戳文件从头抓（历史 JSON 不会被删除） |
| `MAX_RETRIES` | 每页失败重试次数（默认 2） |
| `SLEEP_MIN` / `SLEEP_MAX` | V1.0.1 多线程模式下表示**并发批次之间的**随机暂停秒数（防封禁），不再是每页暂停 |

## 输出格式与文件命名

每次运行同步生成**一对带时间戳的结果文件**（JSON + CSV，同基名），命名规则：

```text
YYYYMMDD_HHMMSS_PER_PAGE_MAX_PAGE.json
YYYYMMDD_HHMMSS_PER_PAGE_MAX_PAGE.csv
```

- `YYYYMMDD_HHMMSS`：程序启动时的本机时间（精确到秒，**两个文件使用同一个时间戳**，整个任务期间文件名不变）
- 例如 2026-09-06 01:30:00 启动、`PER_PAGE=100`、`MAX_PAGE=200`：

```text
20260906_013000_100_200.json
20260906_013000_100_200.csv
```

- **两个文件的字段、记录数量、记录顺序完全一致**；CSV 使用 UTF-8 BOM 编码，Excel / LibreOffice 直接打开中文不乱码
- 历史抓取结果不会被删除或覆盖，可放心反复运行。若相同配置任务在同一秒同时启动：程序会原子占用一对输出文件名；发生同秒冲突时，后启动的任务会等待到下一个可用秒级文件名，不会覆盖已有结果。
- JSON 文件内容为标准 JSON 数组（全部为中文业务字段）：

```json
[{
  "标题": "Chilot brazilian gri TF18, de dama, Uniconf, XL",
  "品牌": "Uniconf",
  "一级类": "Apparel Woman",
  "二级类": "Women Lingerie & Pijamas",
  "三级类": "Women Panties",
  "四级类": "",
  "五级类": "",
  "产品类型": "",
  "PNK码": "DQSQ7MBBM",
  "最低价": "14.26",
  "图片": "https://s13emagst.akamaized.net/.../image.jpg",
  "颜色": "",
  "评论数量": "2",
  "商品评分": "5",
  "完整类目": "Apparel Woman > Women Lingerie & Pijamas > Women Panties",
  "是否属于高风险类目": "false",
  "是否二手": "SGR包装",
  "当前账号是否允许在该类目添加 Offer": "1",
  "页数": "1",
  "佣金": "23%",
  "每页数量": "100"
}]
```

- 字段：`标题`（product_name）、`品牌`（brand_name）、`一级类`~`五级类`（由 `category_path` 按 `>` 拆分，最多五级）、`产品类型`（属性 `Tip produs`，主接口未返回时为空）、`PNK码`（`part_number_key`）、`最低价`（best_price）、`图片`（images 接口按 PNK 关联）、`颜色`（属性 `Culoare`，主接口未返回时为空）、`评论数量`、`商品评分`、`完整类目`（原始 category_path）、`是否属于高风险类目`、`是否二手`、`当前账号是否允许在该类目添加 Offer`、`页数`、`佣金`（estimate 接口按 PNK 关联，百分比）、`每页数量`
- `页数` 字段用于断点续传
- CSV 文件第一行是同样的 21 个中文字段表头，之后每行对应一条商品记录
- 用 pandas 读取：`pd.read_json('20260906_013000_100_200.json')` / `pd.read_csv('20260906_013000_100_200.csv', encoding='utf-8-sig')`

## 断点续传（RESUME = True）

时间戳文件名下程序会**自动查找最近一次与当前 `PER_PAGE` + `MAX_PAGE` 配置匹配**的抓取结果文件：

1. 扫描当前目录中形如 `*_100_200.json`（按你当前的 PER_PAGE/MAX_PAGE）的文件
2. 取文件名时间戳最新的一个，从它的 `progress.txt` 进度继续追加写入
3. 若没有找到匹配文件，则新建时间戳文件从头开始（并清理旧进度，避免错位续传）

> 更换 `PER_PAGE` 或 `MAX_PAGE` 后，旧结果不会与新配置混写；`RESUME=False` 则每次运行都新建一个时间戳文件。

## 常见问题

**提示"登录已失效（Cookie 过期）"**：按上文步骤重新复制 Cookie 覆盖 cookies.txt，重启脚本即可，已抓数据不丢。

**连续收到 403**：连续 3 个页面最终返回 403 时程序会自动停止（不再创建新的请求批次），Cookie 或 `aws-waf-token` 可能已失效，更新 cookies.txt 后重新运行即可（已抓数据不丢）。

**想从头重新抓**：把 `RESUME` 改成 `False`，运行时会新建一个时间戳文件从头抓；历史时间戳 JSON 文件不会被删除。

## 免责声明

本项目仅用于抓取自己在 eMAG 卖家后台账号下的商品数据，请遵守平台服务条款，合理控制请求频率。
