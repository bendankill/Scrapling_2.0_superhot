# Scrapling_2.0_superhot

基于 [Scrapling](https://github.com/D4Vinci/Scrapling) 框架的 eMAG 卖家后台商品数据抓取工具。

通过模拟浏览器请求 eMAG 后台 API 接口，循环翻页抓取海量商品数据（实测支持 21 万+ 条），支持断点续传、失败重试、登录失效检测，输出标准 JSON 文件。

当前版本：**1.0.0**

## 版本日志

### [v1.0.2](https://github.com/bendankill/Scrapling_2.0_superhot/tree/v1.0.2)（2026-09-06）

- 新增 `/ui/offer/images` 图片接口，通过 PNK 关联商品图片
- 新增 `/commission/estimate` 佣金接口，通过 PNK 关联佣金百分比
- 修正 `part_number_key` 的业务含义为 PNK码
- 重构 JSON 输出为中文业务字段
- 新增 `category_path` 最多五级类目拆分
- 位于 `v1.0.2` 分支；main 分支代码仍为 v1.0.0

### [v1.0.1](https://github.com/bendankill/Scrapling_2.0_superhot/tree/v1.0.1)（2026-09-05）

- 输出文件改为时间戳命名：`YYYYMMDD_HHMMSS_PER_PAGE_MAX_PAGE.json`，历史结果不会被覆盖删除
- 新增 `CONCURRENCY` 多线程批次并发抓取（`1` = 单线程），主线程按页码顺序统一写入，JSON 线程安全
- 暂停改为并发批次之间的随机暂停（`SLEEP_MIN`~`SLEEP_MAX`）
- `RESUME=True` 自动匹配最近一次同配置（`PER_PAGE`+`MAX_PAGE`）的抓取文件断点续传
- 位于 `v1.0.1` 分支 / `v1.0.1` tag；main 分支仍为 v1.0.0

### [v1.0.0](https://github.com/bendankill/Scrapling_2.0_superhot/tree/v1.0.0)（2026-09-05）

- 首个版本：Scrapling `Fetcher` + `impersonate='chrome'` 模拟浏览器 TLS 指纹，绕过 WAF 拦截
- 循环翻页（接口总页数自动检测 + `MAX_PAGE` 兜底）、断点续传、失败重试、登录失效检测
- 每页随机暂停防封禁，输出 `emag_products.json` 标准 JSON 数组

## 功能特性

- 基于 Scrapling `Fetcher`（curl_cffi 引擎）+ `impersonate='chrome'` 模拟浏览器 TLS 指纹，绕过 WAF 拦截
- 自动循环翻页：优先按接口返回的总页数自动停止，兜底安全上限防死循环
- 断点续传：每页抓完立即写入文件并记录进度，中途中断重跑不丢数据（可通过 `RESUME` 配置开关）
- 失败重试：每页失败自动重试，仍失败则记录错误日志并跳过，程序绝不中断
- 登录失效检测：Cookie 过期时自动识别（302 跳转/登录页响应），明确提示并停止
- 防封禁：每页随机暂停 1.5~3.5 秒
- 输出标准 JSON 数组文件，可直接用 pandas 读取

## 环境要求

- Python 3.9+
- Scrapling 0.4.13+（`pip install -U scrapling`）

## 快速开始

```bash
# 1. 安装依赖
pip install -U scrapling

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
| `RESUME` | 断点续传开关：`True` 从上次进度继续；`False` 清空旧数据从头抓 |
| `MAX_RETRIES` | 每页失败重试次数（默认 3） |
| `SLEEP_MIN` / `SLEEP_MAX` | 每页之间的随机暂停秒数（防封禁） |

## 输出格式

抓取结果保存在 `emag_products.json`，标准 JSON 数组格式：

```json
[{"Title": "...", "Brand": "...", "Category": "...", "PNK": "", "PN": "...", "Image_URL": "", "_page": 1}]
```

- 字段：`Title`（标题）、`Brand`（品牌）、`Category`（分类）、`PNK`、`PN`（零件号）、`Image_URL`（图片）、`_page`（来源页码）
- `_page` 字段用于断点续传，不需要可以忽略
- 用 pandas 读取：`pd.read_json('emag_products.json')`

## 常见问题

**提示"登录已失效（Cookie 过期）"**：按上文步骤重新复制 Cookie 覆盖 cookies.txt，重启脚本即可，已抓数据不丢。

**连续收到 403**：Cookie 或 `aws-waf-token` 过期，同上处理。

**想从头重新抓**：把 `RESUME` 改成 `False`，运行时会自动清空旧数据文件。

## 免责声明

本项目仅用于抓取自己在 eMAG 卖家后台账号下的商品数据，请遵守平台服务条款，合理控制请求频率。
