# -*- coding: utf-8 -*-
"""
eMAG 后台商品数据抓取脚本
==========================
原理：模拟浏览器向 eMAG 后台接口循环发送 POST 请求，按"批次并发"抓取商品数据
      （每批 CONCURRENCY 页同时请求，主线程按页码顺序追加写入 JSON 文件），
      每页依次请求三个接口：opportunities（主商品）→ images（图片）→ estimate（佣金），
      以 PNK码（part_number_key）关联，输出中文业务字段；中途断掉重新运行即可接着抓，
      已抓到的数据不会丢。

使用前准备：
1. 安装依赖（需要 Python 3.10+）：
   python -m pip install -U "scrapling[fetchers]"
   （需要 0.4.13 以上版本，否则请求 API 不同会报错）
2. 把浏览器里复制的完整 Cookie 保存到同目录下的 cookies.txt 文件中（一行）
   Cookie 会过期（一般几小时到 1 天），过期后重新复制并覆盖 cookies.txt 即可
3. 运行：      python emag_scraper.py
"""

import glob
import json
import math
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from scrapling.fetchers import Fetcher

# ==================== 一、配置区（按需修改） ====================

VERSION = '1.0.2'      # 程序版本号

# 三个接口（都用同一份登录态 Cookie，通过 PNK码/part_number_key 关联）
API_URL = 'https://marketplace.emag.ro/api-ui/opportunities/'      # 主接口：商品数据 + 翻页入口
IMAGES_API_URL = 'https://marketplace.emag.ro/ui/offer/images'         # 图片接口：按 PNK 批量取图片 URL
ESTIMATE_API_URL = 'https://marketplace.emag.ro/commission/estimate'   # 佣金接口：按 PNK 批量取佣金百分比

PER_PAGE = 100       # 每页条数。实测服务端接受 100 条/页：100 条时总页数 2134、全程约 2 小时；
                    # 25 条时总页数约 8535、全程约 8 小时。想抓得快就把这里改成 100
MAX_PAGE = 2        # 翻页安全上限（当前是测试值：只抓 5 页验证效果）。
                    # 接口会返回总页数，脚本抓满会自动停止；测试没问题后改成 10000 再正式开跑
RESUME = False       # 断点续传开关：True = 从上次进度继续抓；False = 清空旧数据、从第 1 页重新抓
CONCURRENCY = 1     # 并发数量：同时请求的页面数（一批请求的页数）。1 = 单线程模式；
                    # 5 = 每批同时抓 5 页；想调节抓取速度只改这个数字即可
MAX_RETRIES = 2     # 每一页失败后的重试次数（登录失效不重试，会直接提示并停止）
SLEEP_MIN = 1.5     # 每批并发请求之间的随机暂停最小秒数（V1.0.1 起按"批"暂停，不再是每页暂停）
SLEEP_MAX = 3.5     # 每批并发请求之间的随机暂停最大秒数

JSON_FILE = ''      # 输出文件名：程序启动时按 {YYYYMMDD_HHMMSS}_{PER_PAGE}_{MAX_PAGE}.json 动态生成
PROGRESS_FILE = 'progress.txt'      # 断点续传进度文件（记录已抓完的页码）
ERROR_LOG = 'error_log.txt'         # 失败页码的日志文件
COOKIE_FILE = 'cookies.txt'         # 存放 Cookie 的文件（Cookie 过期后替换里面内容即可）

# 请求头：全部来自你提供的 cURL（保持和浏览器一致，服务器才不容易拦截）
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://marketplace.emag.ro',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"iOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1',
    'x-requested-with': 'XMLHttpRequest',
}

# 来源页（referer）里的 page 要跟着请求一起变，用 {page} 当占位符
REFERER_TEMPLATE = ('https://marketplace.emag.ro/opportunities/list'
                    '?duplicated_documentation=2'
                    '&sort=%7B%22field%22:%22performance%22,%22direction%22:%22asc%22%7D'
                    '&performance=1&page={page}')

# 主接口字段候选键（内部映射用；按顺序挨个试，最终输出为中文业务字段）。
# 已按真实返回数据核实：part_number_key 就是 PNK码；图片/佣金通过 PNK 从另两个接口关联
MAIN_FIELD_KEYS = {
    'title': ['product_name'],
    'brand': ['brand_name'],
    'pnk': ['part_number_key', 'partNumberKey'],
    'best_price': ['best_price'],
    'reviews': ['reviews'],
    'rating': ['rating'],
    'is_high_risk_category': ['is_high_risk_category'],
    'recycle_warranty_label': ['recycle_warranty_label'],
    'allowed_to_add_offer_in_category': ['allowed_to_add_offer_in_category'],
    # estimate 佣金接口需要（真实接口核实：brandId 取 brand_doc_id，categoryId 取 category_scm_id）
    'brand_id': ['brand_doc_id', 'brand_id', 'brandId'],
    'category_id': ['category_scm_id', 'category_id', 'categoryId'],
    'vendor_id': ['vendor_id', 'vendorId'],
}

# V1.0.2 最终输出字段（顺序固定，每条 JSON 记录只允许这些键，全部为中文）
OUTPUT_FIELDS = [
    '标题', '品牌', '一级类', '二级类', '三级类', '四级类', '五级类',
    '产品类型', 'PNK码', '最低价', '图片', '颜色', '评论数量', '商品评分',
    '完整类目', '是否属于高风险类目', '是否二手',
    '当前账号是否允许在该类目添加 Offer', '页数', '佣金', '每页数量',
]

# 图片/佣金响应里的候选键（基于真实响应：images 用 pnk/imageURL；estimate 响应按复合键回显）
IMAGE_PNK_KEYS = ['pnk', 'part_number_key', 'partNumberKey', 'PNK', 'product_pnk']
IMAGE_URL_KEYS = ['imageURL', 'image_url', 'imageUrl', 'image', 'url', 'main_image']
ESTIMATE_VALUE_KEYS = ['value', 'commission', 'commission_value', 'percentage']


def sanitize_concurrency():
    """保护 CONCURRENCY 配置：非正整数时自动按 1（单线程）处理并打印提示，不让程序崩溃"""
    global CONCURRENCY
    if not isinstance(CONCURRENCY, int) or CONCURRENCY < 1:
        print(f'[提示] CONCURRENCY={CONCURRENCY!r} 无效（必须为 >= 1 的整数），已自动按 1 处理（单线程模式）。')
        CONCURRENCY = 1


sanitize_concurrency()


# ==================== 二、Cookie 加载 ====================

def load_cookie():
    """从 cookies.txt 读取 Cookie 字符串（Cookie 过期后，替换文件内容即可）"""
    if not os.path.exists(COOKIE_FILE):
        print(f'[错误] 找不到 {COOKIE_FILE} 文件！')
        print('请把浏览器里重新复制的 Cookie 字符串保存到该文件（一行），再运行脚本。')
        return None
    with open(COOKIE_FILE, encoding='utf-8') as f:
        cookie = f.read().strip()
    if not cookie:
        print(f'[错误] {COOKIE_FILE} 是空的！请粘贴 Cookie 后重试。')
        return None
    return cookie


# ==================== 三、请求头与请求体构造 ====================

def make_payload(page):
    """构造 POST 请求体，动态替换 page 参数（其余字段和 cURL 里保持一致）"""
    return {
        'per_page': PER_PAGE,
        'duplicated_documentation': 2,
        'sort': [{'field': 'performance', 'direction': 'asc'}],
        'performance': [1],
        'page': page,
    }


def make_headers(page, cookie):
    """构造请求头：基础头 + 动态 referer + Cookie"""
    headers = dict(HEADERS)
    headers['referer'] = REFERER_TEMPLATE.format(page=page)
    headers['cookie'] = cookie
    return headers


# ==================== 四、JSON 解析（通用逻辑） ====================

def safe_json(resp):
    """把 Scrapling 的响应对象转成 Python 字典（兼容不同版本的 Scrapling）"""
    # 方式 1：Scrapling 自带 JSON 解析（0.4.13 以上 json 是方法 json()，旧版是属性 json）
    try:
        json_attr = getattr(resp, 'json', None)
        data = json_attr() if callable(json_attr) else json_attr
        if data:
            return data
    except Exception:
        pass
    # 方式 2：用底层原始响应对象解析
    raw = getattr(resp, 'response', None)
    if raw is not None:
        return raw.json()
    # 方式 3：手动解析响应体
    return json.loads(resp.body)


# 接口返回的 JSON 里，商品列表最可能放在这些键下面（按顺序找，找到就停）
PRIORITY_KEYS = ['products', 'items', 'results', 'opportunities', 'data',
                 'list', 'records', 'rows', 'offers']


def find_items(data):
    """在整段 JSON 里找到『商品列表』（一列字典）。找不到返回 None，找到空列表返回 []"""
    # 第一步：按常见键名找（更准确）
    if isinstance(data, dict):
        for key in PRIORITY_KEYS:
            if key in data and isinstance(data[key], list):
                return data[key]
    # 第二步：兜底递归，找第一个"列表且元素是字典"的列表
    return _find_first_list(data)


def _find_first_list(obj):
    if isinstance(obj, list):
        return obj if obj and isinstance(obj[0], dict) else None
    if isinstance(obj, dict):
        for value in obj.values():
            result = _find_first_list(value)
            if result is not None:
                return result
    return None


def search_key(obj, target):
    """在 JSON 里递归查找某个键的值（只认数字）"""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == target and isinstance(value, (int, float)):
                return value
            result = search_key(value, target)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj[:3]:  # 列表只检查前 3 个元素，避免拖慢速度
            result = search_key(item, target)
            if result is not None:
                return result
    return None


def detect_total_pages(data):
    """尝试从返回数据里读出总页数；读不到返回 None（此时靠 MAX_PAGE 兜底）"""
    # 先找"总页数"字段
    for key in ['total_pages', 'totalPages', 'page_count', 'pageCount',
                'pages', 'last_page', 'lastPage']:
        value = search_key(data, key)
        if isinstance(value, int) and value > 0:
            return value
    # 再找"总条数"字段，用它除以每页条数算出总页数
    for key in ['total', 'total_count', 'totalCount']:
        value = search_key(data, key)
        if isinstance(value, int) and value > 0:
            return math.ceil(value / PER_PAGE)
    return None


def get_value(item, keys):
    """按候选键名列表，从一条商品数据里取字段值"""
    for key in keys:
        if not isinstance(item, dict) or key not in item:
            continue
        value = item[key]
        if value is None:
            continue
        if isinstance(value, list):          # 值是个列表（比如多张图），取第一个
            value = value[0] if value else ''
        if isinstance(value, dict):          # 值是个对象（比如图片 {url: ...}）
            for sub in ['url', 'src', 'link', 'value']:
                if sub in value and isinstance(value[sub], str):
                    return value[sub]
            continue
        if isinstance(value, bool):          # bool 是 int 子类，必须先于数字判断
            return 'true' if value else 'false'
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return ''


def get_raw(item, keys):
    """按候选键名列表取原始值（保留数字/布尔类型，供 estimate 请求构造使用）"""
    if not isinstance(item, dict):
        return None
    for key in keys:
        if key in item and item[key] not in (None, ''):
            return item[key]
    return None


def normalize_scalar(value):
    """把任意标量统一为输出字符串：None→''、布尔→'true'/'false'、数字→去多余小数点的字符串"""
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    return ''


def normalize_commission(value):
    """把 estimate 返回的佣金值统一为百分比字符串：23/23.0/'23'/'23%'→'23%'，None→''
    （真实接口返回 '23.00' 这样的百分比字符串，不是 0~1 小数，不乘不除）"""
    if value is None or isinstance(value, bool):
        return ''
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value.is_integer():
            return f'{int(value)}%'
        return f'{value}%'
    if isinstance(value, str):
        text = value.strip().rstrip('%').strip()
        if not text:
            return ''
        try:
            num = float(text)
            return f'{int(num)}%' if num.is_integer() else f'{num}%'
        except ValueError:
            return f'{text}%'
    return ''


def get_category_path(item):
    """取完整类目：字符串原样返回；列表则把各级 name 用 ' > ' 连接"""
    raw = get_raw(item, ['category_path'])
    if raw is None:
        return ''
    if isinstance(raw, str):
        return raw.strip()
    if isinstance(raw, list):
        parts = []
        for part in raw:
            if isinstance(part, str) and part.strip():
                parts.append(part.strip())
            elif isinstance(part, dict):
                for k in ('name', 'title', 'label', 'value'):
                    if isinstance(part.get(k), str) and part[k].strip():
                        parts.append(part[k].strip())
                        break
        return ' > '.join(parts)
    return ''


def split_category_path(path):
    """把类目路径按 '>' 拆成最多五级，不足补空字符串"""
    parts = [x.strip() for x in path.split('>') if x.strip()] if path else []
    return (parts + [''] * 5)[:5]


# 属性类字段（Tip produs / Culoare 等）可能所在的容器键，以及属性条目内部的名称/值键
ATTR_CONTAINER_KEYS = ['attributes', 'features', 'characteristics', 'properties', 'specs']
ATTR_NAME_KEYS = ['name', 'label', 'key', 'characteristic_name', 'attribute_name']
ATTR_VALUE_KEYS = ['value', 'values', 'val', 'attribute_value']


def extract_attribute(item, target):
    """按属性名精确匹配取属性值（只在已知属性容器内查找，不做无边界递归）。
    找不到返回 ''"""
    if not isinstance(item, dict):
        return ''
    for container_key in ATTR_CONTAINER_KEYS:
        container = item.get(container_key)
        found = _attr_from_container(container, target)
        if found:
            return found
    direct = item.get(target)
    if direct not in (None, '', [], {}):
        return direct
    return ''


def _attr_from_container(container, target):
    if isinstance(container, dict):
        name = _first_value(container, ATTR_NAME_KEYS)
        if isinstance(name, str) and name.strip() == target:
            value = _first_value(container, ATTR_VALUE_KEYS)
            return value if value is not None else ''
        if target in container and container[target] not in (None, '', [], {}):
            return container[target]
    elif isinstance(container, list):
        for entry in container:
            if isinstance(entry, dict):
                name = _first_value(entry, ATTR_NAME_KEYS)
                if isinstance(name, str) and name.strip() == target:
                    value = _first_value(entry, ATTR_VALUE_KEYS)
                    return value if value is not None else ''
    return ''


def _first_value(obj, keys):
    """从字典里按候选键取第一个有效值（列表取首元素，对象取 url/src/link/value）"""
    if not isinstance(obj, dict):
        return None
    for key in keys:
        if key not in obj:
            continue
        value = obj[key]
        if isinstance(value, list):
            value = value[0] if value else ''
        elif isinstance(value, dict):
            value = _first_value(value, ['url', 'src', 'link', 'value'])
        if value not in (None, '', [], {}):
            return value
    return None


def extract_row(item, page, image_map=None, commission_map=None):
    """把一条商品数据整理成一条 V1.0.2 中文 JSON 记录。
    图片/佣金通过 PNK码 从 map 中关联（map 由本页批量接口请求构建）"""
    image_map = image_map or {}
    commission_map = commission_map or {}
    pnk = get_value(item, MAIN_FIELD_KEYS['pnk'])
    full_path = get_category_path(item)
    cats = split_category_path(full_path)
    row = {
        '标题': normalize_scalar(get_value(item, MAIN_FIELD_KEYS['title'])),
        '品牌': normalize_scalar(get_value(item, MAIN_FIELD_KEYS['brand'])),
        '一级类': cats[0],
        '二级类': cats[1],
        '三级类': cats[2],
        '四级类': cats[3],
        '五级类': cats[4],
        '产品类型': normalize_scalar(extract_attribute(item, 'Tip produs')),
        'PNK码': pnk,
        '最低价': normalize_scalar(get_value(item, MAIN_FIELD_KEYS['best_price'])),
        '图片': image_map.get(pnk, ''),
        '颜色': normalize_scalar(extract_attribute(item, 'Culoare')),
        '评论数量': normalize_scalar(get_value(item, MAIN_FIELD_KEYS['reviews'])),
        '商品评分': normalize_scalar(get_value(item, MAIN_FIELD_KEYS['rating'])),
        '完整类目': full_path,
        '是否属于高风险类目': normalize_scalar(get_value(item, MAIN_FIELD_KEYS['is_high_risk_category'])),
        '是否二手': normalize_scalar(get_value(item, MAIN_FIELD_KEYS['recycle_warranty_label'])),
        '当前账号是否允许在该类目添加 Offer': normalize_scalar(get_value(item, MAIN_FIELD_KEYS['allowed_to_add_offer_in_category'])),
        '页数': str(page),
        '佣金': normalize_commission(commission_map.get(pnk)) if pnk else '',
        '每页数量': str(PER_PAGE),
    }
    return row


# ==================== 五、保存与断点续传 ====================

def build_json_filename():
    """生成本次运行的输出文件名：{YYYYMMDD_HHMMSS}_{PER_PAGE}_{MAX_PAGE}.json
    只在任务开始时调用一次，整个任务期间保持不变（不会每保存一页重新生成）。
    用 O_CREAT | O_EXCL 原子占位：同一秒多个任务竞争同一文件名时只有一个成功，
    失败的等到下一秒再试，绝不覆盖或删除已有文件"""
    while True:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{timestamp}_{PER_PAGE}_{MAX_PAGE}.json'
        try:
            # 检查 + 创建作为一个原子操作完成，杜绝"先检查后使用"的竞争窗口
            fd = os.open(filename, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            print(f'[提示] 输出文件 {filename} 已被占用，等待下一秒生成新文件名（不会覆盖已有文件）...')
            time.sleep(1.1)   # 略超 1 秒，确保醒来时已进入下一秒
            continue
        # 占位成功：立即初始化为合法 JSON []，并强制落盘
        failed = False
        try:
            os.write(fd, b'[]')
            os.fsync(fd)
        except BaseException:
            failed = True
            raise
        finally:
            os.close(fd)
            if failed:
                try:
                    os.remove(filename)   # 初始化失败不留下坏的占位文件（先关句柄，Windows 才能删）
                except OSError:
                    pass
        return filename


def find_latest_json():
    """断点续传用：在当前目录找最近一次与本配置（PER_PAGE + MAX_PAGE）匹配的抓取结果文件。
    只认 {YYYYMMDD_HHMMSS}_{PER_PAGE}_{MAX_PAGE}.json 格式的文件，
    文件名里的时间戳格式固定，字符串排序即时间顺序，取最大（最新）的一个；找不到返回 None"""
    pattern = f'*_{PER_PAGE}_{MAX_PAGE}.json'
    candidates = [name for name in glob.glob(pattern)
                  if re.fullmatch(r'\d{8}_\d{6}_\d+_\d+\.json', name)]
    return max(candidates) if candidates else None


def save_page(rows):
    """把一页的数据立即追加写入 JSON 文件（标准 JSON 数组格式，不会内存溢出）。
    原地追加；写入过程中发生异常（含 Ctrl+C）时，把文件回滚到写入前的合法状态，
    再把原异常继续向外抛出（绝不吞掉 KeyboardInterrupt）"""
    size_before = os.path.getsize(JSON_FILE) if os.path.exists(JSON_FILE) else 0
    rollback_size = None      # 写入前的文件大小；None 表示写入还没开始，无需回滚
    try:
        if size_before <= 2:
            # 新文件/空文件：先建立合法的基础状态 []，再往里追加
            with open(JSON_FILE, 'w', encoding='utf-8') as f:
                f.write('[]')
                f.flush()
                os.fsync(f.fileno())
            rollback_size = 2
        else:
            rollback_size = size_before

        with open(JSON_FILE, 'r+', encoding='utf-8') as f:
            f.seek(rollback_size - 1)   # 覆盖掉结尾的 ]
            for i, row in enumerate(rows):
                # 已有数据时第一条也要逗号（覆盖掉了 ]）；空文件的第一条不加逗号
                if i > 0 or rollback_size > 2:
                    f.write(',')
                f.write(json.dumps(row, ensure_ascii=False))
            f.write(']')                # 补回结尾的 ]
            f.flush()
            os.fsync(f.fileno())        # 强制写盘，防止意外断电丢数据
    except BaseException:
        # 局部回滚：截断回写入前的大小，补回结尾的 ]，恢复合法 JSON 后重新抛出原异常
        try:
            if rollback_size is not None and os.path.exists(JSON_FILE):
                with open(JSON_FILE, 'r+', encoding='utf-8') as f:
                    f.truncate(rollback_size)
                    f.seek(rollback_size - 1)
                    f.write(']')
                    f.flush()
                    os.fsync(f.fileno())
        except Exception:
            pass   # 回滚本身失败不掩盖原异常
        raise


def save_progress(page):
    """把已完成的页码写入进度文件（断点续传靠它）"""
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        f.write(str(page))


def reset_data_files():
    """从头开始抓（RESUME = False）时清理本次运行的状态文件。
    注意：只清理进度文件和错误日志；JSON_FILE 是刚原子占位成功的新文件，
    绝不能删除（删掉后另一个进程又能取得同一个名字，原子保护就失效了）；
    历史 JSON 文件名带时间戳，也一律不删除"""
    for file in (PROGRESS_FILE, ERROR_LOG):
        if os.path.exists(file):
            os.remove(file)


def get_start_page():
    """计算从第几页开始：优先看进度文件，其次看 JSON 文件最后一条记录的 _page"""
    if not RESUME:
        return 1
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, encoding='utf-8') as f:
                return int(f.read().strip()) + 1
        except Exception:
            pass
    if os.path.exists(JSON_FILE) and os.path.getsize(JSON_FILE) > 2:
        last_page = _last_page_from_json()
        if last_page:
            return last_page
    return 1


def _last_page_from_json():
    """从 JSON 文件末尾读出最后一条记录的页码（只读末尾 16KB，速度快）"""
    try:
        size = os.path.getsize(JSON_FILE)
        with open(JSON_FILE, encoding='utf-8') as f:
            f.seek(max(0, size - 16384))
            tail = f.read().strip()
        tail = tail.rstrip(']').rstrip()   # 去掉结尾的 ] 和空白
        idx = tail.rfind('{')              # 最后一条记录的开头
        if idx < 0:
            return None
        row = json.loads(tail[idx:])
        # V1.0.2 输出用"页数"字段；兼容旧版 "_page"
        page_value = row.get('页数', row.get('_page', 0))
        return int(page_value) + 1
    except Exception:
        return None


def record_error(page, message):
    """把失败信息追加写入错误日志"""
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 第 {page} 页 | {message}\n"
    with open(ERROR_LOG, 'a', encoding='utf-8') as f:
        f.write(line)


# ==================== 六、单页请求（工作线程内执行，含重试，绝不中断程序） ====================

# 连续收到 403 的次数（403 通常是 Cookie 过期）。
# 注意：并发模式下此变量只由主线程读写，工作线程通过返回值上报状态码，避免多线程竞争
consecutive_403 = 0


def looks_like_login_page(resp):
    """判断响应内容是不是登录页（登录失效时服务器会返回登录页的 HTML）"""
    try:
        body = resp.body
        if isinstance(body, bytes):
            body = body.decode('utf-8', errors='ignore')
        low = body.lower()
        return ('login' in low or 'auth.emag' in low) and ('<!doctype' in low or '<html' in low)
    except Exception:
        return False


def post_api(url, headers, payload, page, label='接口'):
    """通用 POST（工作线程内执行）：重试、跳转/登录页检测、最终 403 标记。
    返回 dict：
      result: 'ok'=成功 / 'login_expired'=登录失效 / 'failed'=失败
      data / error / status: 解析结果、错误描述、最后一次状态码
      had_403: 最终（重试后）是否仍为 403"""
    last_status = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = Fetcher.post(
                url,
                headers=headers,
                json=payload,
                timeout=30,
                impersonate='chrome',       # 模拟 Chrome 浏览器的 TLS 指纹，防止被服务器拦截
                follow_redirects=False,     # 不自动跟随跳转：正常接口不会跳转，一旦跳转说明登录失效
            )
            status = resp.status
            last_status = status

            # 接口跳转（302 等）= 登录失效，重试也没用，直接报告
            if status in (301, 302, 303, 307, 308):
                return {'result': 'login_expired', 'data': None,
                        'error': f'{label}登录失效（接口跳转到登录页）', 'status': status,
                        'had_403': False}

            if status == 200:
                try:
                    return {'result': 'ok', 'data': safe_json(resp),
                            'error': None, 'status': status, 'had_403': False}
                except Exception:
                    # 状态码 200 但解析不出 JSON：再确认是不是返回了登录页
                    if looks_like_login_page(resp):
                        return {'result': 'login_expired', 'data': None,
                                'error': f'{label}登录失效（返回登录页）', 'status': status,
                                'had_403': False}

            if attempt < MAX_RETRIES:
                print(f'[重试] 第 {page} 页{label}返回状态码 {status}，第 {attempt}/{MAX_RETRIES} 次重试...')
                time.sleep(random.uniform(3, 6))
        except Exception as exc:
            if attempt < MAX_RETRIES:
                print(f'[重试] 第 {page} 页{label}网络异常：{exc}，第 {attempt}/{MAX_RETRIES} 次重试...')
                time.sleep(random.uniform(3, 6))
            else:
                return {'result': 'failed', 'data': None,
                        'error': f'{label}异常：{exc}', 'status': last_status,
                        'had_403': last_status == 403}
    return {'result': 'failed', 'data': None,
            'error': f'{label}状态码 {last_status}，重试 {MAX_RETRIES} 次仍失败',
            'status': last_status, 'had_403': last_status == 403}


def make_api_headers(cookie, referer):
    """构造辅助接口（images/estimate）请求头：复用基础头 + 固定 referer + Cookie"""
    headers = dict(HEADERS)
    headers['referer'] = referer
    headers['cookie'] = cookie
    return headers


def build_images_payload(pnks):
    """构造 images 批量请求体（products = 本页全部有效 PNK，resolution 固定 150x150）"""
    return {'products': list(pnks), 'resolution': '150x150'}


def build_estimate_item(item):
    """从主接口商品数据构造 estimate 请求项；缺必填字段返回 None（跳过该商品）。
    真实接口核实：brandId 取 brand_doc_id、categoryId 取 category_scm_id、
    price 取 best_price；vendorId 主接口不提供，服务器按登录态自动识别，无需发送"""
    pnk = get_value(item, MAIN_FIELD_KEYS['pnk'])
    brand_id = get_raw(item, MAIN_FIELD_KEYS['brand_id'])
    category_id = get_raw(item, MAIN_FIELD_KEYS['category_id'])
    price = get_raw(item, MAIN_FIELD_KEYS['best_price'])
    if not pnk or category_id is None or price is None:
        return None
    entry = {
        'partNumberKey': pnk,
        'brandId': brand_id,
        'categoryId': category_id,
        'price': price,
    }
    return entry


def build_estimate_payload(estimate_items):
    """构造 estimate 批量请求体。注意：products 的值是 JSON 序列化后的字符串，
    不是普通数组（已按真实接口格式核实）"""
    return {'products': json.dumps(estimate_items, ensure_ascii=False)}


def parse_image_map(data):
    """解析 images 响应，构建 {PNK: 图片URL}。真实响应结构：
    {'isError': ..., 'messages': ..., 'results': [{'pnk': ..., 'imageURL': ...}]}
    每个 PNK 只取第一条有效 URL"""
    image_map = {}
    entries = []
    if isinstance(data, dict):
        for key in ('results', 'products', 'items', 'data', 'images'):
            if isinstance(data.get(key), list):
                entries = data[key]
                break
        if not entries:
            entries = list(data.values())
    elif isinstance(data, list):
        entries = data
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        pnk = _first_value(entry, IMAGE_PNK_KEYS)
        url = _first_value(entry, IMAGE_URL_KEYS)
        if isinstance(pnk, str) and pnk and isinstance(url, str) and url:
            image_map.setdefault(pnk, url)   # 同 PNK 多条时保留第一条
    return image_map


def parse_commission_map(data):
    """解析 estimate 响应，构建 {PNK: 原始value}（value 在写 JSON 时再格式化为百分比）。
    真实响应结构：{'242169,DQSQ7MBBM,32773,2280': {'value': '23.00', ...}}
    键格式为 {vendorId},{pnk},{brandId},{categoryId}，逗号拆分后第 2 段是 PNK"""
    commission_map = {}
    if isinstance(data, dict):
        for key, value in data.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                continue
            parts = key.split(',')
            pnk = parts[1] if len(parts) > 1 else key
            if pnk:
                commission_map[pnk] = _first_value(value, ESTIMATE_VALUE_KEYS)
    return commission_map


def fetch_page(page, cookie):
    """请求某一页（在工作线程里执行，不写任何共享状态、不写任何文件）。
    流程：主接口 → images 批量接口 → estimate 批量接口（顺序执行，每页约 3 个请求）。
    返回 dict：
      result: 'ok'=成功 / 'login_expired'=登录失效 / 'failed'=失败
      data:   主接口响应 JSON（仅 result='ok' 时有值）
      error:  失败原因描述（由主线程统一写入 error_log.txt）
      status: 主接口最后一次响应的状态码
      had_403: 本页三个请求中是否有任一请求最终仍为 403（页面级 final 403 标记）
      image_map / commission_map: {PNK: 值} 关联表
      errors: [(页码, 描述)] 辅助接口失败/字段缺失记录（由主线程统一写 error_log）"""
    errors = []
    result = post_api(API_URL, make_headers(page, cookie), make_payload(page), page, label='主接口')
    if result['result'] == 'login_expired':
        return {'result': 'login_expired', 'data': None, 'error': result['error'],
                'status': result['status'], 'had_403': result['had_403'],
                'image_map': {}, 'commission_map': {}, 'errors': []}
    if result['result'] != 'ok':
        return {'result': 'failed', 'data': None, 'error': result['error'],
                'status': result['status'], 'had_403': result['had_403'],
                'image_map': {}, 'commission_map': {}, 'errors': []}
    data = result['data']
    had_403 = result['had_403']

    image_map = {}
    commission_map = {}
    items = find_items(data)
    if items is not None:
        pnks = []
        estimate_items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            pnk = get_value(item, MAIN_FIELD_KEYS['pnk'])
            if not pnk:
                errors.append((page, '商品缺少 part_number_key（PNK码），图片/佣金将留空'))
                continue
            pnks.append(pnk)
            estimate_entry = build_estimate_item(item)
            if estimate_entry is None:
                errors.append((page, f'PNK {pnk} 缺少 estimate 必填字段（PNK码/类目ID/价格），跳过佣金查询'))
            else:
                estimate_items.append(estimate_entry)

        # images：补充接口。失败只留空图片，不丢整页商品
        if pnks:
            aux = post_api(IMAGES_API_URL, make_api_headers(cookie, 'https://marketplace.emag.ro/opportunities/list'),
                           build_images_payload(pnks), page, label='图片接口')
            had_403 = had_403 or aux['had_403']
            if aux['result'] == 'ok':
                image_map = parse_image_map(aux['data'])
            elif aux['result'] == 'login_expired':
                return {'result': 'login_expired', 'data': data, 'error': aux['error'],
                        'status': aux['status'], 'had_403': had_403,
                        'image_map': {}, 'commission_map': {}, 'errors': errors}
            else:
                errors.append((page, f'图片接口失败：{aux["error"]}（本页商品图片留空）'))

        # estimate：补充接口。失败只留空佣金，不丢整页商品
        if estimate_items:
            aux = post_api(ESTIMATE_API_URL, make_api_headers(cookie, 'https://marketplace.emag.ro/opportunities/list'),
                           build_estimate_payload(estimate_items), page, label='佣金接口')
            had_403 = had_403 or aux['had_403']
            if aux['result'] == 'ok':
                commission_map = parse_commission_map(aux['data'])
            elif aux['result'] == 'login_expired':
                return {'result': 'login_expired', 'data': data, 'error': aux['error'],
                        'status': aux['status'], 'had_403': had_403,
                        'image_map': {}, 'commission_map': {}, 'errors': errors}
            else:
                errors.append((page, f'佣金接口失败：{aux["error"]}（本页商品佣金留空）'))

    return {'result': 'ok', 'data': data, 'error': None, 'status': result['status'],
            'had_403': had_403, 'image_map': image_map,
            'commission_map': commission_map, 'errors': errors}


def fetch_batch(pages, cookie, executor):
    """并发抓取一批页面：一次只把本批（数量 = CONCURRENCY）提交到线程池，
    等待全部完成后返回 {页码: 结果}。绝不一次性提交全部任务"""
    futures = {executor.submit(fetch_page, page, cookie): page for page in pages}
    results = {}
    for future in as_completed(futures):
        page = futures[future]
        results[page] = future.result()   # fetch_page 内部已兜底，不会抛异常
        print(f'[请求完成] 第 {page} 页')
    return results


# ==================== 七、主程序 ====================

def main():
    global JSON_FILE, consecutive_403

    # ---- 第一步：确定本次输出文件（时间戳命名，任务期间保持不变）----
    if RESUME:
        latest = find_latest_json()
        if latest:
            JSON_FILE = latest
            print(f'断点续传：找到最近一次抓取文件 {JSON_FILE}')
        else:
            JSON_FILE = build_json_filename()
            print(f'断点续传：未找到与当前配置（PER_PAGE={PER_PAGE}，MAX_PAGE={MAX_PAGE}）匹配的历史文件，'
                  f'将新建文件从头开始')
            if os.path.exists(PROGRESS_FILE):
                os.remove(PROGRESS_FILE)   # 旧进度是其他配置留下的，清理掉避免错位续传
    else:
        JSON_FILE = build_json_filename()
        reset_data_files()
        print('已清理本次运行的进度状态，从第 1 页开始抓取。（历史时间戳 JSON 文件不会被删除）')

    # ---- 第二步：启动信息 ----
    print('=' * 60)
    print(f'eMAG 商品数据抓取脚本 V{VERSION}')
    print(f'每页请求：{PER_PAGE} 条')
    print(f'最大抓取页数：{MAX_PAGE}')
    print(f'并发数量：{CONCURRENCY}')
    print(f'批次间隔：{SLEEP_MIN}~{SLEEP_MAX} 秒')
    print(f'断点续传：{"开（从上次进度继续）" if RESUME else "关（从头开始抓）"}')
    print(f'输出文件：{JSON_FILE}')
    print('=' * 60)

    cookie = load_cookie()
    if cookie is None:
        return

    start_page = get_start_page()
    if start_page > 1:
        print(f'检测到上次进度：从第 {start_page} 页继续（断点续传）')
    else:
        print('从头开始抓取（第 1 页）')

    page = start_page
    total_saved = 0
    empty_streak = 0        # 连续找不到商品列表的次数
    total_pages = None      # 接口返回的总页数（读到后不再创建超出它的批次）
    start_time = time.time()
    finished = False
    login_expired = False
    blocked_403 = False     # 连续多个页面最终返回 403，达到阈值后停止

    try:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            while page <= MAX_PAGE:
                # 已知总页数后，不再创建超出总页数的新批次
                if total_pages and page > total_pages:
                    print(f'已达到接口返回的总页数（{total_pages} 页），抓取完成！')
                    finished = True
                    break

                batch_pages = list(range(page, min(page + CONCURRENCY, MAX_PAGE + 1)))
                results = fetch_batch(batch_pages, cookie, executor)

                # 主线程按页码顺序统一处理：统计 403、写 JSON、更新进度、写错误日志
                for current_page in sorted(results):
                    res = results[current_page]

                    # 403 连续计数只在主线程更新，避免多线程竞争。
                    # 页面级 final 403：本页三个接口（主/图片/佣金）中任一请求重试后最终仍为 403，
                    # 本页就记一次；本页全部请求都没有最终 403 则清零。统计单位是"页"，
                    # 同一页内 images 403 + estimate 403 不会算成两次
                    if res.get('had_403'):
                        consecutive_403 += 1
                        if consecutive_403 >= 3:
                            print('!!! 连续多个页面最终返回 403，Cookie 或 WAF Token 很可能已失效。')
                            print('!!! 程序将停止，不再创建新的请求批次。')
                            print('!!! 请更新 cookies.txt 后重新运行。')
                            record_error(current_page, '连续多个页面最终返回 403，程序已停止')
                            blocked_403 = True
                            break
                    else:
                        consecutive_403 = 0

                    # 登录失效：不再处理本批剩余页，也不再创建新批次
                    if res['result'] == 'login_expired':
                        print('!!! 接口返回跳转/登录页，说明登录已失效（Cookie 过期）。')
                        print('!!! 请重新复制 Cookie 覆盖 cookies.txt，然后重启脚本（已抓数据不会丢）。')
                        record_error(current_page, res['error'])
                        login_expired = True
                        break

                    if res['result'] != 'ok':
                        print(f'[跳过] 第 {current_page} 页已记录到 error_log.txt，继续下一页')
                        record_error(current_page, res['error'])
                        continue

                    # 超出总页数的页面：请求虽已发出，但数据不写入结果
                    if total_pages and current_page > total_pages:
                        continue

                    # 辅助接口失败/字段缺失记录（工作线程只收集，主线程统一写日志）
                    for err_page, err_msg in res.get('errors', []):
                        record_error(err_page, err_msg)

                    data = res['data']
                    items = find_items(data)

                    # 情况 1：返回的数据里没找到商品列表（结构可能变了）
                    if items is None:
                        empty_streak += 1
                        record_error(current_page, '未在返回数据中找到商品列表')
                        if empty_streak >= 3:
                            print('!!! 连续多页没找到商品列表，接口返回结构可能和预期不同。')
                            print('!!! 请停止脚本，把任意一页的原始 JSON 发给我调整解析代码。')
                        continue
                    empty_streak = 0

                    # 情况 2：商品列表为空，说明已经抓完了（本批更高页码的数据不再写入）
                    if not items:
                        print(f'第 {current_page} 页返回的商品列表为空，数据已全部抓完，正常结束。')
                        finished = True
                        break

                    rows = [extract_row(item, current_page, res.get('image_map', {}), res.get('commission_map', {}))
                            for item in items if isinstance(item, dict)]
                    save_page(rows)              # 只有主线程会调用写文件函数
                    save_progress(current_page)  # 记录进度，方便断点续传
                    total_saved += len(rows)

                    elapsed_min = (time.time() - start_time) / 60
                    print(f'[保存成功] 第 {current_page} 页 | 本页 {len(rows)} 条'
                          f' | 累计 {total_saved} 条 | 用时 {elapsed_min:.1f} 分钟')

                    # 如果接口返回了总页数，达到后自动停止
                    detected = detect_total_pages(data)
                    if detected and total_pages is None:
                        total_pages = detected
                    if total_pages and current_page >= total_pages:
                        print(f'已达到接口返回的总页数（{total_pages} 页），抓取完成！')
                        finished = True
                        break

                if finished or login_expired or blocked_403:
                    break

                page = batch_pages[-1] + 1
                if page <= MAX_PAGE:
                    # 每批并发请求之间的随机暂停（不是每个 Worker 各自暂停）
                    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    except KeyboardInterrupt:
        print('\n已手动停止。已保存的数据仍在 JSON 文件中，重新运行脚本会从断点继续。')

    if blocked_403:
        print('由于连续收到 403，程序已停止。')
        print('Cookie 或 aws-waf-token 可能已经失效。')
        print('请更新 cookies.txt 后重新运行。')
    elif login_expired:
        print('由于登录失效，程序已停止。更新 Cookie 后重新运行即可继续抓取。')
    elif finished:
        print(f'全部完成！本次共抓取 {total_saved} 条数据，保存在 {JSON_FILE}')
    else:
        print(f'本次运行结束（共保存 {total_saved} 条）。重新运行脚本可继续抓取。')


if __name__ == '__main__':
    main()
