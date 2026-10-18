# -*- coding: utf-8 -*-
"""the unit2api：OpenAI 兼容反代 → OpenCode 免费层网关直连，只放行 free 模型。

架构：**双通道直连**，每通道一个自包含文件（门禁规格全在各自文件里，上游改
门禁只改对应文件，不动 server.py）：

  lane_zen.py       legacy /zen/v1 —— v1.3.7 形状，仅需一个 read 工具（优先）
  lane_inference.py /inference/v1 —— 2.x 指纹，bash+read 最少集（回落）

server.py 只留公共设施：session 指纹（两通道同源校验，共用缓存）、dumps、
模型清单、三协议路由、通道选择与冷却调度。

通道顺序用 `UPSTREAM_LANES` 配置（逗号分隔，默认 `zen,inference`）：
  UPSTREAM_LANES=zen,inference   双通道（默认，zen 优先）
  UPSTREAM_LANES=inference       只走 inference（旧行为）
  UPSTREAM_LANES=zen             只走 zen

风控响应分类：403 FreeTierError → 该通道冷却 600s 后跳过；429 限流 → 短冷却
60s 并原样透传；全部通道不可用 → 本地 503 快速失败。

只落请求/响应 body 原文，绝不落 header（Authorization 是明文凭证）。
"""
import json
import logging
import os
import random
import string
import threading
import time
import uuid
from logging.handlers import RotatingFileHandler

import requests
from dotenv import load_dotenv
from flask import Flask, Response, g, jsonify, request

import anthropic_bridge
import lane_inference
import lane_zen

load_dotenv()

BASE = os.path.dirname(os.path.abspath(__file__))
API_KEY = os.environ.get("API_KEY", "").strip()
LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = int(os.getenv("PORT", "8390"))

# ── 通道注册表（每通道一个自包含文件，门禁规格全在各自 lane 文件里）────────────
_LANE_MODS = {"zen": lane_zen, "inference": lane_inference}
# 通道顺序：UPSTREAM_LANES 逗号分隔（默认 zen 优先、inference 回落）。
_LANE_ORDER = [s.strip() for s in
               os.environ.get("UPSTREAM_LANES", "zen,inference").split(",")
               if s.strip() in _LANE_MODS] or ["zen", "inference"]
# 兼容别名：旧引用点（_fetch_free / 探针）仍可用
API_BASE = lane_inference.BASE
LEGACY_ZEN_BASE = lane_zen.BASE
_LANES = {name: mod.BASE for name, mod in _LANE_MODS.items()}
# 上游已废弃模型（实测 410 ModelDeprecated）：剔除，避免误调必失败
DEPRECATED = lane_inference.DEPRECATED

# ── 日志（与 cline2api / qoder2api 同一标准）──────────────────────────────────
LOG_FILE = os.path.join(BASE, "request.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)
logging.getLogger("werkzeug").disabled = True

# ── dumps（只落 body 原文）───────────────────────────────────────────────────
DUMP_DIR = os.environ.get("DUMP_DIR", "") or os.path.join(BASE, "dumps")
DUMP_ON = os.environ.get("DUMP", "1").strip() != "0"
DUMP_MAX_BYTES = 512 * 1024
DUMP_MAX_FILES = int(os.environ.get("DUMP_MAX_FILES", "200"))


def _dump_trim():
    if DUMP_MAX_FILES <= 0:
        return
    try:
        entries = sorted((e for e in os.scandir(DUMP_DIR) if e.is_file()),
                         key=lambda e: e.stat().st_mtime)
    except OSError:
        return
    for stale in entries[:len(entries) - DUMP_MAX_FILES]:
        try:
            os.remove(stale.path)
        except OSError:
            pass


# dump 文件名前缀启动段：rid 每进程从 r0001 重新计数，旧版同名 rid 跳重启后直接覆盖旧证据
_BOOT_TAG = time.strftime("%m%d-%H%M%S", time.localtime()) + "-p%d" % os.getpid()
_seq_lock = threading.Lock()
_req_seq = 0


def _next_req_id():
    global _req_seq
    with _seq_lock:
        _req_seq += 1
        return "r%04d" % _req_seq


def _dump_save(rid, kind, data):
    if not DUMP_ON or not data:
        return
    try:
        os.makedirs(DUMP_DIR, exist_ok=True)
        text = data if isinstance(data, str) else data.decode("utf-8", "replace")
        truncated = len(text) > DUMP_MAX_BYTES
        path = os.path.join(DUMP_DIR, "%s.%s.%s.txt%s" % (_BOOT_TAG, rid, kind, ".truncated" if truncated else ""))
        with open(path, "w", encoding="utf-8") as f:
            f.write(text[:DUMP_MAX_BYTES])
        _dump_trim()
    except OSError as e:
        logger.warning("[%s] 落盘失败: %s", rid, e)


# 免费层是匿名面：V2 门禁只校验指纹，不认 key。2026-10-15 实测（/inference/v1，同一份
# 指纹与 body）：`Bearer public` 四次全 200，换成本地存的那把 sk- key 四次全 401
# `Upstream request failed: Invalid credential`。所以不再读凭证文件——
# `credentials.json` 留着备用，但不参与任何上游请求。要改用真 key 时设 UPSTREAM_AUTH
# （鉴权常量本身定义在 lane_*.py 的 AUTH，下方有兼容别名）。


# ── 闸门指纹 ──────────────────────────────────────────────────────────────────
_HEX = string.digits + "abcdef"
_B62 = string.digits + string.ascii_letters


def _ident(prefix: str) -> str:
    """官方 ID 形态：ses_ / req_ + 12位hex + 14位base62。"""
    return "%s_%s%s" % (prefix,
                        "".join(random.choice(_HEX) for _ in range(12)),
                        "".join(random.choice(_B62) for _ in range(14)))


# ── V2 session 指纹 ────────────────────────────────────────────────────────────
# 上游 2026-10-11 起校验 session id 的降序时间戳语义（2026-10-15 消融定案）：
# 前 12 位 hex 必须是 ~(ts_ms<<12 + counter) 的低 48 位（对齐上游
# packages/schema/src/identifier.ts 的 descending TypeID），纯随机/升序即 403，
# counter 有上界 63（64 即拒；下方 _new_oc_session 的每毫秒计数器实际远达不到），
# 时效容忍 ≥8 天。同一会话值又必须稳定（prompt 缓存亲和），所以用
# "种子→会话映射 + 落盘 + 3 天 TTL"，而不是逐请求生成。
_SESSION_TTL_MS = 3 * 24 * 3600 * 1000
_SESSION_MAX = 256
_SESSION_FILE = os.path.join(BASE, "session_cache.json")
_session_map = {}
_id_lock = threading.Lock()
_id_last_ms = 0
_id_counter = 0


def _new_oc_session() -> str:
    """ses_ + 26 位降序 TypeID 主体（前 12 hex 反转时间戳 + 14 base62）。"""
    global _id_last_ms, _id_counter
    with _id_lock:
        now_ms = int(time.time() * 1000)
        if now_ms != _id_last_ms:
            _id_last_ms, _id_counter = now_ms, 0
        _id_counter += 1
        counter = _id_counter
    value = (~(now_ms * 0x1000 + counter)) & 0xFFFFFFFFFFFF
    head = "".join(format((value >> (40 - 8 * i)) & 0xFF, "02x") for i in range(6))
    tail = "".join(_B62[b % 62] for b in os.urandom(14))
    return "ses_" + head + tail


def _load_session_map():
    try:
        with open(_SESSION_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _session_map.update(
                {k: v for k, v in data.items()
                 if isinstance(v, dict) and isinstance(v.get("ses"), str) and v.get("ts")})
    except Exception:
        pass


def _save_session_map():
    try:
        with open(_SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(_session_map, f, ensure_ascii=False)
    except Exception:
        pass


def _oc_session_for(seed: str) -> str:
    """会话种子 → 上游 session（稳定 + 降序编码 + 3 天 TTL 再生成）。

    命名避开模块级 _upstream_session（那是 requests.Session 连接池）。"""
    now_ms = int(time.time() * 1000)
    entry = _session_map.get(seed)
    if entry and now_ms - int(entry["ts"]) < _SESSION_TTL_MS:
        return entry["ses"]
    if len(_session_map) >= _SESSION_MAX:
        oldest = min(_session_map, key=lambda k: int(_session_map[k]["ts"]))
        _session_map.pop(oldest, None)
    _session_map[seed] = {"ses": _new_oc_session(), "ts": now_ms}
    _save_session_map()
    return _session_map[seed]["ses"]


# 过闸门必需：units 声明 bash + read，且必须 OpenAI 线格式
def _session_seed(p: dict) -> str:
    """会话种子 = 首条用户消息（同对话稳定，跨请求不变）。

    chat 线读 messages；Responses 线读 input（字符串或分段列表）。
    """
    for m in p.get("messages") or []:
        if m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, list):
                c = " ".join(x.get("text", "") for x in c if isinstance(x, dict))
            if c:
                return str(c)[:256]
    inp = p.get("input")
    if isinstance(inp, str) and inp:
        return inp[:256]
    if isinstance(inp, list):
        for x in inp:
            if isinstance(x, dict) and x.get("type") == "message":
                c = x.get("content")
                if isinstance(c, list):
                    c = " ".join(i.get("text", "") for i in c
                                 if isinstance(i, dict) and i.get("type") == "input_text")
                if c:
                    return str(c)[:256]
    return ""


def _stable_session(seed: str) -> str:
    """社区标准：同一会话必须用固定 session id（否则像滥用触发风控 403）。

    形态 ses_ + 12位hex + 14位base62；由 seed 确定性派生。
    """
    # V2 指纹：降序时间戳编码 + 种子稳定映射（见 _oc_session_for）
    return _oc_session_for(seed)


# 闸门工具规格已下沉到各 lane 文件（每通道自包含）：
#   lane_zen.GATE.../prepare_body   —— zen 通道仅需一个 read
#   lane_inference.GATE_TOOLS       —— bash+read 最少集 + glob/grep
# 此处仅保留兼容别名（探针/旧脚本引用），路由不再使用。
GATE_TOOLS = lane_inference.GATE_TOOLS
GATE_TOOLS_FLAT = lane_inference.GATE_TOOLS_FLAT
UPSTREAM_AUTH = lane_inference.AUTH


def _upstream_headers(seed: str = "") -> dict:
    """inference 通道指纹头（薄代理，规格见 lane_inference.py）。"""
    return lane_inference.build_headers(_stable_session(seed))


def _zen_headers(seed: str = "") -> dict:
    """zen 通道指纹头（薄代理，规格见 lane_zen.py）。"""
    return lane_zen.build_headers(_stable_session(seed))


# ── 上游转发：共享连接池 + 连接级单次重试 ────────────────────────────────────
# 2026-10-13 实测：chat 线 210 次失败全是 `SSLEOFError(UNEXPECTED_EOF_WHILE_READING)`，
# 而**同期 24 次纯 TLS 握手、以及串行/并发 GET+POST 全部 0 失败**。结论：
#   1) 不是证书/协议/体积问题（失败请求 body 中位 277KB，成功 334KB，同分布）；
#   2) 是上游在坏窗口里"接受 TCP+TLS、读完请求后不发响应直接 FIN"的偶发断连。
# 两个动作各自解决一部分：
#   A. 共享 Session（keep-alive 连接池）：原先每次 `requests.post` 新建 Session，
#      每请求都要重走 TCP+TLS 握手，坏窗口里握手次数≈失败次数。池化后复用已建立
#      的长连接，直接把暴露在坏窗口里的握手数量降下来。
#   B. 连接级失败重试 1 次：只对"拿不到响应头"的异常重试。SSE 字节一旦开始下发
#      就绝不重试（客户端已收到半截流，重发会重复输出且重复计费）。
#      另外日志里同 msgs 数的请求会在同秒重复出现，说明 opencode 客户端本就会对
#      我们的 502 重发 —— 服务端补这一次，总请求数反而比放任客户端重试更少。
_UPSTREAM_POOL = int(os.getenv("UPSTREAM_POOL", "32"))
_UPSTREAM_RETRY = int(os.getenv("UPSTREAM_RETRY", "1"))
# 并发闸（2026-10-18 增）：01:11 那波 10 连发 ~1MB body 后即被风控标记，
# 限流是防再犯的核心。默认 4，可用 UPSTREAM_CONCURRENCY 调。
_UPSTREAM_CONCURRENCY = int(os.getenv("UPSTREAM_CONCURRENCY", "4"))
_upstream_sema = threading.BoundedSemaphore(_UPSTREAM_CONCURRENCY)


def _new_upstream_session() -> requests.Session:
    s = requests.Session()
    # max_retries=0：重试由 _post_upstream 显式控制，避免 urllib3 静默重试放大请求量。
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=_UPSTREAM_POOL, pool_maxsize=_UPSTREAM_POOL,
        max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


_upstream_session = _new_upstream_session()


def _is_retryable_upstream_error(e: Exception) -> bool:
    """只在"尚未拿到任何响应"时重试；其余一律放过。

    分类依据：`ConnectTimeout` 本身是 `ConnectionError` 的子类，所以下面这一条
    已经覆盖了它；`ReadTimeout` 只属于 `Timeout`（不是 `ConnectionError`），
    落在最后返回 False —— 读超时说明请求很可能已被上游受理并可能已计费，
    重发就是二次扣额度，宁可不试。
    """
    return isinstance(e, requests.exceptions.ConnectionError)


class _ErrResp:
    """冷却期快速失败的本地合成响应：三个路由的 `status_code >= 400 → 原样透传`
    错误分支只消费 status_code/text/headers，不会走到 iter_*，故只实现这三个。"""

    def __init__(self, status, text):
        self.status_code = status
        self.text = text
        self.headers = {"Content-Type": "application/json"}


# ── 风控冷却：403 FreeTierError 后该通道静默 N 秒，不再撞上游 ─────────────────
# 2026-10-18 实测：1MB 级并发轰一轮后上游按指纹/IP 打滥用标记，三个模型全 403
# 秒拒，继续撞只会续期标记。按通道分别冷却（zen 被标记不影响 inference，反之亦
# 然）；两通道都在冷却才本地 503 快速失败，把重试节奏交还客户端。
_COOLDOWN_S = int(os.getenv("UPSTREAM_COOLDOWN", "600"))          # 403 指纹拒绝
_COOLDOWN_429_S = int(os.getenv("UPSTREAM_COOLDOWN_429", "60"))   # 429 限流
_gate = {"zen": 0.0, "inference": 0.0}
_gate_lock = threading.Lock()
_LANES = {"zen": LEGACY_ZEN_BASE, "inference": API_BASE}


def _lane_cooling(lane):
    """该通道剩余冷却秒数；0 = 未冷却。（返回剩余而非绝对时刻，调用方直接可读。）"""
    with _gate_lock:
        until = _gate[lane]
        return int(until - time.time()) + 1 if time.time() < until else 0


def _gate_mark(lane, rid, why, seconds=None):
    with _gate_lock:
        _gate[lane] = time.time() + (seconds or _COOLDOWN_S)
    logger.warning("[%s] 通道 %s 命中 %s，冷却 %ds 内不再请求",
                   rid, lane, why, seconds or _COOLDOWN_S)


def _post_lane(lane, path, payload, seed, rid, timeout, flat=False):
    """单通道一次发送（含连接级重试）；返回 Response 或抛异常。

    body 由该通道的 lane 模块自行 prepare（每通道自包含的门禁规格），
    headers 的 session 由 server 统一生成后传入（两通道同源校验）。
    """
    mod = _LANE_MODS[lane]
    body = mod.prepare_body(payload, flat=flat)
    headers = mod.build_headers(_stable_session(seed))
    last = None
    for attempt in range(_UPSTREAM_RETRY + 1):
        try:
            with _upstream_sema:
                return _upstream_session.post(
                    _LANES[lane] + path, json=body,
                    headers=headers, stream=True, timeout=timeout)
        except requests.RequestException as e:
            last = e
            if attempt >= _UPSTREAM_RETRY or not _is_retryable_upstream_error(e):
                raise
            # 抖动一下再试：坏窗口通常成串，立刻重发很可能撞同一个坑。
            # 断开的那条连接已从池里剔除，重试自然用新连接。
            back = 0.4 * (attempt + 1) + random.uniform(0, 0.3)
            logger.warning("[%s] 上游连接被切断，%.1fs 后重试 %d/%d: %s: %s",
                           rid, back, attempt + 1, _UPSTREAM_RETRY,
                           type(e).__name__, str(e)[:120])
            time.sleep(back)
    raise last  # pragma: no cover —— 循环内必定 return 或 raise


def _post_upstream(path: str, payload: dict, seed: str, rid: str,
                   timeout: int = 180, flat=False):
    """多通道直连：按 UPSTREAM_LANES 顺序逐通道尝试（默认 zen 优先、inference
    回落；单通道配置即旧行为）。

    返回 (Response, lane)。每通道的 body 准备由各自 lane 模块完成（自包含
    门禁规格），server 不掺和通道细节。403 FreeTierError 触发该通道冷却并
    继续下一通道；所有通道都不可用（都在冷却 / 都 403 / 都断连）时分别以
    503 合成响应、最后一个真实 403、或异常交付给调用方的既有错误分支。
    """
    resp403 = None
    resp403_lane = ""
    resp404 = None
    resp404_lane = ""
    net_err = None
    for lane in _LANE_ORDER:
        left = _lane_cooling(lane)
        if left:
            logger.info("[%s] 通道 %s 冷却中（剩 %ds），跳过", rid, lane, left)
            continue
        try:
            r = _post_lane(lane, path, payload, seed, rid, timeout, flat=flat)
        except requests.RequestException as e:
            net_err = e
            logger.error("[%s] 通道 %s 连接失败: %s: %s",
                         rid, lane, type(e).__name__, str(e)[:160])
            continue
        if r.status_code in (403, 404):
            body = r.text[:400]
            logger.warning("[%s] 通道 %s 上游 %s: %s", rid, lane, r.status_code, body)
            _dump_save(rid, "resp", r.text)
            if r.status_code == 403 and ("freetiererror" in body.lower()
                                         or "free tier" in body.lower()):
                _gate_mark(lane, rid, "FreeTierError")
                resp403, resp403_lane = r, lane
                continue
            if r.status_code == 404:
                # 该通道没有这个端点（如 zen 通道可能只有 /chat/completions，
                # 没有 /responses）→ 不冷却，回落下一通道；全部 404 时交付
                # 最后一个真实 404 让人看见。
                resp404, resp404_lane = r, lane
                continue
            # 非 FreeTier 的 403（形状类问题）：不冷却，原样交付让人看见
            return r, lane
        if r.status_code == 429:
            # 指纹已过、只是限流（FreeUsageLimitError）：短冷却防硬撞，429 原样
            # 透传——客户端据此等一等再试，比吃 503 语义更准。
            logger.warning("[%s] 通道 %s 上游 429 限流: %s",
                           rid, lane, r.text[:200])
            _gate_mark(lane, rid, "RateLimit", seconds=_COOLDOWN_429_S)
            return r, lane
        if r.status_code >= 400:
            logger.warning("[%s] 通道 %s 上游错误 HTTP %s: %s",
                           rid, lane, r.status_code, r.text[:400])
            _dump_save(rid, "resp", r.text)
            return r, lane
        if lane != _LANE_ORDER[0]:
            logger.warning("[%s] 首选通道不可用，回落通道 %s 成功",
                           rid, lane)
        return r, lane
    if resp403 is not None:
        return resp403, resp403_lane
    if resp404 is not None:
        return resp404, resp404_lane
    if net_err is not None:
        raise net_err
    left = max(_lane_cooling(l) for l in _LANE_ORDER)
    logger.warning("[%s] 全部通道均在风控冷却（剩 %ds），本地快速失败",
                   rid, int(left))
    return _ErrResp(503, json.dumps({
        "error": {"message": "upstream free tier flagged (403), cooldown %ds left, retry later" % int(left),
                  "type": "overloaded_error"}})), "cooldown"


app = Flask(__name__)

# ── free 模型清单（活清单，10 分钟刷新；落盘上次可用清单，冷启动不从空开始）──────
#
# 为什么这么麻烦：2026-10-13 00:25 那波 `拒绝非免费` 的根因不是「模型变了」，而是
# **清单拉不到 = 清单空 = 闸门把所有请求都判成非免费**。实测这条 GET 的失败是
# **成串来的**（同一时刻 curl 也挂：`schannel: failed to receive handshake`），
# 一次失败要 5~6s 才返回，所以"多试几次"在坏窗口里会把用户请求拖死十几秒。
#
# 三条一起才成立：
#   1) 失败不清空旧清单（一次抖动不该丢掉已知可用的名单）
#   2) 清单落盘（重启不从空开始）
#   3) 请求路径上的补拉**只发一次、超时很短**，失败立刻走命名兜底 —— 重试交给后台线程
_free = {"ids": [], "at": 0.0, "fail_until": 0.0}
_free_lock = threading.Lock()
_FREE_CACHE = os.path.join(BASE, "free_cache.json")
_FREE_TTL = 600          # 活清单有效期
_INLINE_TIMEOUT = 4      # 请求路径上补拉的超时：宁可快速失败，不可拖死用户
_FAIL_BACKOFF = 30       # 连续失败后这么久之内不再在请求路径上重试


def _free_ids_from_names(ids):
    """zen 全清单里筛免费面。命名规则即免费面契约：付费 id（claude-*、gpt-* 等）不会匹配。"""
    return [i for i in ids
            if (i.endswith("-free") or i == "big-pickle") and i not in DEPRECATED]


def _load_free_cache():
    try:
        with open(_FREE_CACHE, encoding="utf-8") as f:
            data = json.load(f)
        ids = _free_ids_from_names(data.get("ids") or [])
    except Exception:
        return  # 没有缓存或缓存坏了：正常，首次启动就是这样
    if not ids:
        return
    with _free_lock:
        _free["ids"], _free["at"] = ids, 0.0
    # at=0 让它看起来过期：后台线程 60s 内就会重拉换新。请求路径上不会重拉
    # —— `_model_free` 只在清单为空时才补拉，这里清单非空，直接用缓存不卡用户。
    logger.info("[models] 用上次落盘清单 %d 个兜底（后台 60s 内换新）", len(ids))


def _save_free_cache(ids):
    try:
        tmp = _FREE_CACHE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"ids": ids, "at": int(time.time())}, f)
        os.replace(tmp, _FREE_CACHE)
    except Exception as e:
        logger.warning("[models] 清单落盘失败（不影响转发）: %s", e)


def _fetch_free(force: bool = False, quick: bool = False):
    """从 zen 活清单拉 free 模型（清单会变，活清单优先）。

    `quick=True` 给请求路径用：只试一次、超时 4s、失败进退避窗口。坏窗口里一次
    失败要 5~6s 才返回，重试三轮会在用户请求上挂十几秒 —— 那比"暂时按命名兜底"糟得多。
    真正的重试由后台线程做（它挂多久都不影响转发）。

    失败时**保留旧清单不清空**：2026-10-13 00:25 那波 `拒绝非免费` 就是清单空了，
    把 `mimo-v2.6-flash-free`、`space-bunny-free` 这些合法免费模型一并拒掉。

    新鲜度按**是否联系上上游**记（`at`），不按「清单非空」记：上游真把免费面清空时
    （全部转付费、或命名规则改了导致筛子筛空），空清单也是可信答案，得按 TTL 认
    10 分钟 —— 否则每条请求都会重打一次上游，还会把日志刷成一片 `free 模型 0 个`。
    """
    if not force and time.time() - _free["at"] < _FREE_TTL:
        return
    if quick and time.time() < _free["fail_until"]:
        return          # 刚失败过：这个窗口别再让每个请求都去撞一次
    attempts = 1 if quick else 3
    timeout = _INLINE_TIMEOUT if quick else 20
    last_err = None
    for attempt in range(attempts):
        try:
            # 模型清单与推理同为匿名面；按通道顺序取首个未冷却通道拉取
            lane = next((l for l in _LANE_ORDER if not _lane_cooling(l)),
                        _LANE_ORDER[0])
            headers = _LANE_MODS[lane].build_headers(_stable_session(""))
            base = _LANES[lane]
            r = _upstream_session.get(base + "/models",
                                      headers=headers, timeout=timeout)
            if r.status_code != 200:
                logger.warning("[models] 清单拉取 HTTP %s (lane=%s)",
                               r.status_code, lane)
                return
            ids = [m.get("id") for m in (r.json().get("data") or []) if m.get("id")]
            free_ids = _free_ids_from_names(ids)
            with _free_lock:
                _free["ids"], _free["at"] = free_ids, time.time()
                _free["fail_until"] = 0.0
            if free_ids:
                _save_free_cache(free_ids)
            logger.info("[models] free 模型 %d 个: %s", len(free_ids), ", ".join(free_ids[:12]))
            return
        except Exception as e:
            last_err = e
            if attempt < attempts - 1:
                time.sleep(0.5 * (attempt + 1))
    with _free_lock:
        _free["fail_until"] = time.time() + _FAIL_BACKOFF
    # 带异常类型：这个 except 把网络错误和代码错误（清单结构变了、r.json() 抛的
    # 解析错）吞在同一句「拉取失败」里，只打消息下次排查会认错方向。
    logger.warning("[models] 拉取失败（试 %d 次）: %s: %s；保留旧清单 %d 个",
                   attempts, type(last_err).__name__, last_err, len(_free["ids"]))


def _refresh_loop():
    """后台刷新：每 60s 醒一次，清单还新鲜就立刻返回。

    为什么不是睡 600：原来只在成功的那一轮之后才计时，一次失败要等满 10 分钟才再试，
    坏窗口能连撞 2~3 轮 —— 用户这段时间全在吃 400。改成短周期轮询，失败后 60s 就有
    下一次机会；成功时 `_FREE_TTL` 仍保证 10 分钟才真打一次上游。
    """
    while True:
        time.sleep(60)
        _fetch_free()


def _model_free(mid: str) -> bool:
    if not _free["ids"]:
        _fetch_free(quick=True)
    if _free["ids"]:
        return mid in _free["ids"]
    # 拉不到、也没有可用缓存（首次启动正撞上坏窗口）：按免费面命名规则兜底。
    # 付费 id（claude-* / gpt-* / gemini-* …）匹配不上这条规则，所以这不放宽
    # 「防误打订阅面扣费」的原意，只是不再把合法 free 模型当成非免费全拒。
    return mid.endswith("-free") or mid == "big-pickle"


@app.before_request
def _auth_and_rid():
    g.rid = _next_req_id()
    g.t0 = time.time()
    if API_KEY and request.path != "/health":
        if request.headers.get("Authorization") != "Bearer " + API_KEY:
            return jsonify(error="unauthorized"), 401
    ua = (request.headers.get("User-Agent") or "-").replace("\r", " ").replace("\n", " ")
    try:
        payload = request.get_json(force=True, silent=True) or {}
    except Exception:
        payload = {}
    g.payload = payload
    logger.info("[%s] in: %s %s | model=%s msgs=%s tools=%s stream=%s | ua=%s | body=%dB",
                g.rid, request.method, request.path,
                payload.get("model"), len(payload.get("messages") or []),
                len(payload.get("tools") or []), payload.get("stream"), ua,
                request.content_length or 0)
    if request.path in ("/v1/chat/completions", "/v1/messages"):
        _dump_save(g.rid, "req", request.get_data(cache=True, as_text=True))


@app.route("/health")
def health():
    return jsonify(ok=True, service="the unit2api", route="zen-direct",
                   free_models=len(_free["ids"]))


@app.route("/v1/models")
def models():
    if not _free["ids"]:
        # quick：列模型是客户端一进来就打的请求，不能在这里挂十几秒等上游
        _fetch_free(quick=True)
    now = int(time.time())
    return jsonify(object="list", data=[
        {"id": mid, "object": "model", "created": now, "owned_by": "the unit-zen-free"}
        for mid in _free["ids"]])


PASSTHROUGH = ("model", "messages", "temperature", "top_p", "max_tokens",
               "unit_choice", "stop", "user", "stream_options", "reasoning_effort")



@app.route("/v1/chat/completions", methods=["POST"])
def chat():
    p = g.payload
    model = p.get("model") or ""
    if model and not _model_free(model):
        logger.warning("[%s] 拒绝非免费模型: %s", g.rid, model)
        return jsonify(error={"message": "model %s 不在免费清单（the unit2api 只转发 free 模型）" % model,
                              "type": "invalid_request_error"}), 400

    payload = {k: v for k, v in p.items() if k in PASSTHROUGH}
    payload["model"] = model
    payload["stream"] = True            # 上游只收流式；非流式本地聚合
    payload["stream_options"] = {"include_usage": True}
    # 闸门工具注入已下沉到各 lane 文件（lane_*.prepare_body，随通道而异）：
    # 路由只管协议透传，不再掺和通道门禁细节。
    want_stream = bool(p.get("stream"))

    # ── 上游：按 UPSTREAM_LANES 顺序逐通道尝试（默认 zen 优先、inference 回落）──
    # 连接被切断时补试一次，理由与边界见 _post_lane。
    try:
        upstream, lane = _post_upstream("/chat/completions", payload,
                                        _session_seed(p), g.rid)
    except requests.RequestException as e:
        # 必须在这里返回：upstream 不存在，继续往下走 upstream.status_code
        # 会抛 AttributeError，把网络故障伪装成 500。
        logger.error("[%s] 直连失败: %s: %s", g.rid, type(e).__name__, e)
        return jsonify(error={"message": "upstream error: %s" % e,
                              "type": "upstream_error"}), 502

    if upstream.status_code >= 400:
        body = upstream.text[:400]
        logger.warning("[%s] 上游错误 HTTP %s (lane=%s): %s",
                       g.rid, upstream.status_code, lane, body)
        _dump_save(g.rid, "resp", upstream.text)
        return Response(upstream.text, status=upstream.status_code,
                        content_type=upstream.headers.get("Content-Type", "application/json"))

    if want_stream:
        # 生成器是惰性的，迭代时请求上下文已结束，不能再碰 flask.g
        # （Working outside of application context → done 日志丢失/500）。
        rid, t0 = g.rid, g.t0

        def gen():
            n = 0
            ttft = None
            try:
                for chunk in upstream.iter_content(chunk_size=4096):
                    if chunk:
                        if ttft is None:
                            ttft = time.time() - t0
                        n += len(chunk)
                        yield chunk
            finally:
                # 必须记字节数与首帧耗时：只记耗时时"200 但空流"和"正常返回"无法区分
                logger.info("[%s] done: stream %dB %.1fs ttft=%s", rid, n,
                            time.time() - t0,
                            "%.1fs" % ttft if ttft else "-")
        return Response(gen(), status=200, content_type="text/event-stream")

    # 非流式：本地聚合 SSE
    content, reasoning = [], []
    finish = None
    tool_calls = {}          # index -> {id,name,arguments}（流式按 index 分片）
    usage = None
    for raw in upstream.iter_lines(decode_unicode=False):
        # 上游 SSE 不带 charset，requests 的 unicode 模式会按 Latin-1 猜，
        # 中文全毁；必须字节级收，显式 UTF-8 解码。
        line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        if not line or not line.startswith("data:"):
            continue
        if line.strip() == "data: [DONE]":
            break
        try:
            delta = json.loads(line[5:].strip())
        except Exception:
            continue
        if isinstance(delta.get("usage"), dict):
            usage = delta["usage"]
        for ch in (delta.get("choices") or []):
            d = ch.get("delta") or {}
            if d.get("content"):
                content.append(d["content"])
            if d.get("reasoning_content"):
                reasoning.append(d["reasoning_content"])
            for tc in d.get("tool_calls") or []:
                slot = tool_calls.setdefault(tc.get("index", 0),
                                             {"id": "", "name": "", "arguments": ""})
                fn = tc.get("function") or {}
                slot["id"] = tc.get("id") or slot["id"]
                slot["name"] += fn.get("name") or ""
                slot["arguments"] += fn.get("arguments") or ""
            if ch.get("finish_reason"):
                finish = ch["finish_reason"]
    text = "".join(content)
    message = {"role": "assistant", "content": text}
    if reasoning:
        message["reasoning_content"] = "".join(reasoning)
    if tool_calls:
        message["tool_calls"] = [
            {"id": v["id"] or "call_%d" % i, "type": "function",
             "function": {"name": v["name"], "arguments": v["arguments"]}}
            for i, v in sorted(tool_calls.items())]
        if not text:
            message["content"] = None
        if finish == "stop":
            finish = "tool_calls"
    out = {"id": "chatcmpl-" + uuid.uuid4().hex[:24], "object": "chat.completion",
           "created": int(time.time()), "model": model,
           "choices": [{"index": 0, "message": message,
                        "finish_reason": finish or "stop"}],
           "usage": usage or {"prompt_tokens": 0, "completion_tokens": 0,
                              "total_tokens": 0}}
    body = json.dumps(out, ensure_ascii=False)
    logger.info("[%s] done: status=200 %dB %.1fs", g.rid, len(body), time.time() - g.t0)
    _dump_save(g.rid, "resp", body)
    return Response(body, status=200, content_type="application/json")


# ── /v1/responses 透传（muse-spark-* 等仅支持 Responses 协议的模型）──────────
# 与 chat 同一套免费层指纹；差别：上游路径 /responses、工具为扁平格式
# （无 "function" 包裹层，闸门工具见 lane_inference.GATE_TOOLS_FLAT）、
# 会话种子兼容 input 字段。协议本身不转换，原样透传。


@app.route("/v1/responses", methods=["POST"])
def responses_route():
    p = g.payload
    model = p.get("model") or ""
    if model and not _model_free(model):
        return jsonify(error={"message": "model %s 不在免费清单" % model,
                              "type": "invalid_request_error"}), 400

    payload = dict(p)
    payload["model"] = model
    # 过闸门必需：stream:true（实测 stream:false 即 403）；stream_options
    # 非必需（2026-10-15 F8/G10 定案），补 include_usage 仅对齐官方客户端形
    payload["stream"] = True
    if isinstance(payload.get("stream_options"), dict):
        payload["stream_options"]["include_usage"] = True
    else:
        payload["stream_options"] = {"include_usage": True}
    # 闸门工具注入已下沉到各 lane 文件（flat=True 走扁平工具格式）
    want_stream = bool(p.get("stream"))

    try:
        upstream, lane = _post_upstream("/responses", payload, _session_seed(p), g.rid,
                                        flat=True)
    except requests.RequestException as e:
        logger.error("[%s] 上游连接失败: %s: %s", g.rid, type(e).__name__, e)
        return jsonify(error={"message": str(e), "type": "upstream_error"}), 502

    if upstream.status_code >= 400:
        logger.warning("[%s] 上游错误 HTTP %s (lane=%s): %s", g.rid,
                       upstream.status_code, lane, upstream.text[:400])
        _dump_save(g.rid, "resp", upstream.text)
        return Response(upstream.text, status=upstream.status_code,
                        content_type=upstream.headers.get("Content-Type", "application/json"))

    if want_stream:
        rid, t0 = g.rid, g.t0

        def gen():
            n = 0
            ttft = None
            try:
                for chunk in upstream.iter_content(chunk_size=4096):
                    if chunk:
                        if ttft is None:
                            ttft = time.time() - t0
                        n += len(chunk)
                        yield chunk
            finally:
                logger.info("[%s] done: responses %dB %.1fs ttft=%s", rid, n,
                            time.time() - t0,
                            "%.1fs" % ttft if ttft else "-")
        return Response(gen(), status=200,
                        content_type=upstream.headers.get("Content-Type", "text/event-stream"))

    # 非流式：聚合 Responses SSE。终止帧可能是 completed（正常）或
    # incomplete（max_output_tokens 被推理耗尽即截断，muse-spark 等推理模型
    # 小额度必现）或 failed；三者都是合法的 Responses 终态，原样返回。
    done = None
    for raw in upstream.iter_lines(decode_unicode=False):
        line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        if not line.startswith("data:"):
            continue
        try:
            evt = json.loads(line[5:].strip())
        except Exception:
            continue
        t = evt.get("type")
        if t in ("response.completed", "response.incomplete", "response.failed") \
                and isinstance(evt.get("response"), dict):
            done = evt["response"]
            if t != "response.incomplete":
                break
    if done is None:
        return jsonify(error={"message": "上游未返回终止帧（completed/incomplete/failed）",
                              "type": "upstream_error"}), 502
    body = json.dumps(done, ensure_ascii=False)
    logger.info("[%s] done: responses-aggr %dB %.1fs", g.rid, len(body), time.time() - g.t0)
    return Response(body, status=200, content_type="application/json")


# ── /v1/messages：Anthropic Messages 协议 -> 本地翻成 Chat 走上游 ──────────────
# 上游 /inference/v1/messages 是付费凭证面（免费模型 403、付费 401），免费层
# 走不了直通；这里用 anthropic_bridge 在本地把 Anthropic ⇄ Chat 翻掉，上游照旧
# 走 chat 线（同一套免费层指纹 + 闸门工具）。鉴权同 chat，另接受 x-api-key。


@app.route("/v1/messages", methods=["POST"])
def messages_route():
    p = g.payload
    model = p.get("model") or ""
    if model and not _model_free(model):
        return jsonify(anthropic_bridge.error_body(
            "invalid_request_error",
            "model %s 不在免费清单（the unit2api 只转发 free 模型）" % model)), 400

    try:
        chat_payload = anthropic_bridge.anthropic_to_chat(p)
    except Exception as e:
        return jsonify(anthropic_bridge.error_body(
            "invalid_request_error", "请求翻译失败: %s" % e)), 400
    chat_payload["model"] = model
    chat_payload["stream"] = True            # 上游只收流式
    chat_payload["stream_options"] = {"include_usage": True}
    # 闸门工具注入已下沉到各 lane 文件
    want_stream = bool(p.get("stream"))

    try:
        upstream, lane = _post_upstream("/chat/completions", chat_payload,
                                        _session_seed(chat_payload), g.rid)
    except requests.RequestException as e:
        logger.error("[%s] messages 上游连接失败: %s: %s", g.rid, type(e).__name__, e)
        return jsonify(anthropic_bridge.error_body("api_error", str(e))), 502

    if upstream.status_code >= 400:
        body = upstream.text[:400]
        logger.warning("[%s] messages 上游错误 HTTP %s (lane=%s): %s",
                       g.rid, upstream.status_code, lane, body)
        _dump_save(g.rid, "resp", upstream.text)
        err = anthropic_bridge.error_body(
            anthropic_bridge.err_type_for_http(upstream.status_code), body)
        return Response(json.dumps(err, ensure_ascii=False),
                        status=upstream.status_code,
                        content_type="application/json")

    if want_stream:
        rid, t0 = g.rid, g.t0

        def gen():
            n = 0
            ttft = None
            try:
                for frame in anthropic_bridge.chat_sse_to_anthropic(
                        upstream.iter_lines(decode_unicode=False), model):
                    if ttft is None:
                        ttft = time.time() - t0
                    n += len(frame)
                    yield frame
            finally:
                logger.info("[%s] done: messages %dB %.1fs ttft=%s", rid, n,
                            time.time() - t0,
                            "%.1fs" % ttft if ttft else "-")
        return Response(gen(), status=200, content_type="text/event-stream")

    # 非流式：聚合 Chat SSE -> Chat JSON -> Anthropic Message
    chat_json = anthropic_bridge.aggregate_chat_sse(
        upstream.iter_lines(decode_unicode=False), model)
    uerr = chat_json.pop("_upstream_error", None)
    if uerr is not None:
        err = anthropic_bridge.error_body("api_error", uerr[1])
        return Response(json.dumps(err, ensure_ascii=False),
                        status=502, content_type="application/json")
    result = anthropic_bridge.chat_json_to_anthropic(chat_json, model)
    body = json.dumps(result, ensure_ascii=False)
    logger.info("[%s] done: messages-aggr %dB %.1fs", g.rid, len(body),
                time.time() - g.t0)
    return Response(body, status=200, content_type="application/json")


if __name__ == "__main__":
    _load_session_map()       # 落盘的会话映射兜底，重启不丢 prompt 缓存亲和
    _load_free_cache()        # 先用上次落盘的清单兜底，冷启动不再从空清单开始全拒
    # quick：启动只试一次、4s 封顶。上游正在坏窗口时不该把启动拖到几十秒；
    # 真正的补拉交给下面的后台线程（它挂多久都不影响转发）。
    _fetch_free(quick=True)
    threading.Thread(target=_refresh_loop, daemon=True).start()
    app.run(host=LISTEN_HOST, port=LISTEN_PORT, threaded=True)
