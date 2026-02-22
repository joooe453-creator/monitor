# monitor.py (V2.4.1 - stablecoins filtered by whitelist only; minimal false positives)
# 入口：.\.venv\Scripts\python.exe monitor.py
# 需要套件：
#   .\.venv\Scripts\python.exe -m pip install fastapi uvicorn httpx python-dotenv websockets

import asyncio
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# =========================
# 基本設定
# =========================
REFRESH_SECONDS = 60
QUOTE_ASSET = "USDT"

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINGLASS_BASE = "https://open-api-v4.coinglass.com"
BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_MARKPRICE_ARR_WS = "wss://fstream.binance.com/ws/!markPrice@arr@1s"

CACHE_FILE = Path(__file__).with_name("cache_snapshot.json")

# 5m bucket / 1h window（多空比、持倉量的 1h 變化量統一用這套）
BUCKET_SEC = 5 * 60
WINDOW_1H_SEC = 60 * 60

# 啟動補歷史：抓 13 筆（支援更快拿到 1h 變化量）
BOOTSTRAP_LIMIT = 13

# Binance REST 併發與分批（避免 429）
BINANCE_CONCURRENCY = 10
BINANCE_BATCH = 18
BINANCE_BATCH_SLEEP = 0.20

# =========================
# 穩定幣過濾（低誤殺：只用白名單精準比對）
# 只會刪掉這些 symbol，其他一律保留
# =========================
STABLE_SYMBOLS: Set[str] = {
    # 主流
    "USDT","USDC","DAI","TUSD","FDUSD","USDP","USDD","USD1","USDS",
    "PYUSD","GUSD","LUSD","FRAX","SUSD","BUSD",

    # 收益型 / 近年常見
    "USDE","SUSDE","SRUSDE",

    # 你剛提到的（新增）
    "USDG","UDTB","USDY","USD0","USDAI",

    # 其他
    "USTC","EURC",
}

def is_stablecoin_symbol(sym: str) -> bool:
    return sym.upper().strip() in STABLE_SYMBOLS


# =========================
# 建議分數（A + C 融合）
# =========================
# 這些閾值你之後可微調
FUNDING_HIGH_POS = 0.00010  # +0.01% 視為偏多擁擠（A：偏空）
FUNDING_HIGH_NEG = -0.00010  # -0.01% 視為偏空擁擠（A：偏多）

LS_HIGH = 1.10     # 多空比偏多擁擠
LS_LOW = 0.95      # 多空比偏空擁擠
LS_CHG_HIGH = 0.03 # 1h 變化量明顯偏多擴張
LS_CHG_LOW = -0.03 # 1h 變化量明顯偏空擴張

# 機構/對沖中性條件：OI ↑ 但 funding 接近 0 且 LS 變化小 → 偏中性 3 分
FUNDING_NEAR_ZERO = 0.00003
LS_CHG_NEAR_ZERO = 0.01
LS_NEAR_ONE = 0.05  # |ls_now - 1| <= 0.05


# =========================
# 資料結構
# =========================
@dataclass
class Row:
    rank: int
    symbol: str
    price_usd: float
    market_cap_usd: float
    chg_1h_pct: Optional[float] = None
    chg_24h_pct: Optional[float] = None

    binance_symbol: Optional[str] = None

    # 目前資金費率（WS 或 premiumIndex）
    funding_rate_now: Optional[float] = None
    # 資金費率 8h 變化量（結算 fundingRate 最近兩筆差）
    funding_settle_delta_8h: Optional[float] = None

    # 多空比（5m 最新）＆ 1h 變化量
    ls_ratio_now: Optional[float] = None
    ls_ratio_chg_1h: Optional[float] = None

    # 持倉量（合約數量 openInterest）＆ 1h 變化量
    open_interest_now: Optional[float] = None
    open_interest_chg_1h: Optional[float] = None

    # A+C 綜合分數與說明（只針對 OI 1h 上升的幣）
    score_1to5: Optional[int] = None
    score_note: Optional[str] = None

    updated_at: int = 0


class Store:
    def __init__(self):
        self.rows: Dict[str, Row] = {}

        # Binance mapping：baseAsset -> USDT 永續 symbol（exchangeInfo）
        self.base_to_usdt_perp: Dict[str, str] = {}
        self.usdt_perp_symbols: Set[str] = set()

        # buckets: sym -> dict[bucket_ts -> value]
        self.ls_buckets: Dict[str, Dict[int, float]] = {}
        self.oi_buckets: Dict[str, Dict[int, float]] = {}

        # funding settle 8h：每幣每 bucket 只抓一次
        self.funding_settle_last_fetch_bucket: Dict[str, int] = {}

        self.last_ts: int = 0

        # 最後一次可用快照（避免空白）
        self.last_good_snapshot: List[dict] = []
        self.last_good_ts: int = 0

        # 狀態（給前端）
        self.quality: Dict[str, str] = {
            "coinglass": "未開始",
            "coingecko": "未開始",
            "binance": "未開始",
            "binance_ws": "未開始",
        }

        # WS funding cache：binance_symbol -> (ts, rate)
        self.ws_funding: Dict[str, Tuple[int, float]] = {}
        self.ws_last_update_ts: int = 0

    def upsert(self, r: Row):
        self.rows[r.symbol] = r

    def load_cache_from_disk(self):
        if not CACHE_FILE.exists():
            return
        try:
            obj = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            if isinstance(obj, dict) and isinstance(obj.get("data"), list):
                self.last_good_snapshot = obj["data"]
                self.last_good_ts = int(obj.get("ts") or 0)
        except Exception:
            pass

    def save_cache_to_disk(self, ts: int, data: List[dict]):
        try:
            payload = {"ts": ts, "data": data}
            CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    # ===== bucket 工具 =====
    def _bucket_ts(self, ts: int) -> int:
        return (ts // BUCKET_SEC) * BUCKET_SEC

    def push_bucket(self, buckets: Dict[str, Dict[int, float]], sym: str, ts: int, val: float):
        bts = self._bucket_ts(ts)
        d = buckets.setdefault(sym, {})
        d[bts] = val

        cutoff = ts - WINDOW_1H_SEC - BUCKET_SEC
        cutoff_bts = self._bucket_ts(cutoff)
        for k in list(d.keys()):
            if k < cutoff_bts:
                d.pop(k, None)

    def points_in_1h(self, buckets: Dict[str, Dict[int, float]], sym: str, ts: int) -> List[Tuple[int, float]]:
        d = buckets.get(sym) or {}
        cutoff = ts - WINDOW_1H_SEC
        cutoff_bts = self._bucket_ts(cutoff)
        pts = [(t, v) for (t, v) in d.items() if t >= cutoff_bts]
        pts.sort(key=lambda x: x[0])
        return pts

    def change_1h_or_none(self, buckets: Dict[str, Dict[int, float]], sym: str, ts: int) -> Optional[float]:
        pts = self.points_in_1h(buckets, sym, ts)
        if len(pts) < 2:
            return None
        return pts[-1][1] - pts[0][1]


store = Store()
app = FastAPI()


# =========================
# CoinGecko headers
# =========================
def coingecko_headers() -> Dict[str, str]:
    mode = (os.getenv("COINGECKO_KEY_MODE") or "demo").strip().lower()
    key = (os.getenv("COINGECKO_API_KEY") or "").strip()

    if not key:
        return {"User-Agent": "top100-monitor/2.4.1"}
    if mode == "pro":
        return {"x-cg-pro-api-key": key, "User-Agent": "top100-monitor/2.4.1"}
    return {"x-cg-demo-api-key": key, "User-Agent": "top100-monitor/2.4.1"}


def coinglass_headers() -> Dict[str, str]:
    key = (os.getenv("COINGLASS_API_KEY") or "").strip()
    headers = {"User-Agent": "top100-monitor/2.4.1"}
    if key:
        headers["CG-API-KEY"] = key
    return headers


def binance_headers() -> Dict[str, str]:
    headers = {"User-Agent": "top100-monitor/2.4.1"}
    api_key = (os.getenv("BINANCE_API_KEY") or "").strip()
    if api_key:
        headers["X-MBX-APIKEY"] = api_key
    return headers


async def safe_get_json(
    client: httpx.AsyncClient,
    url: str,
    params: dict,
    timeout: int = 20,
    retries: int = 2,
):
    last_err = None
    for i in range(retries + 1):
        try:
            r = await client.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = 2 + i * 2
                if ra and ra.isdigit():
                    wait = min(10, int(ra))
                await asyncio.sleep(wait)
                last_err = f"429（等待 {wait}s 後重試）"
                continue
            r.raise_for_status()
            return r.json(), None
        except Exception as e:
            last_err = str(e)
            await asyncio.sleep(1 + i)
    return None, last_err


def snapshot_from_store(ts: int) -> List[dict]:
    rows = sorted(store.rows.values(), key=lambda x: x.rank)
    return [asdict(r) for r in rows]


def _first_num(obj: dict, keys: List[str]) -> Optional[float]:
    for k in keys:
        if k in obj and obj[k] is not None:
            try:
                return float(obj[k])
            except Exception:
                continue
    return None


async def fetch_coinglass_futures_coins_markets(client: httpx.AsyncClient) -> Tuple[Optional[List[dict]], Optional[str]]:
    data, err = await safe_get_json(
        client,
        f"{COINGLASS_BASE}/api/futures/coins-markets",
        params={},
        timeout=20,
        retries=2,
    )
    if err:
        return None, err
    if not isinstance(data, dict):
        return None, "回傳格式不是 dict"
    code = str(data.get("code", ""))
    if code not in ("0", "200", ""):
        return None, f"code={code}, msg={data.get('msg')}"
    rows = data.get("data")
    if not isinstance(rows, list):
        return None, "data 不是 list"
    return rows, None


def build_rows_from_coinglass_markets(rows_data: List[dict], now: int) -> List[Row]:
    filtered: List[dict] = []
    for item in rows_data:
        sym = str(item.get("symbol") or "").upper().strip()
        if not sym or is_stablecoin_symbol(sym):
            continue
        filtered.append(item)

    filtered.sort(
        key=lambda x: (_first_num(x, ["market_cap_usd", "marketCapUsd", "market_cap"]) or 0.0),
        reverse=True,
    )

    rows: List[Row] = []
    for idx, item in enumerate(filtered[:100], start=1):
        sym = str(item.get("symbol") or "").upper().strip()
        old = store.rows.get(sym)

        price = _first_num(item, ["current_price", "price", "price_usd"]) or 0.0
        mcap = _first_num(item, ["market_cap_usd", "marketCapUsd", "market_cap"]) or 0.0
        chg_1h = _first_num(item, ["price_change_percent_1h", "price_change_percentage_1h"])
        chg_24h = _first_num(item, ["price_change_percent_24h", "price_change_percentage_24h"])

        r = Row(
            rank=idx,
            symbol=sym,
            price_usd=price,
            market_cap_usd=mcap,
            chg_1h_pct=chg_1h,
            chg_24h_pct=chg_24h,
            binance_symbol=None,
            updated_at=now,
        )

        # Coinglass coins-markets can directly provide some futures metrics.
        r.funding_rate_now = _first_num(item, ["avg_funding_rate_by_oi", "avg_funding_rate_by_vol", "funding_rate"])
        r.open_interest_now = _first_num(item, ["open_interest_usd", "openInterestUsd", "open_interest"])
        r.ls_ratio_now = _first_num(item, ["global_long_short_account_ratio", "long_short_ratio", "ls_ratio"])

        if old is not None:
            # Preserve history-derived fields if source doesn't provide them this round.
            if r.funding_settle_delta_8h is None:
                r.funding_settle_delta_8h = old.funding_settle_delta_8h
            if r.ls_ratio_now is None:
                r.ls_ratio_now = old.ls_ratio_now
            if r.ls_ratio_chg_1h is None:
                r.ls_ratio_chg_1h = old.ls_ratio_chg_1h
            if r.open_interest_now is None:
                r.open_interest_now = old.open_interest_now
            if r.open_interest_chg_1h is None:
                r.open_interest_chg_1h = old.open_interest_chg_1h
            if r.score_1to5 is None:
                r.score_1to5 = old.score_1to5
            if r.score_note is None:
                r.score_note = old.score_note

        rows.append(r)

    return rows


# =========================
# Fetch: CoinGecko
# =========================
async def fetch_coingecko_top100(client: httpx.AsyncClient) -> Tuple[Optional[List[dict]], Optional[str]]:
    data, err = await safe_get_json(
        client,
        f"{COINGECKO_BASE}/coins/markets",
        params={
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "1h,24h",
        },
        timeout=20,
        retries=2,
    )
    if err:
        return None, err
    if not isinstance(data, list):
        return None, "回傳格式不是 list"
    return data, None


# =========================
# Fetch: Binance mapping
# =========================
async def fetch_binance_usdt_perp_mapping(client: httpx.AsyncClient) -> Tuple[Dict[str, str], Set[str], Optional[str]]:
    data, err = await safe_get_json(
        client,
        f"{BINANCE_FAPI}/fapi/v1/exchangeInfo",
        params={},
        timeout=20,
        retries=2,
    )
    if err:
        return {}, set(), err

    out: Dict[str, str] = {}
    symbols_set: Set[str] = set()

    if not isinstance(data, dict):
        return {}, set(), "exchangeInfo 回傳格式異常"

    for s in data.get("symbols", []):
        try:
            if s.get("status") != "TRADING":
                continue
            if s.get("contractType") != "PERPETUAL":
                continue
            if s.get("quoteAsset") != QUOTE_ASSET:
                continue
            base = str(s.get("baseAsset") or "").upper()
            sym = str(s.get("symbol") or "").upper()
            if not base or not sym:
                continue
            out.setdefault(base, sym)
            symbols_set.add(sym)
        except Exception:
            continue

    # 1000 倍合約補一層：1000SHIB -> SHIB
    extra: Dict[str, str] = {}
    for base, sym in out.items():
        if base.startswith("1000") and len(base) > 4:
            stripped = base[4:]
            if stripped and stripped not in out and stripped not in extra:
                extra[stripped] = sym
    out.update(extra)

    return out, symbols_set, None


async def fetch_binance_futures_auth_probe(client: httpx.AsyncClient) -> Optional[str]:
    api_key = (os.getenv("BINANCE_API_KEY") or "").strip()
    api_secret = (os.getenv("BINANCE_API_SECRET") or "").strip()
    if not api_key or not api_secret:
        return "未設定 BINANCE_API_KEY/SECRET"

    try:
        params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
        qs = urlencode(params)
        sig = hmac.new(api_secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
        url = f"{BINANCE_FAPI}/fapi/v2/balance?{qs}&signature={sig}"
        r = await client.get(url, timeout=15)
        if r.status_code == 200:
            return "auth ok"
        text = (r.text or "").strip().replace("\n", " ")
        return f"auth {r.status_code}: {text[:90]}"
    except Exception as e:
        return f"auth error: {str(e)[:90]}"


def map_to_binance_symbol(sym: str) -> Optional[str]:
    s = sym.upper()
    if s in store.base_to_usdt_perp:
        return store.base_to_usdt_perp[s]
    # If mapping is unavailable (e.g. region/IP blocked), disable Binance enrich gracefully.
    if not store.usdt_perp_symbols:
        return None
    cand = f"{s}{QUOTE_ASSET}"
    return cand if cand in store.usdt_perp_symbols else None


# =========================
# Fetch: Binance metrics（REST）
# =========================
def _parse_binance_ts_seconds(obj: dict) -> Optional[int]:
    for k in ("timestamp", "time", "createTime"):
        if k in obj:
            try:
                v = int(obj[k])
                return v // 1000 if v > 10_000_000_000 else v
            except Exception:
                pass
    return None


async def fetch_funding_settle_delta_8h(client: httpx.AsyncClient, bsymbol: str) -> Optional[float]:
    j, err = await safe_get_json(
        client,
        f"{BINANCE_FAPI}/fapi/v1/fundingRate",
        params={"symbol": bsymbol, "limit": 2},
        timeout=15,
        retries=2,
    )
    if err or not isinstance(j, list) or len(j) < 2:
        return None
    try:
        arr_sorted = sorted(j, key=lambda x: int(x.get("fundingTime", 0)))
        prev = float(arr_sorted[-2]["fundingRate"])
        last = float(arr_sorted[-1]["fundingRate"])
        return last - prev
    except Exception:
        return None


async def fetch_funding_now_premium_index(client: httpx.AsyncClient, bsymbol: str) -> Optional[float]:
    j, err = await safe_get_json(
        client,
        f"{BINANCE_FAPI}/fapi/v1/premiumIndex",
        params={"symbol": bsymbol},
        timeout=15,
        retries=2,
    )
    if err or not isinstance(j, dict):
        return None
    try:
        return float(j.get("lastFundingRate"))
    except Exception:
        return None


async def fetch_ls_ratio_5m_latest_point(client: httpx.AsyncClient, bsymbol: str) -> Optional[Tuple[int, float]]:
    j, err = await safe_get_json(
        client,
        f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio",
        params={"symbol": bsymbol, "period": "5m", "limit": 1},
        timeout=15,
        retries=2,
    )
    if err or not isinstance(j, list) or not j:
        return None
    try:
        obj = j[-1]
        ts = _parse_binance_ts_seconds(obj) or int(time.time())
        val = float(obj["longShortRatio"])
        return ts, val
    except Exception:
        return None


async def fetch_ls_ratio_5m_series(client: httpx.AsyncClient, bsymbol: str, limit: int = BOOTSTRAP_LIMIT) -> List[Tuple[int, float]]:
    j, err = await safe_get_json(
        client,
        f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio",
        params={"symbol": bsymbol, "period": "5m", "limit": limit},
        timeout=20,
        retries=2,
    )
    if err or not isinstance(j, list) or not j:
        return []
    out: List[Tuple[int, float]] = []
    for obj in j:
        try:
            ts = _parse_binance_ts_seconds(obj)
            val = float(obj["longShortRatio"])
            if ts is None:
                continue
            out.append((ts, val))
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out


def _parse_open_interest_hist_contracts(obj: dict) -> Optional[float]:
    for k in ("sumOpenInterest", "openInterest"):
        if k in obj:
            try:
                return float(obj[k])
            except Exception:
                pass
    return None


async def fetch_open_interest_hist_5m_series(client: httpx.AsyncClient, bsymbol: str, limit: int = BOOTSTRAP_LIMIT) -> List[Tuple[int, float]]:
    j, err = await safe_get_json(
        client,
        f"{BINANCE_FAPI}/futures/data/openInterestHist",
        params={"symbol": bsymbol, "period": "5m", "limit": limit},
        timeout=20,
        retries=2,
    )
    if err or not isinstance(j, list) or not j:
        return []
    out: List[Tuple[int, float]] = []
    for obj in j:
        try:
            ts = _parse_binance_ts_seconds(obj)
            val = _parse_open_interest_hist_contracts(obj)
            if ts is None or val is None:
                continue
            out.append((ts, val))
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out


# =========================
# A + C 綜合打分（1~5）
# =========================
def score_ac_1to5(
    oi_chg_1h: Optional[float],
    funding_now: Optional[float],
    ls_now: Optional[float],
    ls_chg_1h: Optional[float],
) -> Tuple[Optional[int], Optional[str]]:
    # 只針對 OI 1h 上升
    if oi_chg_1h is None or oi_chg_1h <= 0:
        return None, None

    reasons: List[str] = []
    raw = 0  # >0 偏多，<0 偏空（最後映射到 1~5）

    # C：機構/對沖中性訊號（OI↑ 但 funding 接近 0 且 LS 接近 1 且 LS 變化小）
    if (
        funding_now is not None
        and ls_now is not None
        and ls_chg_1h is not None
        and abs(funding_now) <= FUNDING_NEAR_ZERO
        and abs(ls_now - 1.0) <= LS_NEAR_ONE
        and abs(ls_chg_1h) <= LS_CHG_NEAR_ZERO
    ):
        reasons.append("OI↑ 但 funding/LS 平，偏對沖/套利")
        return 3, "；".join(reasons)

    # A：反擁擠（用 funding + LS 判擁擠，擁擠偏反向）
    if funding_now is not None:
        if funding_now >= FUNDING_HIGH_POS:
            raw -= 1
            reasons.append("funding 偏正(多擁擠)")
        elif funding_now <= FUNDING_HIGH_NEG:
            raw += 1
            reasons.append("funding 偏負(空擁擠)")

    if ls_now is not None:
        if ls_now >= LS_HIGH:
            raw -= 1
            reasons.append("LS 偏多(>1)")
        elif ls_now <= LS_LOW:
            raw += 1
            reasons.append("LS 偏空(<1)")

    if ls_chg_1h is not None:
        if ls_chg_1h >= LS_CHG_HIGH:
            raw -= 1
            reasons.append("LS 1h 上升(多擴張)")
        elif ls_chg_1h <= LS_CHG_LOW:
            raw += 1
            reasons.append("LS 1h 下降(空擴張)")

    if not reasons:
        return 3, "OI↑（其他訊號不足）"

    if raw <= -2:
        score = 1
    elif raw == -1:
        score = 2
    elif raw == 0:
        score = 3
    elif raw == 1:
        score = 4
    else:
        score = 5

    return score, "；".join(reasons[:3])


# =========================
# WebSocket 管理
# =========================
class WSManager:
    def __init__(self):
        self.clients: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)

    async def broadcast(self, payload: dict):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


ws_manager = WSManager()


# =========================
# Binance WS：全市場 fundingRate
# =========================
async def binance_markprice_ws_loop():
    try:
        import websockets  # type: ignore
    except Exception:
        store.quality["binance_ws"] = "停用"
        return

    backoff = 1
    while True:
        try:
            store.quality["binance_ws"] = "連線中"
            async with websockets.connect(BINANCE_MARKPRICE_ARR_WS, ping_interval=20, ping_timeout=20) as ws:
                store.quality["binance_ws"] = "已連線"
                backoff = 1
                while True:
                    msg = await ws.recv()
                    now = int(time.time())
                    store.ws_last_update_ts = now

                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue

                    if isinstance(data, list):
                        for obj in data:
                            try:
                                s = str(obj.get("s") or "").upper()
                                if not s:
                                    continue
                                r = obj.get("r")
                                if r is None:
                                    continue
                                store.ws_funding[s] = (now, float(r))
                            except Exception:
                                continue
                    elif isinstance(data, dict):
                        try:
                            s = str(data.get("s") or "").upper()
                            r = data.get("r")
                            if s and r is not None:
                                store.ws_funding[s] = (now, float(r))
                        except Exception:
                            pass

        except Exception:
            store.quality["binance_ws"] = f"斷線({backoff}s)"
            await asyncio.sleep(min(30, backoff))
            backoff = min(30, backoff * 2)


# =========================
# 主刷新迴圈
# =========================
async def refresh_loop():
    headers = coingecko_headers()
    use_coinglass = bool((os.getenv("COINGLASS_API_KEY") or "").strip())

    async with httpx.AsyncClient(headers=headers) as cg_client, httpx.AsyncClient(
        headers=coinglass_headers()
    ) as cgl_client, httpx.AsyncClient(
        headers=binance_headers()
    ) as bn_client:
        if use_coinglass:
            store.base_to_usdt_perp = {}
            store.usdt_perp_symbols = set()
            store.quality["binance"] = "停用（改用 CoinGlass）"
            store.quality["coinglass"] = "已啟用"
        else:
            store.quality["coinglass"] = "未設定 API Key"
            # Binance mapping (best-effort; failure should not block CoinGecko rows)
            try:
                m, symbols_set, err = await fetch_binance_usdt_perp_mapping(bn_client)
                if err:
                    store.base_to_usdt_perp = {}
                    store.usdt_perp_symbols = set()
                    auth_probe = await fetch_binance_futures_auth_probe(bn_client)
                    store.quality["binance"] = f"映射失敗(已降級): {err[:55]} | {auth_probe[:55]}"
                else:
                    store.base_to_usdt_perp = m
                    store.usdt_perp_symbols = symbols_set
                    store.quality["binance"] = "映射成功"
            except Exception:
                store.base_to_usdt_perp = {}
                store.usdt_perp_symbols = set()
                store.quality["binance"] = "映射失敗(已降級): exception"

        sem = asyncio.Semaphore(BINANCE_CONCURRENCY)

        while True:
            try:
                start = time.time()
                now = int(time.time())

                # WS 狀態縮短顯示
                if store.ws_last_update_ts > 0 and store.quality.get("binance_ws", "").startswith("已連線"):
                    age = now - store.ws_last_update_ts
                    store.quality["binance_ws"] = "已連線" if age <= 6 else f"已連線({age}s)"

                rows: List[Row] = []
                source_used = ""

                if use_coinglass:
                    cgl_rows, cgl_err = await fetch_coinglass_futures_coins_markets(cgl_client)
                    if cgl_rows is None:
                        store.quality["coinglass"] = f"失敗: {str(cgl_err)[:90]}"
                    else:
                        rows = build_rows_from_coinglass_markets(cgl_rows, now)
                        source_used = "coinglass"
                        store.quality["coinglass"] = "成功"
                        store.quality["coingecko"] = "停用（由 CoinGlass 提供）"

                # 1) CoinGecko fallback / default source
                if not rows:
                    top, cg_err = await fetch_coingecko_top100(cg_client)

                    if top is None:
                        if use_coinglass:
                            store.quality["coinglass"] = store.quality.get("coinglass", "失敗")
                        if store.last_good_snapshot:
                            store.quality["coingecko"] = "快取"
                            await ws_manager.broadcast(
                                {
                                    "type": "snapshot",
                                    "ts": store.last_good_ts,
                                    "took_ms": 0,
                                    "data": store.last_good_snapshot,
                                    "quality": store.quality,
                                }
                            )
                        else:
                            store.quality["coingecko"] = "失敗"
                            await ws_manager.broadcast(
                                {
                                    "type": "snapshot",
                                    "ts": now,
                                    "took_ms": int((time.time() - start) * 1000),
                                    "data": [],
                                    "quality": store.quality,
                                }
                            )
                            await ws_manager.broadcast(
                                {
                                    "type": "error",
                                    "message": f"CoinGecko 取資料失敗：{cg_err}",
                                    "quality": store.quality,
                                }
                            )

                        await asyncio.sleep(REFRESH_SECONDS)
                        continue

                    source_used = "coingecko"
                    store.quality["coingecko"] = "成功"

                    # 2) 建 100 行（刪穩定幣：只用白名單精準比對）
                    for item in top:
                        sym = str(item.get("symbol", "")).upper()

                        if is_stablecoin_symbol(sym):
                            continue

                        rank = int(item.get("market_cap_rank") or 0)
                        price = float(item.get("current_price") or 0.0)
                        mcap = float(item.get("market_cap") or 0.0)

                        chg_1h = item.get("price_change_percentage_1h_in_currency")
                        chg_24h = item.get("price_change_percentage_24h_in_currency")
                        chg_1h = float(chg_1h) if chg_1h is not None else None
                        chg_24h = float(chg_24h) if chg_24h is not None else None

                        bsymbol = map_to_binance_symbol(sym)
                        old = store.rows.get(sym)
                        r = Row(
                            rank=rank,
                            symbol=sym,
                            price_usd=price,
                            market_cap_usd=mcap,
                            chg_1h_pct=chg_1h,
                            chg_24h_pct=chg_24h,
                            binance_symbol=bsymbol,
                            updated_at=now,
                        )

                        if old is not None:
                            r.funding_rate_now = old.funding_rate_now
                            r.funding_settle_delta_8h = old.funding_settle_delta_8h
                            r.ls_ratio_now = old.ls_ratio_now
                            r.ls_ratio_chg_1h = old.ls_ratio_chg_1h
                            r.open_interest_now = old.open_interest_now
                            r.open_interest_chg_1h = old.open_interest_chg_1h
                            r.score_1to5 = old.score_1to5
                            r.score_note = old.score_note

                        rows.append(r)

                if not rows:
                    if store.last_good_snapshot:
                        store.quality["coingecko"] = "快取"
                        await ws_manager.broadcast(
                            {
                                "type": "snapshot",
                                "ts": store.last_good_ts,
                                "took_ms": 0,
                                "data": store.last_good_snapshot,
                                "quality": store.quality,
                            }
                        )
                    else:
                        store.quality["coingecko"] = "失敗（來源回空）"
                        await ws_manager.broadcast(
                            {
                                "type": "snapshot",
                                "ts": now,
                                "took_ms": int((time.time() - start) * 1000),
                                "data": [],
                                "quality": store.quality,
                            }
                        )
                        await ws_manager.broadcast(
                            {
                                "type": "error",
                                "message": "資料來源回空（CoinGlass/CoinGecko）",
                                "quality": store.quality,
                            }
                        )

                    await asyncio.sleep(REFRESH_SECONDS)
                    continue

                for r in rows:
                    store.upsert(r)

                keep = {r.symbol for r in rows}
                for k in list(store.rows.keys()):
                    if k not in keep:
                        store.rows.pop(k, None)

                store.last_ts = now

                data_phase1 = snapshot_from_store(now)
                store.last_good_snapshot = data_phase1
                store.last_good_ts = now
                store.save_cache_to_disk(now, data_phase1)

                await ws_manager.broadcast(
                    {
                        "type": "snapshot",
                        "ts": now,
                        "took_ms": int((time.time() - start) * 1000),
                        "data": data_phase1,
                        "quality": store.quality,
                    }
                )

                async def enrich_one(rw: Row):
                    if not rw.binance_symbol:
                        rw.funding_rate_now = None
                        rw.funding_settle_delta_8h = None
                        rw.ls_ratio_now = None
                        rw.ls_ratio_chg_1h = None
                        rw.open_interest_now = None
                        rw.open_interest_chg_1h = None
                        rw.score_1to5 = None
                        rw.score_note = None
                        return

                    async with sem:
                        try:
                            hit = store.ws_funding.get(rw.binance_symbol.upper())
                            if hit is not None:
                                _, fr = hit
                                rw.funding_rate_now = fr
                            else:
                                rw.funding_rate_now = await fetch_funding_now_premium_index(bn_client, rw.binance_symbol)
                        except Exception:
                            pass

                        try:
                            bts = store._bucket_ts(now)
                            last_b = store.funding_settle_last_fetch_bucket.get(rw.symbol)
                            if last_b != bts:
                                store.funding_settle_last_fetch_bucket[rw.symbol] = bts
                                rw.funding_settle_delta_8h = await fetch_funding_settle_delta_8h(bn_client, rw.binance_symbol)
                        except Exception:
                            pass

                        try:
                            if not (store.ls_buckets.get(rw.symbol) or {}):
                                series = await fetch_ls_ratio_5m_series(bn_client, rw.binance_symbol, limit=BOOTSTRAP_LIMIT)
                                for ts_s, val in series:
                                    store.push_bucket(store.ls_buckets, rw.symbol, ts_s, val)

                            latest_ls = await fetch_ls_ratio_5m_latest_point(bn_client, rw.binance_symbol)
                            if latest_ls is not None:
                                ts_s, val = latest_ls
                                rw.ls_ratio_now = val
                                store.push_bucket(store.ls_buckets, rw.symbol, ts_s, val)

                            rw.ls_ratio_chg_1h = store.change_1h_or_none(store.ls_buckets, rw.symbol, now)
                        except Exception:
                            pass

                        try:
                            if not (store.oi_buckets.get(rw.symbol) or {}):
                                series = await fetch_open_interest_hist_5m_series(bn_client, rw.binance_symbol, limit=BOOTSTRAP_LIMIT)
                                for ts_s, val in series:
                                    store.push_bucket(store.oi_buckets, rw.symbol, ts_s, val)
                                if series:
                                    rw.open_interest_now = series[-1][1]
                            else:
                                series = await fetch_open_interest_hist_5m_series(bn_client, rw.binance_symbol, limit=1)
                                if series:
                                    ts_s, val = series[-1]
                                    rw.open_interest_now = val
                                    store.push_bucket(store.oi_buckets, rw.symbol, ts_s, val)

                            rw.open_interest_chg_1h = store.change_1h_or_none(store.oi_buckets, rw.symbol, now)
                        except Exception:
                            pass

                        try:
                            s, note = score_ac_1to5(
                                rw.open_interest_chg_1h,
                                rw.funding_rate_now,
                                rw.ls_ratio_now,
                                rw.ls_ratio_chg_1h,
                            )
                            rw.score_1to5 = s
                            rw.score_note = note
                        except Exception:
                            pass

                if source_used == "coinglass":
                    # Recompute score from whatever Coinglass metrics are available.
                    for rw in rows:
                        try:
                            s, note = score_ac_1to5(
                                rw.open_interest_chg_1h,
                                rw.funding_rate_now,
                                rw.ls_ratio_now,
                                rw.ls_ratio_chg_1h,
                            )
                            rw.score_1to5 = s
                            rw.score_note = note
                        except Exception:
                            pass

                    data_phase2 = snapshot_from_store(now)
                    await ws_manager.broadcast(
                        {
                            "type": "snapshot",
                            "ts": now,
                            "took_ms": int((time.time() - start) * 1000),
                            "data": data_phase2,
                            "quality": store.quality,
                        }
                    )
                elif store.usdt_perp_symbols:
                    for i in range(0, len(rows), BINANCE_BATCH):
                        chunk = rows[i : i + BINANCE_BATCH]
                        tasks = [enrich_one(r) for r in chunk]
                        await asyncio.gather(*tasks, return_exceptions=True)
                        await asyncio.sleep(BINANCE_BATCH_SLEEP)

                    data_phase2 = snapshot_from_store(now)
                    await ws_manager.broadcast(
                        {
                            "type": "snapshot",
                            "ts": now,
                            "took_ms": int((time.time() - start) * 1000),
                            "data": data_phase2,
                            "quality": store.quality,
                        }
                    )

                await asyncio.sleep(REFRESH_SECONDS)
            except Exception as loop_err:
                # Keep the service alive even if one cycle crashes unexpectedly.
                store.quality["coingecko"] = f"失敗({type(loop_err).__name__})"
                await ws_manager.broadcast(
                    {
                        "type": "error",
                        "message": f"刷新循環失敗：{loop_err}",
                        "quality": store.quality,
                    }
                )
                await asyncio.sleep(REFRESH_SECONDS)


# =========================
# FastAPI lifecycle
# =========================
@app.on_event("startup")
async def startup():
    load_dotenv()
    store.load_cache_from_disk()
    use_coinglass = bool((os.getenv("COINGLASS_API_KEY") or "").strip())

    # 啟動先推快取：第一次開也秒出表格
    if store.last_good_snapshot:
        store.last_ts = store.last_good_ts
        store.quality["coingecko"] = "快取"
        store.quality["coinglass"] = "已啟用" if use_coinglass else "未設定 API Key"
        store.quality["binance"] = "停用（改用 CoinGlass）" if use_coinglass else "未知"
        asyncio.create_task(
            ws_manager.broadcast(
                {
                    "type": "snapshot",
                    "ts": store.last_good_ts,
                    "took_ms": 0,
                    "data": store.last_good_snapshot,
                    "quality": store.quality,
                }
            )
        )

    if use_coinglass:
        store.quality["binance_ws"] = "停用（改用 CoinGlass）"
    else:
        asyncio.create_task(binance_markprice_ws_loop())
    asyncio.create_task(refresh_loop())


# =========================
# 前端（精簡版）
# =========================
HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Top100 Monitor Dashboard</title>
  <style>
    :root {
      --bg: #0b1220;
      --panel: rgba(18, 28, 48, 0.9);
      --panel2: rgba(255, 255, 255, 0.03);
      --line: rgba(255, 255, 255, 0.08);
      --text: #e8efff;
      --muted: #9fb0d5;
      --good: #8df0b8;
      --bad: #ffb3b3;
      --shadow: 0 20px 60px rgba(0, 0, 0, 0.35);
    }
    * { box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans TC", Arial, sans-serif;
      margin: 0;
      color: var(--text);
      background:
        radial-gradient(circle at 15% 15%, rgba(70, 110, 255, 0.22), transparent 42%),
        radial-gradient(circle at 85% 10%, rgba(0, 219, 170, 0.14), transparent 38%),
        linear-gradient(180deg, #070b14 0%, #0b1220 100%);
    }
    .shell { max-width: 1600px; margin: 0 auto; padding: 20px; }
    .hero {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.015));
      box-shadow: var(--shadow);
      padding: 16px 18px;
      margin-bottom: 14px;
    }
    .hero h1 { margin: 0 0 6px; font-size: 20px; letter-spacing: .02em; }
    .hero p { margin: 0; color: var(--muted); font-size: 13px; line-height: 1.5; }
    .meta { margin-bottom: 12px; display: grid; gap: 4px; }
    .small { font-size: 12px; opacity: 0.9; line-height: 1.5; color: var(--muted); }
    .pill { display: inline-block; padding: 3px 9px; border-radius: 999px; border: 1px solid var(--line); font-size: 12px; }
    .pill.on { background: rgba(141, 240, 184, 0.14); color: var(--good); }
    .pill.off { background: rgba(255, 179, 179, 0.1); color: var(--bad); }

    .controls {
      display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin: 10px 0 14px;
      border: 1px solid var(--line); border-radius: 14px; background: var(--panel2); padding: 12px;
    }
    .controls label { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; color: var(--muted); }
    .controls select, .controls input[type="text"] {
      padding: 7px 10px; font-size: 13px; color: var(--text); background: rgba(255,255,255,0.03);
      border: 1px solid var(--line); border-radius: 10px;
    }
    .controls input[type="checkbox"] { accent-color: #5a8cff; }

    .tableWrap {
      overflow-x: auto; border: 1px solid var(--line); border-radius: 14px; background: var(--panel);
      box-shadow: var(--shadow);
    }
    table { border-collapse: collapse; width: 100%; min-width: 2050px; }
    th, td { border: 1px solid rgba(255,255,255,0.05); padding: 10px 10px; font-size: 13px; vertical-align: middle; }
    th { background: rgba(255,255,255,0.03); color: var(--text); position: sticky; top: 0; z-index: 1; }
    td { background: transparent; }
    tbody tr:nth-child(even) td { background: rgba(255,255,255,0.015); }

    td.hm { font-variant-numeric: tabular-nums; text-align: center; font-weight: 600; border-color: #eeeeee; min-width: 90px; }
    td.num { font-variant-numeric: tabular-nums; text-align: right; }
    td.sym { font-weight: 700; }
    td.tag { text-align:center; font-weight: 800; }

    th .thwrap { line-height: 1.25; white-space: normal; }

    th.w-rank { min-width: 60px; }
    th.w-sym  { min-width: 90px; }
    th.w-price{ min-width: 120px; }
    th.w-mcap { min-width: 120px; }
    th.w-1h, th.w-24h { min-width: 90px; }

    th.w-frNow { min-width: 150px; }
    th.w-fr8h  { min-width: 190px; }

    th.w-lsNow { min-width: 140px; }
    th.w-ls1h  { min-width: 180px; }

    th.w-oiNow { min-width: 160px; }
    th.w-oi1h  { min-width: 180px; }

    th.w-score { min-width: 90px; }
    th.w-note { min-width: 320px; }
    @media (max-width: 768px) {
      .shell { padding: 12px; }
      .hero h1 { font-size: 17px; }
    }

  </style>
</head>
<body>
  <div class="shell">
  <div class="hero">
    <h1>Crypto Top100 Monitor</h1>
    <p>即時監控市值前 100 幣種，整合 CoinGecko + Binance（Funding / Long-Short / Open Interest）資料。</p>
  </div>
  <div class="meta">
    <div>連線狀態：<span id="wsPill" class="pill off">連線中…</span></div>
    <div class="small" id="last">正在載入資料中…</div>
    <div class="small" id="quality"></div>
    <div class="small" id="err" style="color:#ffb3b3;"></div>
  </div>

  <div class="controls">
    <label><input type="checkbox" id="onlyFutures" /> 只看有合約</label>
    <label>搜尋：
      <input type="text" id="search" placeholder="例如 BTC" />
    </label>
    <label>排序：
      <select id="sortKey">
        <option value="rank">排名</option>
        <option value="price_usd">價格</option>
        <option value="market_cap_usd">市值</option>
        <option value="chg_1h_pct">1 小時</option>
        <option value="chg_24h_pct">24 小時</option>

        <option value="funding_rate_now">目前資金費率</option>
        <option value="funding_settle_delta_8h">資金費率 8 小時變化量</option>

        <option value="ls_ratio_now">目前多空比</option>
        <option value="ls_ratio_chg_1h">多空比 1 小時變化量</option>

        <option value="open_interest_now">持倉量</option>
        <option value="open_interest_chg_1h">持倉量 1 小時變化量</option>

        <option value="score_1to5">分數</option>
      </select>
    </label>
    <label>方向：
      <select id="sortDir">
        <option value="asc">小 → 大</option>
        <option value="desc">大 → 小</option>
      </select>
    </label>
  </div>

  <div class="tableWrap">
    <table>
      <thead>
        <tr>
          <th class="w-rank"><div class="thwrap">#</div></th>
          <th class="w-sym"><div class="thwrap">幣種</div></th>
          <th class="w-price"><div class="thwrap">價格</div></th>
          <th class="w-mcap"><div class="thwrap">市值</div></th>
          <th class="w-1h"><div class="thwrap">1 小時</div></th>
          <th class="w-24h"><div class="thwrap">24 小時</div></th>

          <th class="w-frNow"><div class="thwrap">目前資金費率</div></th>
          <th class="w-fr8h"><div class="thwrap">資金費率 8 小時變化量</div></th>

          <th class="w-lsNow"><div class="thwrap">目前多空比</div></th>
          <th class="w-ls1h"><div class="thwrap">多空比 1 小時變化量</div></th>

          <th class="w-oiNow"><div class="thwrap">持倉量</div></th>
          <th class="w-oi1h"><div class="thwrap">持倉量 1 小時變化量</div></th>

          <th class="w-score"><div class="thwrap">分數</div></th>
          <th class="w-note"><div class="thwrap">說明</div></th>
        </tr>
      </thead>
      <tbody id="tb">
        <tr><td colspan="14" style="text-align:center; opacity:0.7; padding: 18px;">正在載入資料中…</td></tr>
      </tbody>
    </table>
  </div>

<script>
  const tb = document.getElementById("tb");
  const wsPill = document.getElementById("wsPill");
  const lastEl = document.getElementById("last");
  const qualityEl = document.getElementById("quality");
  const errEl = document.getElementById("err");
  function renderQuality(quality) {
    const cgl = quality?.coinglass || "-";
    const cg = quality?.coingecko || "-";
    const bn = quality?.binance || "-";
    const bws = quality?.binance_ws || "-";
    qualityEl.textContent = `資料狀態：CoinGlass：${cgl}；CoinGecko：${cg}；Binance：${bn}；WS：${bws}`;
  }

  const onlyFuturesEl = document.getElementById("onlyFutures");
  const searchEl      = document.getElementById("search");
  const sortKeyEl     = document.getElementById("sortKey");
  const sortDirEl     = document.getElementById("sortDir");

  let latestRows = [];

  function fmt(x, digits=6) {
    if (x === null || x === undefined) return "-";
    if (typeof x !== "number") return x;
    return x.toFixed(digits);
  }

  function fmtSigned(x, digits=4) {
    if (x === null || x === undefined) return "-";
    if (typeof x !== "number") return x;
    const sign = x > 0 ? "+" : "";
    return sign + x.toFixed(digits);
  }

  function fmtBig(x) {
    if (x === null || x === undefined) return "-";
    if (typeof x !== "number") return x;
    const ax = Math.abs(x);
    if (ax >= 1e12) return (x/1e12).toFixed(2) + "T";
    if (ax >= 1e9) return (x/1e9).toFixed(2) + "B";
    if (ax >= 1e6) return (x/1e6).toFixed(2) + "M";
    if (ax >= 1e3) return (x/1e3).toFixed(2) + "K";
    return x.toFixed(2);
  }

  function fmtBigSigned(x) {
    if (x === null || x === undefined) return "-";
    if (typeof x !== "number") return x;
    const sign = x > 0 ? "+" : "";
    return sign + fmtBig(x);
  }

  function fmtPctFromRate(rate, digits=3) {
    if (rate === null || rate === undefined) return "-";
    if (typeof rate !== "number") return rate;
    const pct = rate * 100;
    const sign = pct > 0 ? "+" : "";
    return sign + pct.toFixed(digits) + "%";
  }

  function heatStyle(pct) {
    if (pct === null || pct === undefined || typeof pct !== "number") return "";
    const maxAbs = 10.0;
    const x = Math.max(-maxAbs, Math.min(maxAbs, pct));
    const t = Math.min(1, Math.abs(x) / maxAbs);
    const isUp = x >= 0;
    const hue = isUp ? 140 : 0;
    const sat = 65;
    const light = 96 - t * 22;
    const bg = `hsl(${hue} ${sat}% ${light}%)`;
    const fg = "#0b1424";
    return `background:${bg}; color:${fg}; text-shadow:none;`;
  }

  function heatText(pct) {
    if (pct === null || pct === undefined || typeof pct !== "number") return "-";
    const sign = pct > 0 ? "+" : "";
    return sign + pct.toFixed(2) + "%";
  }

  function hasContract(r) {
    return !!r.binance_symbol;
  }

  function applyFiltersAndSort(rows) {
    const q = (searchEl.value || "").trim().toUpperCase();
    let out = rows.slice();

    if (onlyFuturesEl.checked) out = out.filter(hasContract);
    if (q) out = out.filter(r => (r.symbol || "").toUpperCase().includes(q));

    const key = sortKeyEl.value;
    const dir = sortDirEl.value;

    out.sort((a,b) => {
      const va = (a[key] === null || a[key] === undefined) ? -Infinity : a[key];
      const vb = (b[key] === null || b[key] === undefined) ? -Infinity : b[key];
      if (va < vb) return dir === "asc" ? -1 : 1;
      if (va > vb) return dir === "asc" ? 1 : -1;
      return 0;
    });

    return out;
  }

  function scoreStyle(s) {
    if (s === null || s === undefined) return "";
    if (s <= 2) return "background:#ffe5e5;";
    if (s === 3) return "background:#f5f5f5;";
    return "background:#eaffea;";
  }

  function render() {
    const rows = applyFiltersAndSort(latestRows);
    tb.innerHTML = "";

    if (!rows.length) {
      tb.innerHTML = `<tr><td colspan="14" style="text-align:center; opacity:0.7; padding: 18px;">沒有符合條件的資料</td></tr>`;
      return;
    }

    for (const r of rows) {
      const tr = document.createElement("tr");
      const sc = (r.score_1to5 === null || r.score_1to5 === undefined) ? "-" : r.score_1to5;
      const note = r.score_note || "-";

      tr.innerHTML = `
        <td class="num">${r.rank}</td>
        <td class="sym">${r.symbol}</td>
        <td class="num">${fmt(r.price_usd, 6)}</td>
        <td class="num">${fmtBig(r.market_cap_usd)}</td>

        <td class="hm" style="${heatStyle(r.chg_1h_pct)}">${heatText(r.chg_1h_pct)}</td>
        <td class="hm" style="${heatStyle(r.chg_24h_pct)}">${heatText(r.chg_24h_pct)}</td>

        <td class="num">${fmtPctFromRate(r.funding_rate_now, 3)}</td>
        <td class="num">${fmtPctFromRate(r.funding_settle_delta_8h, 3)}</td>

        <td class="num">${(r.ls_ratio_now === null || r.ls_ratio_now === undefined) ? "-" : fmt(r.ls_ratio_now, 3)}</td>
        <td class="num">${fmtSigned(r.ls_ratio_chg_1h, 4)}</td>

        <td class="num">${(r.open_interest_now === null || r.open_interest_now === undefined) ? "-" : fmtBig(r.open_interest_now)}</td>
        <td class="num">${fmtBigSigned(r.open_interest_chg_1h)}</td>

        <td class="tag" style="${(typeof sc === 'number') ? scoreStyle(sc) : ''}">${sc}</td>
        <td>${note}</td>
      `;
      tb.appendChild(tr);
    }
  }

  [onlyFuturesEl, sortKeyEl, sortDirEl].forEach(el => el.addEventListener("change", render));
  searchEl.addEventListener("input", render);

  const wsProto = (location.protocol === "https:") ? "wss" : "ws";
  const ws = new WebSocket(`${wsProto}://${location.host}/ws`);

  ws.onopen = () => { wsPill.textContent = "已連線"; wsPill.className = "pill on"; };
  ws.onclose = () => { wsPill.textContent = "已斷線"; wsPill.className = "pill off"; };
  ws.onerror = () => { wsPill.textContent = "錯誤"; wsPill.className = "pill off"; };

  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);

    if (msg.type === "snapshot") {
      errEl.textContent = "";
      latestRows = msg.data || [];
      const ts = msg.ts || Math.floor(Date.now()/1000);
      const took = msg.took_ms || 0;
      lastEl.textContent = `最後更新：${new Date(ts*1000).toLocaleString()}｜耗時：${took} ms`;

      if (msg.quality) renderQuality(msg.quality);

      render();
      return;
    }

    if (msg.type === "error") {
      errEl.textContent = msg.message || "資料更新失敗";
      if (msg.quality) renderQuality(msg.quality);
    }
  };
</script>
</div>
</body>
</html>
"""


@app.get("/healthz")
async def healthz():
    return {"ok": True, "ts": int(time.time()), "last_ts": store.last_ts, "quality": store.quality}


@app.get("/")
async def index():
    return HTMLResponse(HTML)


@app.websocket("/ws")
async def ws(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        now = int(time.time())

        if store.last_good_snapshot:
            await ws.send_json(
                {
                    "type": "snapshot",
                    "ts": store.last_good_ts or now,
                    "took_ms": 0,
                    "data": store.last_good_snapshot,
                    "quality": store.quality,
                }
            )
        else:
            data = snapshot_from_store(store.last_ts or now)
            await ws.send_json({"type": "snapshot", "ts": store.last_ts or now, "took_ms": 0, "data": data, "quality": store.quality})

        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
    except Exception:
        ws_manager.disconnect(ws)


if __name__ == "__main__":
    import socket
    import uvicorn

    def pick_free_port(host: str, start_port: int = 8001, max_tries: int = 200) -> int:
        """
        自動找可用 port：
        8001 被佔用 -> 8002 -> 8003 -> 8004 -> ...
        """
        for p in range(start_port, start_port + max_tries):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind((host, p))
                    return p
                except OSError:
                    continue
        raise RuntimeError(f"找不到可用 port（從 {start_port} 起試了 {max_tries} 個）")

    is_managed = bool(os.getenv("PORT"))
    host = "0.0.0.0" if is_managed else "127.0.0.1"
    if is_managed:
      port = int(os.getenv("PORT", "8000"))
    else:
      port = pick_free_port(host, start_port=8001, max_tries=200)

    print(f"[monitor] using http://{host}:{port}" + ("  (managed port)" if is_managed else "  (auto-picked)"))
    uvicorn.run(app, host=host, port=port, reload=False)









