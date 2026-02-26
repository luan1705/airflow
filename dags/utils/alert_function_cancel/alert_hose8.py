import os, json, math, logging, concurrent.futures
from typing import Dict, Tuple, List
from collections.abc import Mapping, Sequence
from .List.symbol_list import HOSE8

import pandas as pd
import numpy as np
import redis

# ================ CONFIG ================
REDIS_URL = os.getenv("REDIS_URL", "redis://default:%40Vns123456@videv.cloud:6379/1")
CHANNEL   = "alert_function"

POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL, decode_responses=True,
    socket_timeout=2.5, socket_connect_timeout=2.0,
    health_check_interval=30, max_connections=3, timeout=1.0
)
r = redis.Redis(connection_pool=POOL)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ================ JSON SANITIZER (NaN/Inf/NaT -> None) ================
def to_native(o):
    if isinstance(o, Mapping):
        return {k: to_native(v) for k, v in o.items()}
    if isinstance(o, Sequence) and not isinstance(o, (str, bytes, bytearray)):
        return [to_native(v) for v in o]
    try:
        if isinstance(o, pd.Timestamp):
            return None if pd.isna(o) else o.isoformat()
        if o is pd.NA:
            return None
    except Exception:
        pass
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        f = float(o)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    return o


# ================ REDIS IO ================
def read_alert(symbol: str) -> Dict:
    """Đọc alert_function:{symbol} -> dict (có thể rỗng)."""
    try:
        raw = r.get(f"alert_function:{symbol}")
        return json.loads(raw) if raw else {}
    except Exception as e:
        logging.warning("Read alert %s fail: %s", symbol, e)
        return {}

def get_ohlcv_history(symbol: str) -> pd.DataFrame:
    """Đọc list JSON ở key 'history_tradingview: {symbol}' -> DataFrame (có thể rỗng)."""
    try:
        vals = r.lrange(f"history_tradingview: {symbol}", 0, -1)
        if not vals:
            return pd.DataFrame(columns=["time","close","volume"])
        df = pd.DataFrame([json.loads(v) for v in vals])
        if "time" in df: df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["close"]  = pd.to_numeric(df.get("close"),  errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
        df = df.dropna(subset=["close"]).reset_index(drop=True)
        if "time" not in df: df["time"] = pd.NaT
        return df[["time","close","volume"]]
    except Exception as e:
        logging.error("History %s fail: %s", symbol, e)
        return pd.DataFrame(columns=["time","close","volume"])

def publish(payload: Dict, channel: str = CHANNEL) -> bool:
    """Publish JSON-safe, đổi NaN/Inf/NaT→None, retry 1 lần nếu lỗi."""
    try:
        payload.setdefault("source", channel)
        msg = json.dumps(to_native(payload), ensure_ascii=False, allow_nan=False)
        r.publish(channel, msg)
        return True
    except Exception as e:
        logging.warning("Publish fail (%s): %s. Retrying...", channel, e)
        try:
            _r = redis.Redis(connection_pool=POOL)
            msg = json.dumps(to_native(payload), ensure_ascii=False, allow_nan=False)
            _r.publish(channel, msg)
            logging.info("Reconnected & published")
            return True
        except Exception as e2:
            logging.error("Retry publish failed: %s", e2)
            return False


# ================ INDICATORS ================
def _ensure_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame({"close": pd.Series(dtype="float64"),
                             "volume": pd.Series(dtype="float64")})
    if "close" not in df:  df["close"]  = pd.NA
    if "volume" not in df: df["volume"] = 0
    df["close"]  = pd.to_numeric(df["close"],  errors="coerce").astype("float64")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("float64")
    return df[["close","volume"]]

def compute_indicators(df: pd.DataFrame, new_close: float, new_volume: float) -> Tuple[pd.DataFrame, Dict]:
    df = _ensure_ohlcv(df)
    df = pd.concat([df, pd.DataFrame([{"close": float(new_close), "volume": float(new_volume)}])], ignore_index=True)

    s_close  = pd.to_numeric(df["close"],  errors="coerce").astype("float64")
    s_volume = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("float64")

    # MAs
    df["MA10"] = s_close.rolling(10).mean()
    df["MA20"] = s_close.rolling(20).mean()
    df["MA50"] = s_close.rolling(50).mean()

    # MACD
    ema12 = s_close.ewm(span=12, adjust=False).mean()
    ema26 = s_close.ewm(span=26, adjust=False).mean()
    df["MACD"]        = ema12 - ema26
    df["signal_Line"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["histogram"]   = df["MACD"] - df["signal_Line"]

    # Volume MAs
    df["volume_10"] = s_volume.rolling(10).mean()
    df["volume_20"] = s_volume.rolling(20).mean()
    df["volume_50"] = s_volume.rolling(50).mean()

    # ===== RSI (Wilder) — dùng np.nan, tránh dtype object =====
    delta = s_close.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()

    den = avg_loss.replace(0, np.nan)
    rs  = avg_gain / den

    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.astype("float64").replace([np.inf, -np.inf], np.nan)
    df["RSI"] = rsi

    # ===== MFI (close-only) =====
    tp = s_close
    mf = tp * s_volume
    pos_mf = mf.where(tp.diff() > 0, 0.0).astype("float64")
    neg_mf = mf.where(tp.diff() < 0, 0.0).astype("float64")

    pos_sum = pos_mf.rolling(14).sum()
    neg_sum = neg_mf.rolling(14).sum()

    den2 = neg_sum.replace(0, np.nan)
    mfr  = pos_sum / den2

    mfi = 100 - (100 / (1 + mfr))
    mfi = mfi.astype("float64").replace([np.inf, -np.inf], np.nan)
    df["MFI"] = mfi

    # ===== Làm sạch và round tất cả chỉ báo float =====
    for col in [
        "MA10","MA20","MA50","MACD","signal_Line","histogram",
        "volume_10","volume_20","volume_50","RSI","MFI"
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        df[col] = df[col].replace([np.inf, -np.inf], np.nan).round(2)

    # ===== Flags =====
    flags = {k: False for k in (
        "close_up_ma10","close_up_ma20","close_up_ma50",
        "close_down_ma10","close_down_ma20","close_down_ma50",
        "macd_cross_up","macd_cross_down"
    )}
    if len(df) >= 2:
        c0, c1 = s_close.iloc[-2], s_close.iloc[-1]
        ma10_0, ma10_1 = df["MA10"].iloc[-2], df["MA10"].iloc[-1]
        ma20_0, ma20_1 = df["MA20"].iloc[-2], df["MA20"].iloc[-1]
        ma50_0, ma50_1 = df["MA50"].iloc[-2], df["MA50"].iloc[-1]
        macd_0, macd_1 = df["MACD"].iloc[-2], df["MACD"].iloc[-1]
        sig_0,  sig_1  = df["signal_Line"].iloc[-2], df["signal_Line"].iloc[-1]

        if pd.notna(ma10_0) and pd.notna(ma10_1):
            flags["close_up_ma10"]   = (c0 < ma10_0) and (c1 >= ma10_1)
            flags["close_down_ma10"] = (c0 > ma10_0) and (c1 <= ma10_1)
        if pd.notna(ma20_0) and pd.notna(ma20_1):
            flags["close_up_ma20"]   = (c0 < ma20_0) and (c1 >= ma20_1)
            flags["close_down_ma20"] = (c0 > ma20_0) and (c1 <= ma20_1)
        if pd.notna(ma50_0) and pd.notna(ma50_1):
            flags["close_up_ma50"]   = (c0 < ma50_0) and (c1 >= ma50_1)
            flags["close_down_ma50"] = (c0 > ma50_0) and (c1 <= ma50_1)
        if all(pd.notna(x) for x in (macd_0, sig_0, macd_1, sig_1)):
            flags["macd_cross_up"]   = (macd_0 < sig_0) and (macd_1 >= sig_1)
            flags["macd_cross_down"] = (macd_0 > sig_0) and (macd_1 <= sig_1)

    last = df.iloc[-1]
    snap = {
        **{k: bool(v) for k,v in flags.items()},
        "RSI":        float(last["RSI"])        if pd.notna(last["RSI"])        else None,
        "MFI":        float(last["MFI"])        if pd.notna(last["MFI"])        else None,
        "MA10":       float(last["MA10"])       if pd.notna(last["MA10"])       else None,
        "MA20":       float(last["MA20"])       if pd.notna(last["MA20"])       else None,
        "MA50":       float(last["MA50"])       if pd.notna(last["MA50"])       else None,
        "volume_10":  float(last["volume_10"])  if pd.notna(last["volume_10"])  else None,
        "volume_20":  float(last["volume_20"])  if pd.notna(last["volume_20"])  else None,
        "volume_50":  float(last["volume_50"])  if pd.notna(last["volume_50"])  else None,
    }
    return df, snap

# ================ TRIGGER STATE ================
def read_trigger_state(symbol: str) -> Dict:
    """Đọc trạng thái đã bắn trigger chưa cho từng symbol."""
    try:
        raw = r.get(f"alert_trigger_state:{symbol}")
        return json.loads(raw) if raw else {}
    except Exception as e:
        logging.warning("Read trigger_state %s fail: %s", symbol, e)
        return {}

def write_trigger_state(symbol: str, state: Dict) -> None:
    """Ghi trạng thái trigger (đã bắn event nào rồi)."""
    try:
        r.set(f"alert_trigger_state:{symbol}", json.dumps(to_native(state), ensure_ascii=False))
    except Exception as e:
        logging.warning("Write trigger_state %s fail: %s", symbol, e)


def write_status_state(symbol: str, status: Dict) -> None:
    """Ghi trạng thái alert_status hiện tại vào Redis."""
    try:
        # chỉ lưu phần content (symbol + fields)
        r.set(
            f"alert_status_state:{symbol}",
            json.dumps(to_native(status), ensure_ascii=False, allow_nan=False)
        )
    except Exception as e:
        logging.warning("Write alert_status_state %s fail: %s", symbol, e)




# ================ ORCHESTRATION ================
def run_once(symbol: str) -> bool:
    alert = read_alert(symbol)
    if not alert:
        logging.warning("Không có alert hiện tại cho %s", symbol); return False
    new_close  = alert.get("close")
    new_volume = alert.get("totalVol") or alert.get("volume") or 0
    if new_close is None:
        logging.warning("Alert thiếu 'close' cho %s", symbol); return False

    hist = get_ohlcv_history(symbol)
    if hist is None or hist.empty: hist = pd.DataFrame(columns=["close","volume"])
    _, snap = compute_indicators(hist, new_close, new_volume)
    # 4) Build STATUS payload (trạng thái hiện tại)
    status_fields = [
        "close_up_ma10", "close_up_ma20", "close_up_ma50",
        "close_down_ma10", "close_down_ma20", "close_down_ma50",
        "RSI", "MFI",
        "MA10", "MA20", "MA50",
        "volume_10", "volume_20", "volume_50",
    ]

    status_content = {
        "symbol": symbol,
    }
    for k in status_fields:
        if k in snap:
            status_content[k] = snap.get(k)
    
    write_status_state(symbol, status_content)

    status_payload = {
        "function": "alert_status",
        "content": status_content,
        "source": "alert_status",  # ghi rõ source như mẫu của bạn
    }

    ok_status = publish(status_payload, channel=CHANNEL)

    # 5) Build TRIGGER payload (event 1-bar như macd_cross)
    EVENT_KEYS = ["macd_cross_up", "macd_cross_down"]

    trigger_state_old = read_trigger_state(symbol)      # state cũ từ Redis
    trigger_state_new = dict(trigger_state_old)         # sẽ ghi lại state mới
    events = []

    for name in EVENT_KEYS:
        current = bool(snap.get(name, False))           # trạng thái hiện tại (True/False)
        prev    = bool(trigger_state_old.get(name, False))  # trạng thái cũ trong Redis

        # Rising edge: False -> True => bắn trigger
        if current and not prev:
            events.append(name)

        # Luôn cập nhật state mới = current
        trigger_state_new[name] = current

    ok_trigger = True

    if events:
        trigger_payload = {
            "function": "alert_trigger",
            "content": {
                "symbol": symbol,
                "event": events,
            },
            "source": "alert_trigger",
        }
        ok_trigger = publish(trigger_payload, channel=CHANNEL)

        # Chỉ khi publish OK mới ghi lại state True (để nếu lỗi còn bắn lại được)
        if ok_trigger:
            write_trigger_state(symbol, trigger_state_new)
    else:
        # Không có event nhưng có thể có chuyển True -> False
        # => vẫn ghi state mới để lần True tiếp theo còn bắn lại
        if trigger_state_new != trigger_state_old:
            write_trigger_state(symbol, trigger_state_new)

    return bool(ok_status and ok_trigger)

def _run_once_safe(symbol: str) -> Dict:
    try:
        ok = run_once(symbol)
        return {"symbol": symbol, "ok": bool(ok), "error": None}
    except Exception as e:
        import traceback
        logging.error("❌ run_once failed for %s: %s\n%s", symbol, repr(e), traceback.format_exc())
        return {"symbol": symbol, "ok": False, "error": repr(e)}

def alert_all_stocks(symbol_list: List[str]) -> List[Dict]:
    msgs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as ex:
        futs = {ex.submit(_run_once_safe, s): s for s in symbol_list}
        for f in concurrent.futures.as_completed(futs):
            msgs.append(f.result())
    bad = [m["symbol"] for m in msgs if not m["ok"]]
    if bad: logging.error("Symbols lỗi: %s", ", ".join(bad))
    else:   logging.info("✅ Tất cả symbols OK")
    return msgs


# ================ EXAMPLE ================
def alert_hose8():
    alert_all_stocks(HOSE8)
