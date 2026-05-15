from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import concurrent.futures
import logging
from pathlib import Path
import re

from utils.create_list.symbol_list import (
    HOSE,
    HNX,
    UPCOM,
    DERIVATIVES,
    CW,
    HNXBOND,
    ETFHOSE,
    indices,
)

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech",
    pool_size=10,
    max_overflow=20,
    pool_timeout=60,
)

SCHEMA_CHECK = "ohlcv_check"  # số 1 = chuẩn
SCHEMA_NORMAL = "ohlcv"       # số 2 = cần kiểm tra
SYMBOL_LIST_FILE = (
    Path(__file__).resolve().parents[1]
    / "create_list"
    / "symbol_list.py"
)
PRICE_ATOL = 1

def normalize_symbol(symbol: str) -> str:
    """
    Chuẩn hóa symbol giống logic bên job insert dữ liệu.
    """
    if symbol == "HNXUpcomIndex":
        return "UPCOMINDEX"

    if symbol == "HNXIndex":
        return "HNXINDEX"

    return symbol


def compare_symbol(symbol: str) -> bool:
    """
    Return:
        True  = symbol bị lệch giữa ohlcv_check và ohlcv
        False = symbol không lệch hoặc thiếu dữ liệu
    """

    symbol = normalize_symbol(symbol)
    table = f"{symbol}_1D"

    # số 1 = ohlcv_check = dữ liệu chính / chuẩn
    query1 = f'''
        SELECT time, open, high, low, close, volume
        FROM {SCHEMA_CHECK}."{table}"
    '''

    # số 2 = ohlcv = dữ liệu thường / cần kiểm tra
    query2 = f'''
        SELECT time, open, high, low, close, volume
        FROM {SCHEMA_NORMAL}."{table}"
    '''

    df1 = pd.read_sql(query1, engine)
    df2 = pd.read_sql(query2, engine)

    if df1.empty:
        log.warning(f"⚠️ {symbol}: ohlcv_check không có dữ liệu")
        return False

    if df2.empty:
        log.warning(f"⚠️ {symbol}: ohlcv không có dữ liệu")
        return False

    # Lấy ohlcv_check làm gốc
    # Chỉ so sánh các dòng time có trong ohlcv_check
    df = df1.merge(
        df2,
        on="time",
        how="left",
        suffixes=("_1", "_2"),
    )

    mask = (
        ~np.isclose(df["open_1"], df["open_2"], equal_nan=True, atol=PRICE_ATOL) |
        ~np.isclose(df["high_1"], df["high_2"], equal_nan=True, atol=PRICE_ATOL) |
        ~np.isclose(df["low_1"], df["low_2"], equal_nan=True, atol=PRICE_ATOL) |
        ~np.isclose(df["close_1"], df["close_2"], equal_nan=True, atol=PRICE_ATOL) #|
        # ~np.isclose(df["volume_1"], df["volume_2"], equal_nan=True, atol=PRICE_ATOL)
    )

    return mask.any()


def check_symbol(symbol: str):
    """
    Hàm wrapper để chạy trong ThreadPoolExecutor.
    """
    try:
        is_diff = compare_symbol(symbol)

        if is_diff:
            log.warning(f"❌ {symbol}: dữ liệu bị lệch")
            return symbol

        # log.info(f"✅ {symbol}: dữ liệu khớp")
        return None

    except Exception as e:
        log.error(f"❌ Error {symbol}: {e}")
        return None


def check_all_symbols(symbol_list, max_workers=5):
    addition = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(check_symbol, symbol) for symbol in symbol_list]

        for future in concurrent.futures.as_completed(futures):
            result = future.result()

            if result is not None:
                addition.append(result)

    return addition

def format_symbol_list(var_name: str, symbols: list[str], items_per_line: int = 8) -> str:
    """
    Format list giống HOSE/HNX trong symbol_list.py.
    """
    symbols = list(dict.fromkeys(symbols))

    lines = [f"{var_name} = ["]

    for i in range(0, len(symbols), items_per_line):
        chunk = symbols[i:i + items_per_line]
        line = "    " + ",  ".join(f'"{symbol}"' for symbol in chunk) + ","
        lines.append(line)

    lines.append("]")

    return "\n".join(lines)


def save_addition_to_symbol_list(addition: list[str]):
    """
    Ghi list addition vào block:
        # <addition START>
        addition = [...]
        # <addition END>
    """

    addition_code = "# <addition START>\n\n"
    addition_code += format_symbol_list("addition", addition, items_per_line=8)
    addition_code += "\n\n# <addition END>"

    content = SYMBOL_LIST_FILE.read_text(encoding="utf-8")

    pattern = r"(?ms)# <addition START>.*?# <addition END>"

    if not re.search(pattern, content):
        raise RuntimeError(
            f"Không tìm thấy block # <addition START> ... # <addition END> trong {SYMBOL_LIST_FILE}"
        )

    content = re.sub(pattern, addition_code, content)

    SYMBOL_LIST_FILE.write_text(content, encoding="utf-8")


def check_all_market_symbols():
    """
    Hàm tổng check tất cả sàn.
    Symbol nào lệch thì add vào addition.
    """

    all_symbols = []
    all_symbols += HOSE
    all_symbols += HNX
    all_symbols += UPCOM
    # all_symbols += DERIVATIVES
    # all_symbols += CW
    # all_symbols += HNXBOND
    # all_symbols += ETFHOSE
    # all_symbols += indices

    # Remove duplicate nếu có
    all_symbols = list(dict.fromkeys(all_symbols))

    log.info(f"🚀 Bắt đầu check tổng số mã: {len(all_symbols)}")

    addition = check_all_symbols(all_symbols, max_workers=5)

    log.info(f"✅ Tổng số mã đã check: {len(all_symbols)}")
    log.info(f"❌ Tổng số mã bị lệch: {len(addition)}")
    log.warning(f"Symbols bị lệch: {addition}")

    save_addition_to_symbol_list(addition)
    log.info(f"✅ Đã ghi addition vào {SYMBOL_LIST_FILE}")

    return None


if __name__ == "__main__":
    addition = check_all_market_symbols()
    print("Symbols bị lệch:", addition)