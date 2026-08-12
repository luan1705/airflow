from sqlalchemy import create_engine, text
import pandas as pd
from pathlib import Path
import re
import logging

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    pool_size=10, max_overflow=20, pool_timeout=60
)

SYMBOL_LIST_FILE = (
    Path(__file__).resolve().parents[2]
    / "create_list"
    / "symbol_list.py"
)


def format_symbol_list(var_name: str, symbols: list[str], items_per_line: int = 8) -> str:
    symbols = list(dict.fromkeys(symbols))
    lines = [f"{var_name} = ["]
    for i in range(0, len(symbols), items_per_line):
        chunk = symbols[i:i + items_per_line]
        lines.append("    " + ",  ".join(f'"{s}"' for s in chunk) + ",")
    lines.append("]")
    return "\n".join(lines)


def save_dividend_date_to_symbol_list(symbols: list[str]):
    code = "# <dividend_date START>\n\n"
    code += format_symbol_list("dividend_date", symbols, items_per_line=8)
    code += "\n\n# <dividend_date END>"

    content = SYMBOL_LIST_FILE.read_text(encoding="utf-8")
    pattern = r"(?ms)# <dividend_date START>.*?# <dividend_date END>"

    if not re.search(pattern, content):
        raise RuntimeError(f"Không tìm thấy block dividend_date trong {SYMBOL_LIST_FILE}")

    content = re.sub(pattern, code, content)
    SYMBOL_LIST_FILE.write_text(content, encoding="utf-8")


def save_dividend_date():
    symbols = pd.read_sql(text("""
        SELECT symbol FROM dividend.dividend 
        WHERE date = CURRENT_DATE
    """), engine)['symbol'].tolist()

    log.info(f"✅ Có {len(symbols)} mã dividend_date: {symbols}")
    save_dividend_date_to_symbol_list(symbols)
    log.info(f"✅ Đã ghi dividend_date vào {SYMBOL_LIST_FILE}")