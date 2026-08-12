import os
import re
import glob
import zipfile
import logging
import unicodedata
import xml.etree.ElementTree as ET

import pandas as pd

from datetime import date
from sqlalchemy import create_engine, text


log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
)

SCHEMA = "macro"
TABLE = "budget_balance"

WORD_NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
}

TRILLION_VND = 1_000_000_000_000


def normalize_text(value) -> str:
    if value is None:
        return ""

    value = str(value).strip().lower()
    value = unicodedata.normalize("NFD", value)

    value = "".join(
        char for char in value
        if unicodedata.category(char) != "Mn"
    )

    value = value.replace("đ", "d")
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def parse_time_from_filename(file_path: str) -> date:
    name = os.path.basename(file_path)

    match = re.search(r"(\d{4})[_-](\d{2})", name)

    if not match:
        raise ValueError(
            f"Không parse được tháng/năm từ tên file: {name}"
        )

    year = int(match.group(1))
    month = int(match.group(2))

    if not 1 <= month <= 12:
        raise ValueError(f"Tháng không hợp lệ: {month}")

    return date(year, month, 1)


def to_float(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    value = str(value).strip()

    if not value:
        return None

    value = value.replace("\xa0", "")
    value = re.sub(r"[^0-9,.\-Ee+]", "", value)

    if not value:
        return None

    if "," in value and "." not in value:
        value = value.replace(",", ".")

    elif "," in value and "." in value:
        if value.rfind(",") > value.rfind("."):
            value = value.replace(".", "")
            value = value.replace(",", ".")
        else:
            value = value.replace(",", "")

    try:
        return float(value)
    except ValueError:
        return None


def to_vnd(value):
    if value is None:
        return None

    return round(
        float(value) * TRILLION_VND
    )


def get_document_text(file_path: str) -> str:
    with zipfile.ZipFile(file_path, "r") as docx_zip:
        document_xml = docx_zip.read(
            "word/document.xml"
        )

    root = ET.fromstring(document_xml)

    paragraphs = []

    for paragraph in root.findall(
        ".//w:p",
        WORD_NS,
    ):
        texts = []

        for text_node in paragraph.findall(
            ".//w:t",
            WORD_NS,
        ):
            if text_node.text:
                texts.append(text_node.text)

        if texts:
            paragraphs.append(
                "".join(texts)
            )

    return "\n".join(paragraphs)


def extract_value(text_data: str, pattern: str):
    match = re.search(
        pattern,
        text_data,
        re.IGNORECASE | re.DOTALL,
    )

    if not match:
        return None

    return to_float(
        match.group(1)
    )


def parse_budget_balance(file_path: str) -> pd.DataFrame:
    report_time = parse_time_from_filename(file_path)

    full_text = normalize_text(
        get_document_text(file_path)
    )

    revenue = extract_value(
        full_text,
        r"tong thu ngan sach nha nuoc"
        r".{0,150}?"
        r"(?:uoc\s+)?dat\s+(?:gan\s+)?"
        r"([\d.,]+)\s+nghin ty dong",
    )

    expenditure = extract_value(
        full_text,
        r"tong chi ngan sach nha nuoc"
        r".{0,150}?"
        r"(?:uoc\s+)?dat\s+(?:gan\s+)?"
        r"([\d.,]+)\s+nghin ty dong",
    )

    revenue = to_vnd(revenue)
    expenditure = to_vnd(expenditure)

    balance = (
        revenue - expenditure
        if revenue is not None and expenditure is not None
        else None
    )

    result = {
        "time": report_time,

        "revenue": revenue,
        "expenditure": expenditure,
        "balance": balance,

        "revenue_ytd": None,
        "expenditure_ytd": None,
        "balance_ytd": None,
    }

    return pd.DataFrame([result])


def upsert_budget_balance(df: pd.DataFrame):
    records = (
        df.astype(object)
        .where(pd.notna(df), None)
        .to_dict(orient="records")
    )

    with engine.begin() as conn:
        conn.execute(text(
            f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'
        ))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE}" (
                time             DATE PRIMARY KEY,

                revenue          DOUBLE PRECISION,
                expenditure      DOUBLE PRECISION,
                balance          DOUBLE PRECISION,

                revenue_ytd      DOUBLE PRECISION,
                expenditure_ytd  DOUBLE PRECISION,
                balance_ytd      DOUBLE PRECISION
            )
        """))

        columns = [
            "revenue",
            "expenditure",
            "balance",
            "revenue_ytd",
            "expenditure_ytd",
            "balance_ytd",
        ]

        for column in columns:
            conn.execute(text(f"""
                ALTER TABLE "{SCHEMA}"."{TABLE}"
                ADD COLUMN IF NOT EXISTS "{column}"
                DOUBLE PRECISION
            """))

        conn.execute(
            text(f"""
                INSERT INTO "{SCHEMA}"."{TABLE}" (
                    time,
                    revenue,
                    expenditure,
                    balance,
                    revenue_ytd,
                    expenditure_ytd,
                    balance_ytd
                )
                VALUES (
                    :time,
                    :revenue,
                    :expenditure,
                    :balance,
                    :revenue_ytd,
                    :expenditure_ytd,
                    :balance_ytd
                )
                ON CONFLICT (time) DO UPDATE SET
                    revenue = EXCLUDED.revenue,
                    expenditure = EXCLUDED.expenditure,
                    balance = EXCLUDED.balance,
                    revenue_ytd = EXCLUDED.revenue_ytd,
                    expenditure_ytd = EXCLUDED.expenditure_ytd,
                    balance_ytd = EXCLUDED.balance_ytd
            """),
            records,
        )

    print(
        f"✅ Upsert {len(df)} rows vào "
        f"{SCHEMA}.{TABLE}"
    )


def _sort_key(file_path: str):
    name = os.path.basename(file_path)

    match = re.search(
        r"(\d{4})[_-](\d{2})",
        name,
    )

    if not match:
        return 0, 0

    return (
        int(match.group(1)),
        int(match.group(2)),
    )


def save_budget_balance(file_path: str):
    df = parse_budget_balance(
        file_path
    )

    print(
        df.to_string(index=False)
    )

    upsert_budget_balance(
        df
    )

def get_latest_file(data_dir: str) -> str:
    files = glob.glob(
        os.path.join(data_dir, "*.docx")
    )

    if not files:
        raise FileNotFoundError(
            f"Không tìm thấy file docx trong {data_dir}"
        )

    return sorted(files, key=_sort_key)[-1]


#=======================Chạy file chỉ định trực tiếp trong terminal=====================
# def budget_balance(**context):
#     save_budget_balance("../../data/word/2026_05.docx")


#=======================Chạy file chỉ định airflow=====================
# def budget_balance(**context):
#     save_budget_balance(
#         "/opt/airflow/dags/utils/vimo/data/word/2026_05.docx"
#     )

#=====================Chạy file mới nhất=====================

# def budget_balance(**context):
#     data_dir = os.path.join(
#         os.path.dirname(__file__),
#         "../../data/word",
#     )
#
#     file_path = get_latest_file(data_dir)
#
#     print(f"📂 File mới nhất: {file_path}")
#
#     save_budget_balance(file_path)


#=====================Chạy tất cả file=====================

def budget_balance(**context):
    data_dir = os.path.join(
        os.path.dirname(__file__),
        "../../data/word",
    )

    files = glob.glob(
        os.path.join(data_dir, "*.docx")
    )

    if not files:
        raise FileNotFoundError(
            f"Không tìm thấy file docx trong {data_dir}"
        )

    for file_path in sorted(
        files,
        key=_sort_key,
    ):
        print(f"📂 Đang chạy: {file_path}")

        try:
            save_budget_balance(
                file_path
            )

        except Exception as exc:
            log.exception(
                "Lỗi file %s: %s",
                file_path,
                exc,
            )


if __name__ == "__main__":
    budget_balance()