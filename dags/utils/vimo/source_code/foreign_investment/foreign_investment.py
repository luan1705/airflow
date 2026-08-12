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
TABLE = "foreign_investment"

# Lưu DB theo đơn vị USD
BILLION_USD = 1_000_000_000

WORD_NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
}


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


def billion_usd_to_usd(value):
    if value is None:
        return None

    return round(float(value) * BILLION_USD)


def get_document_text(file_path: str) -> str:
    """
    Đọc toàn bộ text trong word/document.xml.
    """
    with zipfile.ZipFile(file_path, "r") as docx_zip:
        content = docx_zip.read("word/document.xml")

    root = ET.fromstring(content)

    paragraphs = []

    for paragraph in root.findall(".//w:p", WORD_NS):
        texts = []

        for text_node in paragraph.findall(".//w:t", WORD_NS):
            if text_node.text:
                texts.append(text_node.text)

        if texts:
            paragraphs.append("".join(texts))

    return "\n".join(paragraphs)


def find_registered_fdi(full_text: str):
    pattern = re.compile(
        r"Tổng vốn đầu tư nước ngoài đăng ký vào Việt Nam"
        r".{0,800}?"
        r"(?:ước\s+tính\s+|ước\s+)?"
        r"đạt\s+(?:gần\s+)?"
        r"([\d.,]+)\s+tỷ\s+USD",
        re.IGNORECASE | re.DOTALL,
    )

    match = pattern.search(full_text)

    if not match:
        print("❌ Không tìm thấy registered FDI")
        return None

    value = to_float(match.group(1))

    print(f"✅ Registered FDI raw: {match.group(1)} -> {value}")

    return value


def find_realized_fdi(full_text: str):
    pattern = re.compile(
        r"Vốn đầu tư trực tiếp nước ngoài thực hiện tại Việt Nam"
        r".{0,800}?"
        r"(?:ước\s+tính\s+|ước\s+)?"
        r"đạt\s+(?:gần\s+)?"
        r"([\d.,]+)\s+tỷ\s+USD",
        re.IGNORECASE | re.DOTALL,
    )

    match = pattern.search(full_text)

    if not match:
        print("❌ Không tìm thấy realized FDI")
        return None

    value = to_float(match.group(1))

    print(f"✅ Realized FDI raw: {match.group(1)} -> {value}")

    return value


def parse_foreign_investment(file_path: str) -> pd.DataFrame:
    report_time = parse_time_from_filename(file_path)

    full_text = get_document_text(file_path)

    registered_ytd = find_registered_fdi(full_text)
    realized_ytd = find_realized_fdi(full_text)

    result = {
        "time": report_time,

        "registered": None,
        "realized": None,

        "registered_ytd": billion_usd_to_usd(
            registered_ytd
        ),
        "realized_ytd": billion_usd_to_usd(
            realized_ytd
        ),
    }

    return pd.DataFrame([result])


def upsert_foreign_investment(df: pd.DataFrame):
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
                time            DATE PRIMARY KEY,

                registered      DOUBLE PRECISION,
                realized        DOUBLE PRECISION,

                registered_ytd  DOUBLE PRECISION,
                realized_ytd    DOUBLE PRECISION
            )
        """))

        columns = [
            "registered",
            "realized",
            "registered_ytd",
            "realized_ytd",
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
                    registered,
                    realized,
                    registered_ytd,
                    realized_ytd
                )
                VALUES (
                    :time,
                    :registered,
                    :realized,
                    :registered_ytd,
                    :realized_ytd
                )
                ON CONFLICT (time) DO UPDATE SET
                    registered     = EXCLUDED.registered,
                    realized       = EXCLUDED.realized,
                    registered_ytd = EXCLUDED.registered_ytd,
                    realized_ytd   = EXCLUDED.realized_ytd
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


def save_foreign_investment(file_path: str):
    df = parse_foreign_investment(file_path)

    print(df.to_string(index=False))

    upsert_foreign_investment(df)

def get_latest_file(data_dir: str) -> str:
    files = glob.glob(
        os.path.join(data_dir, "*.docx")
    )

    if not files:
        raise FileNotFoundError(
            f"Không tìm thấy file docx trong {data_dir}"
        )

    return sorted(files, key=_sort_key)[-1]




# ===================== Chạy file chỉ định Airflow =====================

# def foreign_investment(**context):
#     save_foreign_investment(
#         "/opt/airflow/dags/utils/vimo/data/word/2026_05.docx"
#     )


# ===================== Chạy file mới nhất =====================

# def foreign_investment(**context):
#     data_dir = os.path.join(
#         os.path.dirname(__file__),
#         "../../data/word",
#     )
#
#     file_path = get_latest_file(data_dir)
#
#     print(f"📂 File mới nhất: {file_path}")
#
#     save_foreign_investment(file_path)


# ===================== Chạy tất cả file =====================

def foreign_investment(**context):
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

    for file_path in sorted(files, key=_sort_key):
        print(f"📂 Đang chạy: {file_path}")

        try:
            save_foreign_investment(file_path)

        except Exception as exc:
            log.exception(
                "Lỗi file %s: %s",
                file_path,
                exc,
            )


if __name__ == "__main__":
    foreign_investment()