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
TABLE = "retail_sales_non_price"

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

    value = str(value).strip()

    if not value:
        return None

    value = value.replace(",", ".")

    try:
        return float(value)
    except ValueError:
        return None


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


def parse_retail_sales_non_price(file_path: str) -> pd.DataFrame:
    report_time = parse_time_from_filename(
        file_path
    )

    full_text = normalize_text(
        get_document_text(file_path)
    )

    match = re.search(
        r"(?:neu\s+)?loai tru yeu to gia"
        r".{0,100}?"
        r"(tang|giam)\s+([\d.,]+)%",
        full_text,
        re.IGNORECASE | re.DOTALL,
    )

    growth = None

    if match:
        direction = match.group(1)
        value = to_float(
            match.group(2)
        )

        if value is not None:
            growth = value / 100

            if direction == "giam":
                growth = -growth

            growth = round(
                growth,
                6,
            )

    result = {
        "time": report_time,
        "growth": growth,
    }

    return pd.DataFrame([result])


def upsert_retail_sales_non_price(df: pd.DataFrame):
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
                time DATE PRIMARY KEY,
                growth DOUBLE PRECISION
            )
        """))

        conn.execute(
            text(f"""
                INSERT INTO "{SCHEMA}"."{TABLE}" (
                    time,
                    growth
                )
                VALUES (
                    :time,
                    :growth
                )
                ON CONFLICT (time) DO UPDATE SET
                    growth = EXCLUDED.growth
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


def get_latest_file(data_dir: str) -> str:
    files = glob.glob(
        os.path.join(data_dir, "*.docx")
    )

    if not files:
        raise FileNotFoundError(
            f"Không tìm thấy file docx trong {data_dir}"
        )

    return sorted(
        files,
        key=_sort_key,
    )[-1]


def save_retail_sales_non_price(file_path: str):
    df = parse_retail_sales_non_price(
        file_path
    )

    print(
        df.to_string(index=False)
    )

    upsert_retail_sales_non_price(
        df
    )


#=======================Chạy file chỉ định trực tiếp trong terminal=====================
# def retail_sales_non_price(**context):
#     save_retail_sales_non_price("../../data/word/2026_05.docx")


#=======================Chạy file chỉ định airflow=====================
# def retail_sales_non_price(**context):
#     save_retail_sales_non_price(
#         "/opt/airflow/dags/utils/vimo/data/word/2026_05.docx"
#     )


#=====================Chạy file mới nhất=====================
# def retail_sales_non_price(**context):
#     data_dir = os.path.join(
#         os.path.dirname(__file__),
#         "../../data/word",
#     )
#
#     file_path = get_latest_file(data_dir)
#
#     print(f"📂 File mới nhất: {file_path}")
#
#     save_retail_sales_non_price(
#         file_path
#     )


#=====================Chạy tất cả file=====================
def retail_sales_non_price(**context):
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
            save_retail_sales_non_price(
                file_path
            )

        except Exception as exc:
            log.exception(
                "Lỗi file %s: %s",
                file_path,
                exc,
            )


if __name__ == "__main__":
    retail_sales_non_price()