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
TABLE = "budget_expenditure"

WORD_NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
}


def normalize_text(value) -> str:
    """Chuẩn hóa chuỗi để tìm kiếm không phụ thuộc dấu và viết hoa."""
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
    """
    Parse thời gian từ tên file, ví dụ:
        2026_05.docx
        Kinh_te_2026_05.docx
    """
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
    """Chuyển số dạng Việt Nam sang float."""
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

    # Dạng 215,66
    if "," in value and "." not in value:
        value = value.replace(",", ".")

    # Dạng 1.234,56
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


def get_document_text(file_path: str) -> str:
    """
    Đọc toàn bộ text trong word/document.xml.
    """
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


def get_budget_expenditure_section(full_text: str) -> str:
    """
    Tìm đoạn bắt đầu từ Tổng chi ngân sách Nhà nước.
    Không phụ thuộc dấu, xuống dòng hay cách Word chia text run.
    """

    normalized = normalize_text(full_text)

    patterns = [
        r"tong chi ngan sach nha nuoc",
        r"tong chi can doi ngan sach nha nuoc",
    ]

    start = None

    for pattern in patterns:
        match = re.search(pattern, normalized)

        if match:
            start = match.start()
            break

    if start is None:
        raise ValueError(
            "Không tìm thấy phần Tổng chi ngân sách Nhà nước"
        )

    # Lấy đủ dài để chứa:
    # tổng chi tháng + tổng chi YTD + các cấu phần
    return normalized[start:start + 2500]


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


def parse_budget_expenditure(file_path: str) -> pd.DataFrame:
    report_time = parse_time_from_filename(
        file_path
    )

    full_text = get_document_text(
        file_path
    )

    section = get_budget_expenditure_section(
        full_text
    )

    # Tổng chi tháng
    total_expenditure = extract_value(
        section,
        r"tong chi(?: can doi)? ngan sach nha nuoc"
        r".{0,150}?"
        r"(?:uoc\s+)?dat\s+(?:gan\s+)?"
        r"([\d.,]+)\s+nghin ty dong",
    )

    # Tổng chi YTD
    total_expenditure_ytd = extract_value(
        section,
        r"(?:luy ke|tinh chung)"
        r".{0,250}?"
        r"(?:uoc\s+)?(?:dat\s+)?(?:gan\s+)?"
        r"([\d.,]+)\s+nghin ty dong",
    )

    # Chi thường xuyên YTD
    recurrent_ytd = extract_value(
        section,
        r"chi thuong xuyen"
        r".{0,200}?"
        r"(?:uoc\s+)?(?:dat\s+)?(?:gan\s+)?"
        r"([\d.,]+)\s+nghin ty dong",
    )

    # Chi đầu tư phát triển YTD
    development_investment_ytd = extract_value(
        section,
        r"chi dau tu phat trien"
        r".{0,200}?"
        r"(?:uoc\s+)?(?:dat\s+)?(?:gan\s+)?"
        r"([\d.,]+)\s+nghin ty dong",
    )

    # Chi trả nợ lãi YTD
    interest_payment_ytd = extract_value(
        section,
        r"chi tra no lai"
        r".{0,150}?"
        r"(?:uoc\s+)?(?:dat\s+)?(?:gan\s+)?"
        r"([\d.,]+)\s+nghin ty dong",
    )

    result = {
        "time": report_time,

        "recurrent": None,
        "development_investment": None,
        "interest_payment": None,
        "total_expenditure": None,

        "recurrent_ytd": (
            round(recurrent_ytd * 1_000_000_000_000)
            if recurrent_ytd is not None
            else None
        ),

        "development_investment_ytd": (
            round(development_investment_ytd * 1_000_000_000_000)
            if development_investment_ytd is not None
            else None
        ),

        "interest_payment_ytd": (
            round(interest_payment_ytd * 1_000_000_000_000)
            if interest_payment_ytd is not None
            else None
        ),

        "total_expenditure_ytd": None,
    }

    return pd.DataFrame([result])


def upsert_budget_expenditure(df: pd.DataFrame):
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
                time                        DATE PRIMARY KEY,

                recurrent                   DOUBLE PRECISION,
                development_investment      DOUBLE PRECISION,
                interest_payment            DOUBLE PRECISION,
                total_expenditure           DOUBLE PRECISION,

                recurrent_ytd               DOUBLE PRECISION,
                development_investment_ytd  DOUBLE PRECISION,
                interest_payment_ytd        DOUBLE PRECISION,
                total_expenditure_ytd       DOUBLE PRECISION
            )
        """))

        columns = [
            "recurrent",
            "development_investment",
            "interest_payment",
            "total_expenditure",
            "recurrent_ytd",
            "development_investment_ytd",
            "interest_payment_ytd",
            "total_expenditure_ytd",
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
                    recurrent,
                    development_investment,
                    interest_payment,
                    total_expenditure,
                    recurrent_ytd,
                    development_investment_ytd,
                    interest_payment_ytd,
                    total_expenditure_ytd
                )
                VALUES (
                    :time,
                    :recurrent,
                    :development_investment,
                    :interest_payment,
                    :total_expenditure,
                    :recurrent_ytd,
                    :development_investment_ytd,
                    :interest_payment_ytd,
                    :total_expenditure_ytd
                )
                ON CONFLICT (time) DO UPDATE SET
                    recurrent = EXCLUDED.recurrent,
                    development_investment = EXCLUDED.development_investment,
                    interest_payment = EXCLUDED.interest_payment,
                    total_expenditure = EXCLUDED.total_expenditure,
                    recurrent_ytd = EXCLUDED.recurrent_ytd,
                    development_investment_ytd = EXCLUDED.development_investment_ytd,
                    interest_payment_ytd = EXCLUDED.interest_payment_ytd,
                    total_expenditure_ytd = EXCLUDED.total_expenditure_ytd
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


def save_budget_expenditure(file_path: str):
    df = parse_budget_expenditure(
        file_path
    )

    print(
        df.to_string(index=False)
    )

    upsert_budget_expenditure(
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
# def budget_expenditure(**context):
#     save_budget_expenditure("../../data/word/2023_01.docx")


#=======================Chạy file chỉ định airflow=====================
# def budget_expenditure(**context):
#     save_budget_expenditure(
#         "/opt/airflow/dags/utils/vimo/data/word/2026_05.docx"
#     )


#=====================Chạy file mới nhất=====================

def budget_expenditure(**context):
    data_dir = os.path.join(
        os.path.dirname(__file__),
        "../../data/word",
    )

    file_path = get_latest_file(data_dir)

    print(f"📂 File mới nhất: {file_path}")

    save_budget_expenditure(
        file_path
    )


#=====================Chạy tất cả file=====================

# def budget_expenditure(**context):
#     data_dir = os.path.join(
#         os.path.dirname(__file__),
#         "../../data/word",
#     )

#     files = glob.glob(
#         os.path.join(data_dir, "*.docx")
#     )

#     if not files:
#         raise FileNotFoundError(
#             f"Không tìm thấy file docx trong {data_dir}"
#         )

#     for file_path in sorted(
#         files,
#         key=_sort_key,
#     ):
#         print(
#             f"📂 Đang chạy: {file_path}"
#         )

#         try:
#             save_budget_expenditure(
#                 file_path
#             )

#         except Exception as exc:
#             log.exception(
#                 "Lỗi file %s: %s",
#                 file_path,
#                 exc,
#             )


if __name__ == "__main__":
    budget_expenditure()