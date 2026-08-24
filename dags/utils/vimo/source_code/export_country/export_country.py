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
TABLE = "export_country"

# Lưu DB theo đơn vị USD
BILLION_USD = 1_000_000_000

WORD_NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
}

CHART_NS = {
    "c": "http://schemas.openxmlformats.org/drawingml/2006/chart"
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

def normalize_text_keep_newline(value) -> str:
    if value is None:
        return ""

    value = str(value).lower()
    value = unicodedata.normalize("NFD", value)

    value = "".join(
        char for char in value
        if unicodedata.category(char) != "Mn"
    )

    value = value.replace("đ", "d")

    lines = []

    for line in value.splitlines():
        line = re.sub(r"[ \t]+", " ", line).strip()
        lines.append(line)

    return "\n".join(lines)

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
    """Chuyển số dạng Việt Nam hoặc dạng XML sang float."""
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


def get_cache_values(element, cache_names: list[str]) -> list[str]:
    """
    Lấy dữ liệu từ strCache hoặc numCache trong chart XML.
    """
    for cache_name in cache_names:
        cache = element.find(f".//c:{cache_name}", CHART_NS)

        if cache is None:
            continue

        points = []

        for point in cache.findall("./c:pt", CHART_NS):
            idx_element = point.find("./c:v", CHART_NS)

            if idx_element is not None and idx_element.text is not None:
                point_index = int(point.attrib.get("idx", len(points)))
                points.append((point_index, idx_element.text))

        points.sort(key=lambda item: item[0])

        return [value for _, value in points]

    return []


def get_series_name(series) -> str:
    """Lấy tên series, ví dụ 'Xuất khẩu hàng hóa'."""
    values = get_cache_values(
        series,
        [
            "strCache",
            "numCache",
        ],
    )

    # get_cache_values trên toàn series có thể lấy nhầm category,
    # nên ưu tiên tìm riêng trong thẻ tx.
    tx = series.find("./c:tx", CHART_NS)

    if tx is not None:
        tx_values = get_cache_values(
            tx,
            [
                "strCache",
                "numCache",
            ],
        )

        if tx_values:
            return tx_values[0]

        direct_value = tx.find("./c:v", CHART_NS)

        if direct_value is not None:
            return direct_value.text or ""

    return values[0] if values else ""


def get_series_categories(series) -> list[str]:
    """Lấy danh mục của series: Trung Quốc, Hoa Kỳ, Hàn Quốc..."""
    category = series.find("./c:cat", CHART_NS)

    if category is None:
        category = series.find("./c:xVal", CHART_NS)

    if category is None:
        return []

    return get_cache_values(
        category,
        [
            "strCache",
            "numCache",
        ],
    )


def get_series_numbers(series) -> list[float | None]:
    """Lấy giá trị số tương ứng từng danh mục."""
    values_element = series.find("./c:val", CHART_NS)

    if values_element is None:
        values_element = series.find("./c:yVal", CHART_NS)

    if values_element is None:
        return []

    raw_values = get_cache_values(
        values_element,
        [
            "numCache",
            "strCache",
        ],
    )

    return [to_float(value) for value in raw_values]


def parse_chart_xml(chart_content: bytes) -> list[dict]:
    """
    Parse toàn bộ series trong một chart XML.

    Kết quả:
    [
        {
            "name": "Xuất khẩu hàng hóa",
            "categories": [...],
            "values": [...]
        }
    ]
    """
    root = ET.fromstring(chart_content)
    result = []

    for series in root.findall(".//c:ser", CHART_NS):
        name = get_series_name(series)
        categories = get_series_categories(series)
        values = get_series_numbers(series)

        if not categories or not values:
            continue

        result.append({
            "name": name,
            "categories": categories,
            "values": values,
        })

    return result


def find_export_country_chart(file_path: str) -> list[dict]:
    with zipfile.ZipFile(file_path, "r") as docx_zip:
        chart_files = sorted(
            file_name
            for file_name in docx_zip.namelist()
            if re.fullmatch(
                r"word/charts/chart\d+\.xml",
                file_name,
            )
        )

        for chart_file in chart_files:
            xml_content = docx_zip.read(chart_file)
            root = ET.fromstring(xml_content)

            title_parts = [
                node.text
                for node in root.findall(".//a:t", {
                    "a": "http://schemas.openxmlformats.org/drawingml/2006/main"
                })
                if node.text
            ]

            chart_title = normalize_text(
                " ".join(title_parts)
            )

            # CHỈ lấy chart kim ngạch
            if (
                "kim ngach xuat, nhap khau hang hoa"
                not in chart_title
            ):
                continue

            series_list = parse_chart_xml(
                xml_content
            )

            print(
                f"✅ Chọn chart kim ngạch: {chart_file}"
            )
            print(
                f"✅ Chart title: {chart_title}"
            )

            return series_list

    raise ValueError(
        f"Không tìm thấy chart "
        f"'Kim ngạch xuất, nhập khẩu hàng hóa' "
        f"trong {file_path}"
    )


def series_to_dict(series: dict) -> dict[str, float | None]:
    """Chuyển category và value của series thành dictionary."""
    result = {}

    for category, value in zip(
        series["categories"],
        series["values"],
    ):
        result[normalize_text(category)] = value

    return result


def get_export_series(series_list: list[dict]) -> dict:
    for series in series_list:
        name = normalize_text(
            series.get("name")
        )

        if name == "xuat khau hang hoa":
            return series

    raise ValueError(
        "Không tìm thấy series Xuất khẩu hàng hóa"
    )

def get_document_text(file_path: str) -> str:
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

def get_export_section(file_path: str) -> str:
    paragraphs = get_document_paragraphs(
        file_path
    )

    export_paragraphs = []
    inside_export = False

    for paragraph in paragraphs:
        normalized = normalize_text(
            paragraph
        )

        # Bắt đầu đúng block xuất khẩu.
        is_export_heading = (
            "xuat khau hang hoa" in normalized
            or "hang hoa xuat khau" in normalized
        ) and len(normalized) <= 80

        if not inside_export and is_export_heading:
            inside_export = True
            continue

        # Kết thúc khi tới block nhập khẩu.
        is_import_heading = (
            "nhap khau hang hoa" in normalized
            or "hang hoa nhap khau" in normalized
        ) and len(normalized) <= 80

        if inside_export and is_import_heading:
            break

        if inside_export:
            export_paragraphs.append(
                paragraph
            )

    if not export_paragraphs:
        raise ValueError(
            "Không tìm thấy block Xuất khẩu hàng hóa"
        )

    return "\n".join(
        export_paragraphs
    )
def get_document_paragraphs(file_path: str) -> list[str]:
    with zipfile.ZipFile(file_path, "r") as docx_zip:
        content = docx_zip.read(
            "word/document.xml"
        )

    root = ET.fromstring(
        content
    )

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
                texts.append(
                    text_node.text
                )

        if texts:
            value = "".join(
                texts
            ).strip()

            if value:
                paragraphs.append(
                    value
                )

    return paragraphs

def find_export_country_from_text(file_path: str):
    # Chỉ đọc trong block Xuất khẩu hàng hóa -> trước Nhập khẩu hàng hóa.
    export_section = get_export_section(
        file_path
    )

    normalized_export_section = normalize_text(
        export_section
    )

    # Trong block xuất khẩu, chỉ lấy phần bắt đầu từ "Về thị trường..."
    # Hỗ trợ cả "thị trường xuất khẩu hàng hóa"
    # và "thị trường hàng hóa xuất khẩu".
    market_match = re.search(
        r"\bve thi truong\b"
        r".{0,100}?"
        r"\bxuat khau\b"
        r"(.+)",
        normalized_export_section,
        re.DOTALL,
    )

    if not market_match:
        raise ValueError(
            "Không tìm thấy đoạn Về thị trường xuất khẩu"
        )

    market_section = market_match.group(1)

    result = {}

    countries = {
        "usa": ["hoa ky", "my"],
        "china": ["trung quoc"],
        "korea": ["han quoc"],
        "asean": ["asean"],
        "japan": ["nhat ban"],
    }

    for key, aliases in countries.items():
        value = None

        for country in aliases:
            match = re.search(
                rf"\b{country}\b"
                rf"[^.]{{0,220}}?"
                rf"([\d.,]+)\s+ty\s+usd",
                market_section,
                re.DOTALL,
            )

            if match:
                value = to_float(
                    match.group(1)
                )
                break

        result[key] = value

    # EU xử lý riêng trên text gốc:
    # chỉ nhận đúng "EU" viết hoa và đứng độc lập.
    original_market_match = re.search(
        r"Về thị trường"
        r".{0,100}?"
        r"(?:xuất khẩu|hàng hóa xuất khẩu)"
        r"(.+)",
        export_section,
        re.DOTALL,
    )

    result["eu"] = None

    if original_market_match:
        eu_match = re.search(
            r"\bEU\b"
            r"[^.]{0,220}?"
            r"([\d.,]+)\s+tỷ\s+USD",
            original_market_match.group(1),
            re.DOTALL,
        )

        if eu_match:
            result["eu"] = to_float(
                eu_match.group(1)
            )

    print(
        "✅ Export section:"
    )
    print(
        export_section
    )

    print(
        "✅ Export country text:",
        result,
    )

    return result
def billion_usd_to_usd(value):
    if value is None:
        return None

    return round(float(value) * BILLION_USD)


def parse_export_country(file_path: str) -> pd.DataFrame:
    report_time = parse_time_from_filename(
        file_path
    )

    try:
        series_list = find_export_country_chart(
            file_path
        )

        export_series = get_export_series(
            series_list
        )

        export_values = series_to_dict(
            export_series
        )

        usa_ytd = export_values.get("hoa ky")
        china_ytd = export_values.get("trung quoc")
        korea_ytd = export_values.get("han quoc")
        asean_ytd = export_values.get("asean")
        eu_ytd = export_values.get("eu")
        japan_ytd = export_values.get("nhat ban")

        # Nếu chart có nhưng thiếu bất kỳ nước nào
        # thì chuyển sang đọc text
        if (
            usa_ytd is None
            or china_ytd is None
            or korea_ytd is None
            or asean_ytd is None
            or eu_ytd is None
            or japan_ytd is None
        ):
            raise ValueError(
                "Chart export_country thiếu dữ liệu"
            )

        print(
            "✅ Lấy dữ liệu export_country từ chart"
        )

    except Exception as exc:
        print(
            f"⚠️ Không dùng được chart ({exc}), "
            f"chuyển sang đọc từ text"
        )

        export_values = find_export_country_from_text(
            file_path
        )

        usa_ytd = export_values.get("usa")
        china_ytd = export_values.get("china")
        korea_ytd = export_values.get("korea")
        asean_ytd = export_values.get("asean")
        eu_ytd = export_values.get("eu")
        japan_ytd = export_values.get("japan")

        print(
            "✅ Dữ liệu text:",
            {
                "usa": usa_ytd,
                "china": china_ytd,
                "korea": korea_ytd,
                "asean": asean_ytd,
                "eu": eu_ytd,
                "japan": japan_ytd,
            }
        )

    result = {
        "time": report_time,

        "usa": None,
        "china": None,
        "korea": None,
        "asean": None,
        "eu": None,
        "japan": None,

        "usa_ytd": billion_usd_to_usd(
            usa_ytd
        ),

        "china_ytd": billion_usd_to_usd(
            china_ytd
        ),

        "korea_ytd": billion_usd_to_usd(
            korea_ytd
        ),

        "asean_ytd": billion_usd_to_usd(
            asean_ytd
        ),

        "eu_ytd": billion_usd_to_usd(
            eu_ytd
        ),

        "japan_ytd": billion_usd_to_usd(
            japan_ytd
        ),
    }

    return pd.DataFrame([result])


def upsert_export_country(df: pd.DataFrame):
    records = (
        df.where(pd.notna(df), None)
        .to_dict(orient="records")
    )

    with engine.begin() as conn:
        conn.execute(text(
            f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'
        ))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE}" (
                time        DATE PRIMARY KEY,

                usa         DOUBLE PRECISION,
                china       DOUBLE PRECISION,
                korea       DOUBLE PRECISION,
                asean       DOUBLE PRECISION,
                eu          DOUBLE PRECISION,
                japan       DOUBLE PRECISION,

                usa_ytd     DOUBLE PRECISION,
                china_ytd   DOUBLE PRECISION,
                korea_ytd   DOUBLE PRECISION,
                asean_ytd   DOUBLE PRECISION,
                eu_ytd      DOUBLE PRECISION,
                japan_ytd   DOUBLE PRECISION
            )
        """))

        columns = [
                "usa",
                "china",
                "korea",
                "asean",
                "eu",
                "japan",
                "usa_ytd",
                "china_ytd",
                "korea_ytd",
                "asean_ytd",
                "eu_ytd",
                "japan_ytd",
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
                    usa,
                    china,
                    korea,
                    asean,
                    eu,
                    usa_ytd,
                    china_ytd,
                    korea_ytd,
                    asean_ytd,
                    eu_ytd,
                    japan_ytd
                )
                VALUES (
                    :time,
                    :usa,
                    :china,
                    :korea,
                    :asean,
                    :eu,
                    :usa_ytd,
                    :china_ytd,
                    :korea_ytd,
                    :asean_ytd,
                    :eu_ytd,
                    :japan_ytd
                )
                ON CONFLICT (time) DO UPDATE SET
                    usa       = EXCLUDED.usa,
                    china     = EXCLUDED.china,
                    korea     = EXCLUDED.korea,
                    asean     = EXCLUDED.asean,
                    eu        = EXCLUDED.eu,
                    japan     = EXCLUDED.japan,
                    usa_ytd   = EXCLUDED.usa_ytd,
                    china_ytd = EXCLUDED.china_ytd,
                    korea_ytd = EXCLUDED.korea_ytd,
                    asean_ytd = EXCLUDED.asean_ytd,
                    eu_ytd    = EXCLUDED.eu_ytd,
                    japan_ytd = EXCLUDED.japan_ytd
            """),
            records,
        )

    print(
        f"✅ Upsert {len(df)} rows vào "
        f"{SCHEMA}.{TABLE}"
    )

def _sort_key(file_path: str):
    name = os.path.basename(file_path)
    match = re.search(r"(\d{4})[_-](\d{2})", name)

    if not match:
        return 0, 0

    return int(match.group(1)), int(match.group(2))


def save_export_country(file_path: str):
    df = parse_export_country(file_path)

    print(df.to_string(index=False))

    upsert_export_country(df)

def _sort_key(file_path: str):
    name = os.path.basename(file_path)
    match = re.search(r"(\d{4})[_-](\d{2})", name)

    if not match:
        return 0, 0

    return int(match.group(1)), int(match.group(2))


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
# def retail_sales(**context):
#     """Chạy file chỉ định."""
#     save_retail_sales("../data/word/2023_01.xlsx")

# ===================== Chạy file chỉ định Airflow =====================

# def export_country(**context):
#     save_export_country(
#         "/opt/airflow/dags/utils/vimo/data/word/2026_05.docx"
#     )


# ===================== Chạy file mới nhất =====================

def export_country(**context):
    data_dir = os.path.join(
        os.path.dirname(__file__),
        "../../data/word",
    )

    file_path = get_latest_file(data_dir)
    print(f"📂 File mới nhất: {file_path}")

    save_export_country(file_path)


# ===================== Chạy tất cả file =====================

# def export_country(**context):
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

#     for file_path in sorted(files, key=_sort_key):
#         print(f"📂 Đang chạy: {file_path}")

#         try:
#             save_export_country(file_path)

#         except Exception as exc:
#             log.exception(
#                 "Lỗi file %s: %s",
#                 file_path,
#                 exc,
#             )


if __name__ == "__main__":
    export_country()