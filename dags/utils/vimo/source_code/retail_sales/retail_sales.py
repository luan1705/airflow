import pandas as pd
import re
import os
import glob
from datetime import date
from sqlalchemy import create_engine, text
import logging

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
engine = create_engine(DB_URL)

SCHEMA = "macro"
TABLE  = "retail_sales"


def parse_time_from_filename(file_path: str) -> date:
    name = file_path.split("/")[-1].split("\\")[-1]
    match = re.search(r'(\d{4})_(\d{2})', name)
    if not match:
        raise ValueError(f"Không parse được tháng/năm từ tên file: {name}")
    year  = int(match.group(1))
    month = int(match.group(2))
    return date(year, month, 1)


def get_sheet_name(file_path: str) -> str:
    xl = pd.ExcelFile(file_path)

    for sheet in xl.sheet_names:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet, header=None, nrows=5)

            text = " ".join(
                df.fillna("")
                  .astype(str)
                  .values
                  .ravel()
            ).lower()

            if "tổng mức bán lẻ" in text or "tong muc ban le" in text:
                return sheet

        except Exception:
            continue

    raise ValueError(f"Không tìm thấy sheet retail sales trong {file_path}")


def get_value(row: pd.Series, report_time: date):
    """Lấy giá trị tháng từ dòng theo từng format file."""
    month = report_time.month
    year = report_time.year

    if month == 1:
        col_idx = 3

    elif month in [2, 3, 4, 5, 7, 8, 10, 11]:
        col_idx = 2

    elif month in [6, 9]:
        col_idx = 3

    elif month == 12:
        # Từ năm 2022 trở về trước, tháng 12 nằm ở cột iloc[2]
        # Từ năm 2023 trở đi, tháng 12 nằm ở cột iloc[3]
        col_idx = 2 if year <= 2022 else 3

    else:
        return None

    if len(row) <= col_idx or pd.isna(row.iloc[col_idx]):
        return None

    return round(float(row.iloc[col_idx]) * 1_000_000_000)


def parse_retail_sales(file_path: str) -> pd.DataFrame:
    time  = parse_time_from_filename(file_path)
    month = time.month

    sheet = get_sheet_name(file_path)
    df    = pd.read_excel(file_path, sheet_name=sheet, header=None)

    mask     = df[0].astype(str).str.contains("TỔNG SỐ", na=False)
    tong_idx = df[mask].index[0]

    keys   = ["total", "retail", "accommodation", "tourism", "other"]
    result = {"time": time}

    for i, key in enumerate(keys):
        row = df.iloc[tong_idx + i]
        result[key] = get_value(row, time)

    return pd.DataFrame([result])


def upsert_retail_sales(df: pd.DataFrame):
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                time                    DATE PRIMARY KEY,
                total                   DOUBLE PRECISION,
                retail                  DOUBLE PRECISION,
                accommodation           DOUBLE PRECISION,
                tourism                 DOUBLE PRECISION,
                other                   DOUBLE PRECISION,
                total_yoy               DOUBLE PRECISION,
                retail_yoy              DOUBLE PRECISION,
                accommodation_yoy       DOUBLE PRECISION,
                tourism_yoy             DOUBLE PRECISION,
                other_yoy               DOUBLE PRECISION,
                total_ytd               DOUBLE PRECISION,
                retail_ytd              DOUBLE PRECISION,
                accommodation_ytd       DOUBLE PRECISION,
                tourism_ytd             DOUBLE PRECISION,
                other_ytd               DOUBLE PRECISION,
                total_ytd_yoy           DOUBLE PRECISION,
                retail_ytd_yoy          DOUBLE PRECISION,
                accommodation_ytd_yoy   DOUBLE PRECISION,
                tourism_ytd_yoy         DOUBLE PRECISION,
                other_ytd_yoy           DOUBLE PRECISION
            )
        """))
        for col in [
            'total_yoy', 'retail_yoy', 'accommodation_yoy', 'tourism_yoy', 'other_yoy',
            'total_ytd', 'retail_ytd', 'accommodation_ytd', 'tourism_ytd', 'other_ytd',
            'total_ytd_yoy', 'retail_ytd_yoy', 'accommodation_ytd_yoy', 'tourism_ytd_yoy', 'other_ytd_yoy',
        ]:
            conn.execute(text(f"""
                ALTER TABLE {SCHEMA}.{TABLE}
                ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION
            """))

        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.{TABLE}
                (time, total, retail, accommodation, tourism, other)
            VALUES
                (:time, :total, :retail, :accommodation, :tourism, :other)
            ON CONFLICT (time) DO UPDATE SET
                total         = EXCLUDED.total,
                retail        = EXCLUDED.retail,
                accommodation = EXCLUDED.accommodation,
                tourism       = EXCLUDED.tourism,
                other         = EXCLUDED.other
        """), df.to_dict(orient="records"))

    print(f"✅ Upsert {len(df)} rows vào {SCHEMA}.{TABLE}")


def save_retail_sales(file_path: str):
    df = parse_retail_sales(file_path)
    print(df)
    upsert_retail_sales(df)

def _sort_key(f):
    name = os.path.basename(f)
    match = re.search(r'(\d{4})_(\d{2})', name)
    return (int(match.group(1)), int(match.group(2))) if match else (0, 0)

def get_latest_file(data_dir: str) -> str:
    files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    if not files:
        raise FileNotFoundError(f"Không tìm thấy file xlsx trong {data_dir}")
    return sorted(files, key=_sort_key)[-1]

#=======================Chạy file chỉ định trực tiếp trong terminal=====================
# def retail_sales(**context):
#     """Chạy file chỉ định."""
#     save_retail_sales("../data/excel/2023_01.xlsx")

# #=======================Chạy file chỉ định airflow=====================
# def retail_sales(**context):
#     """Chạy file chỉ định."""
#     save_retail_sales("/opt/airflow/dags/utils/vimo/data/excel/2020_09.xlsx")

# =====================Chạy file mới nhất=====================
def retail_sales(**context):
    """Hàm entrypoint cho Airflow: tự tìm file mới nhất và upsert."""
    data_dir = os.path.join(os.path.dirname(__file__), "../../data/excel")
    file_path = get_latest_file(data_dir)
    print(f"📂 File mới nhất: {file_path}")
    save_retail_sales(file_path)

## =====================Chạy tất cả file=====================
# def retail_sales(**context):
#     """Chạy tất cả file trong thư mục data."""
#     data_dir = os.path.join(os.path.dirname(__file__), "../../data/excel")
#     files = glob.glob(os.path.join(data_dir, "*.xlsx"))
#     if not files:
#         raise FileNotFoundError(f"Không tìm thấy file xlsx trong {data_dir}")
#     for file_path in sorted(files, key=_sort_key):
#         print(f"📂 Đang chạy: {file_path}")
#         try:
#             save_retail_sales(file_path)
#         except Exception as e:
#             print(f"⚠️ Lỗi {file_path}: {e} — upsert null")
#             try:
#                 time = parse_time_from_filename(file_path)
#                 df = pd.DataFrame([{"time": time}])
#                 upsert_retail_sales(df)
#             except Exception as e2:
#                 print(f"⚠️ Bỏ qua {file_path}: {e2}")

#===============================================================
 
if __name__ == "__main__":
    retail_sales()