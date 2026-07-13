import pandas as pd
import re
import os
import glob
from datetime import date
from sqlalchemy import create_engine, text
import logging

log = logging.getLogger(__name__)

DB_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
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
        if 'tongmuc' in sheet.lower().replace(' ', '').replace('.', ''):
            return sheet
    raise ValueError(f"Không tìm thấy sheet Tongmuc trong {file_path}")


def get_value(row: pd.Series, month: int):
    """Lấy giá trị tháng từ dòng theo format."""
    if month == 1:
        val = row.iloc[3] if not pd.isna(row.iloc[3]) else None
    elif month in [2, 3, 4, 5, 7, 8, 10, 11]:
        val = row.iloc[2] if not pd.isna(row.iloc[2]) else None
    else:  # T6, T9, T12
        val = row.iloc[3] if not pd.isna(row.iloc[3]) else None
    return round(val * 1_000_000_000) if val is not None else None


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
        result[key] = get_value(row, month)

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

#=======================Chạy file chỉ định trực tiếp trong terminal=====================
# def retail_sales(**context):
#     """Chạy file chỉ định."""
#     save_retail_sales("../data/2023_01.xlsx")

#=======================Chạy file chỉ định airflow=====================
# def retail_sales(**context):
#     """Chạy file chỉ định."""
#     save_retail_sales("/opt/airflow/dags/utils/vimo/data/2023_01.xlsx")

# =====================Chạy file mới nhất=====================
def _sort_key(f):
    name = os.path.basename(f)
    match = re.search(r'(\d{4})_(\d{2})', name)
    return (int(match.group(1)), int(match.group(2))) if match else (0, 0)

def get_latest_file(data_dir: str) -> str:
    files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    if not files:
        raise FileNotFoundError(f"Không tìm thấy file xlsx trong {data_dir}")
    return sorted(files, key=_sort_key)[-1]


# def retail_sales(**context):
#     """Hàm entrypoint cho Airflow: tự tìm file mới nhất và upsert."""
#     data_dir = os.path.join(os.path.dirname(__file__), "../../data")
#     file_path = get_latest_file(data_dir)
#     print(f"📂 File mới nhất: {file_path}")
#     save_retail_sales(file_path)

# =====================Chạy tất cả file=====================
def retail_sales(**context):
    """Chạy tất cả file trong thư mục data."""
    data_dir = os.path.join(os.path.dirname(__file__), "../../data")
    files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    if not files:
        raise FileNotFoundError(f"Không tìm thấy file xlsx trong {data_dir}")
    for file_path in sorted(files, key=_sort_key):
        print(f"📂 Đang chạy: {file_path}")
        save_retail_sales(file_path) 

#===============================================================
 
if __name__ == "__main__":
    retail_sales()